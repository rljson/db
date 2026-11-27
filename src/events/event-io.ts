// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hsh } from '@rljson/hash';
import { Io } from '@rljson/io';
import { Json, JsonValue } from '@rljson/json';
import { ContentType, Rljson, TableCfg, TableKey } from '@rljson/rljson';

import type {
  DbEventContext,
  DbEvents,
  DbReadEvent,
  DbReadOperation,
  DbRowFilter,
  DbWriteEvent,
} from './db-events.ts';

// #############################################################################
/**
 * An Io whose reads and writes pass the hooks of a {@link DbEvents}.
 *
 * - Reads run the before-read hooks; a refusal throws, filters drop rows
 *   from the result.
 * - Writes run the before-write hooks; a refusal throws, filters drop rows
 *   from the data. The write observers start after the write.
 * - Table configurations, content types and lifecycle calls pass unchanged.
 *
 * Without hooks every call goes straight to the wrapped Io.
 */
export class EventIo implements Io {
  /**
   * Constructor
   * @param _io - The wrapped Io
   * @param _events - The event model whose hooks run
   * @param context - Who or what reads and writes through this Io
   * @param depth - How many hooks lie between the caller and this Io
   */
  constructor(
    private readonly _io: Io,
    private readonly _events: DbEvents,
    readonly context: DbEventContext,
    readonly depth: number,
  ) {
    if (_io.readRowsByHashes) {
      this.readRowsByHashes = (request) =>
        this._read(
          'readRowsByHashes',
          { table: request.table, hashes: request.hashes },
          () => this._io.readRowsByHashes!(request),
        );
    }
  }

  // ...........................................................................
  /** The wrapped Io */
  get io(): Io {
    return this._io;
  }

  // ...........................................................................
  /** True when the wrapped Io is open */
  get isOpen(): boolean {
    return this._io.isOpen;
  }

  // ...........................................................................
  /** Initializes the wrapped Io */
  init(): Promise<void> {
    return this._io.init();
  }

  // ...........................................................................
  /** Closes the wrapped Io */
  close(): Promise<void> {
    return this._io.close();
  }

  // ...........................................................................
  /** Resolves once the wrapped Io is ready */
  isReady(): Promise<void> {
    return this._io.isReady();
  }

  // ...........................................................................
  /** Dumps all tables, filtered by the before-read hooks */
  dump(): Promise<Rljson> {
    return this._read('dump', {}, () => this._io.dump());
  }

  // ...........................................................................
  /**
   * Dumps one table, filtered by the before-read hooks
   * @param request - The table to dump
   */
  dumpTable(request: { table: string }): Promise<Rljson> {
    return this._read('dumpTable', { table: request.table }, () =>
      this._io.dumpTable(request),
    );
  }

  // ...........................................................................
  /**
   * Returns the content type of a table
   * @param request - The table
   */
  contentType(request: { table: string }): Promise<ContentType> {
    return this._io.contentType(request);
  }

  // ...........................................................................
  /**
   * Returns true when the table exists
   * @param tableKey - The table
   */
  tableExists(tableKey: TableKey): Promise<boolean> {
    return this._io.tableExists(tableKey);
  }

  // ...........................................................................
  /**
   * Creates or extends a table after the before-write hooks allowed it
   * @param request - The table configuration
   */
  async createOrExtendTable(request: { tableCfg: TableCfg }): Promise<void> {
    if (!this._events.hasWriteHooks && !this._events.hasWriteObservers) {
      return this._io.createOrExtendTable(request);
    }

    const event = this._writeEvent({
      operation: 'createOrExtendTable',
      tables: [request.tableCfg.key],
      tableCfg: request.tableCfg,
    });

    // A filter has no rows to drop here
    if (this._events.hasWriteHooks) await this._events.runBeforeWrite(event);
    await this._io.createOrExtendTable(request);
    this._events.emitAfterWrite(event);
  }

  // ...........................................................................
  /** Returns the raw table configurations */
  rawTableCfgs(): Promise<TableCfg[]> {
    return this._io.rawTableCfgs();
  }

  // ...........................................................................
  /**
   * Writes the data the before-write hooks leave
   * @param request - The data to write
   */
  async write(request: { data: Rljson }): Promise<void> {
    const tables = _tableKeys(request.data);
    let data = request.data;

    if (this._events.hasWriteHooks) {
      const filter = await this._events.runBeforeWrite(
        this._writeEvent({ operation: 'write', tables, data }),
      );
      if (filter) data = _filterRljson(data, filter);
    }

    await this._io.write({ data });

    if (this._events.hasWriteObservers) {
      this._events.emitAfterWrite(
        this._writeEvent({ operation: 'write', tables, data }),
      );
    }
  }

  // ...........................................................................
  /**
   * Reads rows, filtered by the before-read hooks
   * @param request - The table and the where clause
   */
  readRows(request: {
    table: string;
    where: { [column: string]: JsonValue | null };
  }): Promise<Rljson> {
    return this._read(
      'readRows',
      { table: request.table, where: request.where },
      () => this._io.readRows(request),
    );
  }

  // ...........................................................................
  /**
   * Reads rows by their hashes, filtered by the before-read hooks. Only
   * there when the wrapped Io has it.
   */
  readonly readRowsByHashes?: (request: {
    table: string;
    hashes: string[];
  }) => Promise<Rljson>;

  // ...........................................................................
  /**
   * Counts the rows of a table the before-read hooks leave
   * @param table - The table
   */
  async rowCount(table: string): Promise<number> {
    if (!this._events.hasReadHooks) return this._io.rowCount(table);

    const filter = await this._events.runBeforeRead(
      this._readEvent('rowCount', { table }),
    );
    if (!filter) return this._io.rowCount(table);

    const dump = await this._io.dumpTable({ table });
    return _filterRljson(dump, filter)[table]._data.length;
  }

  // ######################
  // Private
  // ######################

  // ...........................................................................
  private async _read(
    operation: DbReadOperation,
    request: Pick<DbReadEvent, 'table' | 'where' | 'hashes'>,
    read: () => Promise<Rljson>,
  ): Promise<Rljson> {
    if (!this._events.hasReadHooks) return read();

    const filter = await this._events.runBeforeRead(
      this._readEvent(operation, request),
    );

    const result = await read();
    return filter ? _filterRljson(result, filter) : result;
  }

  // ...........................................................................
  private _readEvent(
    operation: DbReadOperation,
    request: Pick<DbReadEvent, 'table' | 'where' | 'hashes'>,
  ): DbReadEvent {
    return {
      kind: 'read',
      operation,
      ...request,
      context: this.context,
      depth: this.depth,
      io: this._followUpIo(),
    };
  }

  // ...........................................................................
  private _writeEvent(
    write: Pick<DbWriteEvent, 'operation' | 'tables' | 'data' | 'tableCfg'>,
  ): DbWriteEvent {
    return {
      kind: 'write',
      ...write,
      context: this.context,
      depth: this.depth,
      io: this._followUpIo(),
    };
  }

  // ...........................................................................
  private _followUpIo(): Io {
    return this._events.io(this._io, this.context, this.depth + 1);
  }
}

// .............................................................................
const _tableKeys = (data: Rljson): string[] =>
  Object.keys(data).filter((key) => !key.startsWith('_'));

// .............................................................................
/**
 * Keeps the rows of every table the filter keeps. A table that loses rows
 * gets its hash updated.
 * @param data - The tables
 * @param filter - The filter
 */
const _filterRljson = (data: Rljson, filter: DbRowFilter): Rljson => {
  const result: Record<string, unknown> = {};
  let changed = false;

  for (const [key, value] of Object.entries(data)) {
    const table = value as { _data?: Json[] } | undefined;
    if (key.startsWith('_') || !Array.isArray(table?._data)) {
      result[key] = value;
      continue;
    }

    const rows = table._data.filter((row) => filter(row, key));
    if (rows.length === table._data.length) {
      result[key] = value;
      continue;
    }

    changed = true;
    result[key] = _rehash({ ...table, _data: rows });
  }

  if (changed && '_hash' in result) {
    return _rehash(result) as Rljson;
  }
  return result as Rljson;
};

// .............................................................................
const _rehash = <T>(json: T): T => {
  if (!(json as { _hash?: string })._hash) return json;
  return hsh(json as Json, {
    updateExistingHashes: true,
    throwOnWrongHashes: false,
  }) as T;
};
