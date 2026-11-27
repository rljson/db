// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Io } from '@rljson/io';
import { Json, JsonValue } from '@rljson/json';
import { InsertHistoryRow, Rljson, Route, TableCfg } from '@rljson/rljson';

import { EventIo } from './event-io.ts';

// #############################################################################
/**
 * Describes who or what causes a read or write, e.g. `{ peer: 'client-7' }`.
 * The event model passes it to every hook untouched; its meaning belongs to
 * the code that creates the Io and the hooks that read it.
 */
export type DbEventContext = { readonly [key: string]: JsonValue | undefined };

// #############################################################################
/** The read operations of an Io that run the before-read hooks */
export type DbReadOperation =
  'readRows' | 'readRowsByHashes' | 'dump' | 'dumpTable' | 'rowCount';

// #############################################################################
/** The write operations of an Io that run the before-write hooks */
export type DbWriteOperation = 'write' | 'createOrExtendTable';

// #############################################################################
/** The part every read and write event shares */
export interface DbIoEventBase {
  /** Who or what causes the operation, see {@link DbEventContext} */
  readonly context: DbEventContext;

  /** 0 for an operation from outside, +1 for each hook in between */
  readonly depth: number;

  /**
   * The Io a hook or observer uses for reads and writes of its own. They run
   * the hooks again, one level deeper, so that hooks calling each other end
   * with a {@link DbHookLoopError} instead of running forever.
   */
  readonly io: Io;
}

// #############################################################################
/** A read that is about to happen */
export interface DbReadEvent extends DbIoEventBase {
  /** Distinguishes read from write events */
  readonly kind: 'read';

  /** The Io method that reads */
  readonly operation: DbReadOperation;

  /** The table read, undefined for a dump of all tables */
  readonly table?: string;

  /** The where clause of `readRows` */
  readonly where?: { [column: string]: JsonValue | null };

  /** The hashes of `readRowsByHashes` */
  readonly hashes?: string[];
}

// #############################################################################
/** A write that is about to happen, or has happened for observers */
export interface DbWriteEvent extends DbIoEventBase {
  /** Distinguishes read from write events */
  readonly kind: 'write';

  /** The Io method that writes */
  readonly operation: DbWriteOperation;

  /** The tables written */
  readonly tables: string[];

  /** The data of `write`. Observers get what was written after filtering. */
  readonly data?: Rljson;

  /** The table configuration of `createOrExtendTable` */
  readonly tableCfg?: TableCfg;
}

// #############################################################################
/** An insert of `Db` has been written, with its insert history row */
export interface DbInsertEvent {
  /** Distinguishes the insert event from read and write events */
  readonly kind: 'insert';

  /** The route the insert was made on */
  readonly route: Route;

  /** The insert history row of the insert */
  readonly insertHistoryRow: InsertHistoryRow<any>;
}

// #############################################################################
/**
 * Keeps a row when it returns true. Gets the row and the key of its table.
 * The filter of a hook registered for certain tables is only asked about
 * the rows of these tables.
 */
export type DbRowFilter = (row: Json, table: string) => boolean;

// #############################################################################
/**
 * What a before hook decides:
 * - nothing or `{ allow: true }`: the operation goes on
 * - `{ refuse: reason }`: the operation throws a {@link DbHookRefusedError}
 * - `{ filter }`: only the rows the filter keeps are read or written
 */
export type DbHookDecision =
  | void
  | undefined
  | { readonly allow: true }
  | { readonly refuse: string }
  | { readonly filter: DbRowFilter };

// #############################################################################
/** Runs before a read or write and decides about it */
export type DbBeforeHook<E> = (
  event: E,
) => DbHookDecision | Promise<DbHookDecision>;

// #############################################################################
/** Runs after a write or insert; it cannot change the outcome anymore */
export type DbObserver<E> = (event: E) => unknown;

// #############################################################################
/** How a hook or observer is registered */
export interface DbHookOptions {
  /**
   * Higher runs first. Hooks of the same priority run in the order they
   * were registered. Default 0.
   */
  readonly priority?: number;

  /** Names the hook in errors. Default: the name of the function. */
  readonly name?: string;

  /** Restricts the hook to these tables. Default: all tables. */
  readonly tables?: string[];
}

// #############################################################################
/** How an insert observer is registered */
export interface DbInsertObserverOptions extends DbHookOptions {
  /** Restricts the observer to inserts on this route. Default: all routes. */
  readonly route?: Route;
}

// #############################################################################
/** Thrown when a before hook refuses a read or write */
export class DbHookRefusedError extends Error {
  /**
   * Constructor
   * @param hook - The name of the refusing hook
   * @param reason - Why the hook refused
   * @param event - The refused read or write
   */
  constructor(
    readonly hook: string,
    readonly reason: string,
    readonly event: DbReadEvent | DbWriteEvent,
  ) {
    const table = event.kind === 'read' ? event.table : event.tables.join(', ');
    super(
      `Hook "${hook}" refused ${event.operation}` +
        `${table ? ` on ${table}` : ''}: ${reason}`,
    );
    this.name = 'DbHookRefusedError';
  }
}

// #############################################################################
/** Thrown when hooks trigger each other deeper than allowed */
export class DbHookLoopError extends Error {
  /**
   * Constructor
   * @param depth - The depth that was reached
   * @param maxDepth - The deepest depth allowed
   */
  constructor(
    readonly depth: number,
    readonly maxDepth: number,
  ) {
    super(
      `Hooks triggered each other ${depth} levels deep; at most ${maxDepth} ` +
        'are allowed. A hook probably writes or reads what triggers it again.',
    );
    this.name = 'DbHookLoopError';
  }
}

// #############################################################################
/** Options of {@link DbEvents} */
export interface DbEventsOptions {
  /** The deepest depth hooks may trigger each other to. Default 8. */
  readonly maxDepth?: number;

  /** Receives the errors of observers. Default: console.error. */
  readonly onObserverError?: (message: string, error: unknown) => void;
}

// .............................................................................
type _Entry<F> = {
  readonly fn: F;
  readonly priority: number;
  readonly name: string;
  readonly tables?: ReadonlySet<string>;
  readonly routeFlat?: string;
  readonly seq: number;
};

// #############################################################################
/**
 * The one event model of Rljson.
 *
 * - **Before hooks** run before a read or write of an Io and allow, refuse
 *   or filter it.
 * - **Observers** run after a write, or after an insert of `Db`, and cannot
 *   change it anymore.
 *
 * Hooks run one after the other, the highest priority first and in the
 * order of registration within a priority. The first refusal ends the
 * operation; the filters of all hooks apply together. Observers start in the
 * same order; they are not awaited and their errors are only reported.
 *
 * Reads and writes pass the hooks through {@link DbEvents.io}, which wraps
 * any Io. `Db` wraps its own Io this way.
 */
export class DbEvents {
  /**
   * Constructor
   * @param options - See {@link DbEventsOptions}
   */
  constructor(options: DbEventsOptions = {}) {
    this.maxDepth = options.maxDepth ?? 8;
    this._onObserverError =
      options.onObserverError ??
      ((message, error) => console.error(message, error));
  }

  // ...........................................................................
  /** Returns an event model without hooks, for tests and examples */
  static example(options: DbEventsOptions = {}): DbEvents {
    return new DbEvents(options);
  }

  /** The deepest depth hooks may trigger each other to */
  readonly maxDepth: number;

  // ...........................................................................
  /**
   * Wraps an Io so that its reads and writes pass the hooks
   * @param io - The Io to wrap
   * @param context - Who or what reads and writes through the returned Io
   * @param depth - 0 for operations from outside
   * @returns An Io with the same data
   */
  io(io: Io, context: DbEventContext = {}, depth = 0): Io {
    return new EventIo(io, this, context, depth);
  }

  // ...........................................................................
  /**
   * Registers a hook that runs before every read
   * @param hook - The hook
   * @param options - Priority, name and tables of the hook
   * @returns A function that removes the hook again
   */
  beforeRead(
    hook: DbBeforeHook<DbReadEvent>,
    options: DbHookOptions = {},
  ): () => void {
    return this._add(this._beforeRead, hook, options);
  }

  // ...........................................................................
  /**
   * Registers a hook that runs before every write
   * @param hook - The hook
   * @param options - Priority, name and tables of the hook
   * @returns A function that removes the hook again
   */
  beforeWrite(
    hook: DbBeforeHook<DbWriteEvent>,
    options: DbHookOptions = {},
  ): () => void {
    return this._add(this._beforeWrite, hook, options);
  }

  // ...........................................................................
  /**
   * Registers an observer that runs after every write
   * @param observer - The observer
   * @param options - Priority, name and tables of the observer
   * @returns A function that removes the observer again
   */
  afterWrite(
    observer: DbObserver<DbWriteEvent>,
    options: DbHookOptions = {},
  ): () => void {
    return this._add(this._afterWrite, observer, options);
  }

  // ...........................................................................
  /**
   * Registers an observer that runs after every insert of `Db`
   * @param observer - The observer
   * @param options - Priority, name and route of the observer
   * @returns A function that removes the observer again
   */
  afterInsert(
    observer: DbObserver<DbInsertEvent>,
    options: DbInsertObserverOptions = {},
  ): () => void {
    return this._add(this._afterInsert, observer, options);
  }

  // ...........................................................................
  /**
   * Registers a listener that is called whenever a before-read hook is added
   * or removed, e.g. to drop cached reads
   * @param listener - The listener
   * @returns A function that removes the listener again
   */
  onReadHooksChanged(listener: () => void): () => void {
    this._readHooksChangedListeners.push(listener);
    return () => {
      this._readHooksChangedListeners = this._readHooksChangedListeners.filter(
        (l) => l !== listener,
      );
    };
  }

  // ...........................................................................
  /** True when at least one before-read hook is registered */
  get hasReadHooks(): boolean {
    return this._beforeRead.length > 0;
  }

  /** True when at least one before-write hook is registered */
  get hasWriteHooks(): boolean {
    return this._beforeWrite.length > 0;
  }

  /** True when at least one write observer is registered */
  get hasWriteObservers(): boolean {
    return this._afterWrite.length > 0;
  }

  // ...........................................................................
  /**
   * Runs the before-read hooks
   * @param event - The read about to happen
   * @returns The filter of all hooks together, undefined when none filters
   * @throws DbHookRefusedError when a hook refuses
   * @throws DbHookLoopError when the event is too deep
   */
  async runBeforeRead(event: DbReadEvent): Promise<DbRowFilter | undefined> {
    return this._runBefore(this._beforeRead, event, _tablesOfRead(event));
  }

  // ...........................................................................
  /**
   * Runs the before-write hooks
   * @param event - The write about to happen
   * @returns The filter of all hooks together, undefined when none filters
   * @throws DbHookRefusedError when a hook refuses
   * @throws DbHookLoopError when the event is too deep
   */
  async runBeforeWrite(event: DbWriteEvent): Promise<DbRowFilter | undefined> {
    return this._runBefore(this._beforeWrite, event, event.tables);
  }

  // ...........................................................................
  /**
   * Starts the write observers
   * @param event - The write that happened
   */
  emitAfterWrite(event: DbWriteEvent): void {
    const entries = this._afterWrite
      .filter((e) => _matchesTables(e, event.tables))
      .map((e) => e.fn);
    this._startObservers(
      entries,
      event,
      `observers for write on ${event.tables.join(', ')}`,
    );
  }

  // ...........................................................................
  /**
   * Starts the insert observers of a route and those of all routes
   * @param route - The route the insert was made on
   * @param insertHistoryRow - The insert history row of the insert
   */
  emitInsert(route: Route, insertHistoryRow: InsertHistoryRow<any>): void {
    const entries = this.insertObservers(route);
    const event: DbInsertEvent = { kind: 'insert', route, insertHistoryRow };
    this._startObservers(entries, event, `callbacks for route ${route.flat}`);
  }

  // ...........................................................................
  /**
   * Returns the insert observers that receive the inserts on a route, in the
   * order they run
   * @param route - The route
   * @returns The observers
   */
  insertObservers(route: Route): DbObserver<DbInsertEvent>[] {
    return this._afterInsert
      .filter((e) => e.routeFlat === undefined || e.routeFlat === route.flat)
      .map((e) => e.fn);
  }

  // ######################
  // Private
  // ######################

  private readonly _beforeRead: _Entry<DbBeforeHook<DbReadEvent>>[] = [];
  private readonly _beforeWrite: _Entry<DbBeforeHook<DbWriteEvent>>[] = [];
  private readonly _afterWrite: _Entry<DbObserver<DbWriteEvent>>[] = [];
  private readonly _afterInsert: _Entry<DbObserver<DbInsertEvent>>[] = [];
  private _readHooksChangedListeners: (() => void)[] = [];
  private _seq = 0;
  private readonly _onObserverError: (message: string, error: unknown) => void;

  // ...........................................................................
  private _add<F>(
    list: _Entry<F>[],
    fn: F,
    options: DbInsertObserverOptions,
  ): () => void {
    const entry: _Entry<F> = {
      fn,
      priority: options.priority ?? 0,
      name: options.name || (fn as { name?: string }).name || 'anonymous',
      tables: options.tables ? new Set(options.tables) : undefined,
      routeFlat: options.route?.flat,
      seq: this._seq++,
    };

    list.push(entry);
    list.sort((a, b) => b.priority - a.priority || a.seq - b.seq);
    this._changed(list);

    let removed = false;
    return () => {
      if (removed) return;
      removed = true;
      list.splice(list.indexOf(entry), 1);
      this._changed(list);
    };
  }

  // ...........................................................................
  private _changed(list: unknown) {
    if (list !== this._beforeRead) return;
    for (const listener of this._readHooksChangedListeners) listener();
  }

  // ...........................................................................
  private async _runBefore<E extends DbReadEvent | DbWriteEvent>(
    hooks: _Entry<DbBeforeHook<E>>[],
    event: E,
    tables: string[] | undefined,
  ): Promise<DbRowFilter | undefined> {
    if (event.depth > this.maxDepth) {
      throw new DbHookLoopError(event.depth, this.maxDepth);
    }

    const filters: DbRowFilter[] = [];

    // A snapshot: a hook may add or remove hooks while it runs
    for (const hook of [...hooks]) {
      if (!_matchesTables(hook, tables)) continue;

      const decision = await hook.fn(event);
      if (!decision) continue;

      if ('refuse' in decision) {
        throw new DbHookRefusedError(hook.name, decision.refuse, event);
      }

      if ('filter' in decision) {
        filters.push(_limitToTables(decision.filter, hook.tables));
      }
    }

    if (filters.length === 0) return undefined;
    if (filters.length === 1) return filters[0];
    return (row, table) => filters.every((f) => f(row, table));
  }

  // ...........................................................................
  private _startObservers<E>(
    observers: DbObserver<E>[],
    event: E,
    what: string,
  ) {
    if (observers.length === 0) return;

    const runs = observers.map(async (observer) => observer(event));
    Promise.all(runs).catch((err) => {
      this._onObserverError(`Error notifying ${what}:`, err);
    });
  }
}

// .............................................................................
/** A filter of a table-bound hook keeps all rows of other tables */
const _limitToTables = (
  filter: DbRowFilter,
  tables: ReadonlySet<string> | undefined,
): DbRowFilter =>
  tables ? (row, table) => !tables.has(table) || filter(row, table) : filter;

// .............................................................................
const _tablesOfRead = (event: DbReadEvent): string[] | undefined =>
  event.table === undefined ? undefined : [event.table];

// .............................................................................
const _matchesTables = (
  entry: { tables?: ReadonlySet<string> },
  tables: string[] | undefined,
): boolean => {
  if (!entry.tables) return true;

  // A dump reads all tables: a table-bound hook has to see it
  if (tables === undefined) return true;
  return tables.some((t) => entry.tables!.has(t));
};
