// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Io, IoMem } from '@rljson/io';
import { JsonValue } from '@rljson/json';
import {
  BaseValidator,
  ContentType,
  createInsertHistoryTableCfg,
  Rljson,
  TableCfg,
  Validate,
} from '@rljson/rljson';

/** Implements core functionalities like importing data, setting tables  */
export class Core {
  // ...........................................................................
  constructor(private readonly _io: Io) {}

  static example = async () => {
    return new Core(await IoMem.example());
  };

  /**
   * Cache of raw table configurations.
   * Invalidated whenever a table is created or extended.
   */
  private _tableCfgsCache: TableCfg[] | null = null;

  /**
   * Positive-only cache of existing tables. Tables can never be dropped
   * through the Io interface, so a `true` result stays valid forever.
   */
  private readonly _existingTables = new Set<string>();

  // ...........................................................................
  /**
   * Creates a table and an insertHistory for the table
   * @param tableCfg TableCfg of table to create
   */
  async createTableWithInsertHistory(tableCfg: TableCfg): Promise<void> {
    await this.createTable(tableCfg);
    await this.createInsertHistory(tableCfg);
  }

  /**
   * Creates a table
   * @param tableCfg TableCfg of table to create
   */
  async createTable(tableCfg: TableCfg): Promise<void> {
    this._tableCfgsCache = null;
    return this._io.createOrExtendTable({ tableCfg });
  }
  /**
   * Creates an insertHistory table for a given table
   * @param tableCfg TableCfg of table
   */
  async createInsertHistory(tableCfg: TableCfg): Promise<void> {
    const cfg = createInsertHistoryTableCfg(tableCfg);
    await this.createTable(cfg);
  }

  // ...........................................................................
  /**
   * Returns a dump of the database
   */
  dump(): Promise<Rljson> {
    return this._io.dump();
  }

  /**
   * Returns a dump of a table.
   * @returns a dump of a table.
   * @throws when table name does not exist
   */
  async dumpTable(table: string): Promise<Rljson> {
    return await this._io.dumpTable({ table });
  }

  // ...........................................................................
  /**
   * Imports data into the memory.
   * @param data - The rljson data to import.
   * @param options - Set `validate: false` to skip validation for
   *   internally constructed payloads whose shape is fixed by the caller.
   * @throws {Error} If the data is invalid.
   */
  async import(data: Rljson, options?: { validate?: boolean }): Promise<void> {
    if (options?.validate !== false) {
      // Throw an error if the data is invalid
      const validate = new Validate();
      validate.addValidator(new BaseValidator());

      const result = await validate.run(data);
      // If there are errors and they are not refsNotFound, throw an error
      // refsNotFound can be ignored because we dont check against existing data
      // when importing new data
      if (
        (result.hasErrors || (result.base && result.base.hasErrors)) &&
        !result.base.refsNotFound &&
        !result.base.layerBasesNotFound
      ) {
        throw new Error(
          'The imported rljson data is not valid:\n' +
            JSON.stringify(result, null, 2),
        );
      }
    }

    // Write data
    await this._io.write({ data });
  }

  // ...........................................................................
  async tables(): Promise<Rljson> {
    return await this._io.dump();
  }

  // ...........................................................................
  async hasTable(table: string): Promise<boolean> {
    if (this._existingTables.has(table)) {
      return true;
    }
    const exists = await this._io.tableExists(table);
    if (exists) {
      this._existingTables.add(table);
    }
    return exists;
  }

  // ...........................................................................
  async contentType(table: string): Promise<ContentType> {
    const contentType = await this._io.contentType({ table });
    return contentType;
  }

  // ...........................................................................
  async tableCfg(table: string): Promise<TableCfg> {
    this._tableCfgsCache ??= await this._io.rawTableCfgs();
    const tableCfg = this._tableCfgsCache.find(
      (tc) => tc.key === table,
    ) as TableCfg;
    return tableCfg;
  }

  // ...........................................................................
  /** Reads a specific row from a database table */
  async readRow(table: string, rowHash: string): Promise<Rljson> {
    const result = await this._io.readRows({
      table,
      where: { _hash: rowHash },
    });
    return result;
  }

  // ...........................................................................
  async readRows(
    table: string,
    where: { [column: string]: JsonValue },
  ): Promise<Rljson> {
    return await this._io.readRows({ table, where });
  }
}
