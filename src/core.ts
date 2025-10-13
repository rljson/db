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
  createEditProtocolTableCfg,
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

  // ...........................................................................
  /**
   * Creates a table and an edit protocol for the table
   * @param tableCfg TableCfg of table to create
   */
  async createEditable(tableCfg: TableCfg): Promise<void> {
    await this.createTable(tableCfg);
    await this.createEditProtocol(tableCfg);
  }

  /**
   * Creates a table
   * @param tableCfg TableCfg of table to create
   */
  async createTable(tableCfg: TableCfg): Promise<void> {
    return this._io.createOrExtendTable({ tableCfg });
  }
  /**
   * Creates an edit protocol table for a given table
   * @param tableCfg TableCfg of table
   */
  async createEditProtocol(tableCfg: TableCfg): Promise<void> {
    const cfg = createEditProtocolTableCfg(tableCfg);
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
  dumpTable(table: string): Promise<Rljson> {
    return this._io.dumpTable({ table });
  }

  // ...........................................................................
  /**
   * Imports data into the memory.
   * @param data - The rljson data to import.
   * @throws {Error} If the data is invalid.
   */
  async import(data: Rljson): Promise<void> {
    // Throw an error if the data is invalid
    const validate = new Validate();
    validate.addValidator(new BaseValidator());

    const result = await validate.run(data);
    // If there are errors and they are not refsNotFound, throw an error
    // refsNotFound can be ignored because we dont check against existing data
    // when importing new data
    if (
      (result.hasErrors || (result.base && result.base.hasErrors)) &&
      !result.base.refsNotFound
    ) {
      throw new Error(
        'The imported rljson data is not valid:\n' +
          JSON.stringify(result, null, 2),
      );
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
    return await this._io.tableExists(table);
  }
  // ...........................................................................
  async contentType(table: string): Promise<ContentType | null> {
    const t = await this._io.dumpTable({ table });
    const contentType = t[table]?._type;
    return contentType;
  }

  // ...........................................................................
  /** Reads a specific row from a database table */
  readRow(table: string, rowHash: string): Promise<Rljson> {
    return this._io.readRows({ table, where: { _hash: rowHash } });
  }

  // ...........................................................................
  readRows(
    table: string,
    where: { [column: string]: JsonValue },
  ): Promise<Rljson> {
    return this._io.readRows({ table, where });
  }
}
