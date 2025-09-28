// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hsh } from '@rljson/hash';
import { Io, IoMem } from '@rljson/io';
import { Json, JsonValue, JsonValueH } from '@rljson/json';
import {
  BaseValidator,
  ComponentRef,
  Edit,
  Ref,
  Rljson,
  Route,
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
   * Runs an edit on the database
   * @param edit - The edit to run
   * @throws when the table does not exist
   * @throws when the edit is invalid
   */
  async run(edit: Edit<any>): Promise<any> {
    const route: Route = Route.fromFlat(edit.route);
    const isRoot = route.isRoot;

    if (!isRoot) {
      return this.run({
        ...edit,
        route: route.deeper().flat,
      });
    }

    const tableKey = route.segment();
    const hasTable = await this.hasTable(tableKey);
    if (!hasTable) {
      throw new Error(`Table ${tableKey} does not exist`);
    }

    let ref: Ref;
    if (edit.type === 'components') {
      ref = await this.runComponentEdit(tableKey, edit.value);
    } else {
      throw new Error(`Unsupported edit type: ${edit.type}`);
    }

    return { [tableKey]: ref };
  }

  async runComponentEdit(
    tableKey: string,
    value: JsonValueH,
  ): Promise<ComponentRef> {
    const component = value as JsonValue & { _hash?: string };
    const rlJson = { [tableKey]: { _data: [component] } } as Rljson;
    await this._io.write({ data: rlJson });
    return hsh(component as Json)._hash as string;
  }

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
    const editProtocolTableCfg: TableCfg = {
      key: `${tableCfg.key}Edits`,
      type: 'editProtocol',
      columns: [
        { key: '_hash', type: 'string' },
        { key: 'id', type: 'number' },
        { key: 'route', type: 'string' },
        { key: 'origin', type: 'string' },
        { key: 'previous', type: 'jsonArray' },
      ],
      isHead: true,
      isRoot: true,
      isShared: false,
    };
    await this.createTable(editProtocolTableCfg);
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
   * @throws {Error} If the data is invalid.
   */
  async import(data: Rljson): Promise<void> {
    // Throw an error if the data is invalid
    const validate = new Validate();
    validate.addValidator(new BaseValidator());

    const result = await validate.run(data);
    if (result.hasErrors || (result.base && result.base.hasErrors)) {
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
