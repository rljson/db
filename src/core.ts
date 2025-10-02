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
  Edit,
  EditProtocolRow,
  Rljson,
  Route,
  TableCfg,
  Validate,
} from '@rljson/rljson';

import { TransformComponent } from './transform/transform-component.ts';
import { TransformLayer } from './transform/transform-layer.ts';
import { Transform } from './transform/transform.ts';

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
  async run(edit: Edit<any>): Promise<EditProtocolRow<any>> {
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

    const content = await this.contentType(tableKey);

    let transform: Transform;
    if (content === 'components') {
      transform = new TransformComponent<any>(this._io, edit, tableKey);
    } else if (content === 'layers') {
      transform = new TransformLayer(this._io, edit, tableKey);
    } else {
      throw new Error(`Table ${tableKey} is not supported for edits.`);
    }

    //Run Edit
    const editProtocolRow = await transform.run();

    //Protocol Edit
    await this.protocol(tableKey, editProtocolRow);

    return editProtocolRow;
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
      type: 'edits',
      columns: [
        { key: '_hash', type: 'string' },
        { key: 'timeId', type: 'string' },
        { key: `${tableCfg.key}Ref`, type: 'string' },
        { key: 'route', type: 'string' },
        { key: 'origin', type: 'string' },
        { key: 'previous', type: 'jsonArray' },
      ],
      isHead: false,
      isRoot: false,
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
   * @param data - The rljson data to import.
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
  /**
   * Adds an edit protocol row to the edits table of a table
   * @param table - The table the edit was made on
   * @param editProtocolRow - The edit protocol row to add
   * @throws {Error} If the edits table does not exist
   */
  private async protocol(
    table: string,
    editProtocolRow: EditProtocolRow<any>,
  ): Promise<void> {
    const protocolTable = table + 'Edits';
    const hasTable = await this.hasTable(protocolTable);
    if (!hasTable) {
      throw new Error(`Table ${table} does not exist`);
    }

    //Write edit protocol row to io
    await this._io.write({
      data: {
        [protocolTable]: {
          _data: [editProtocolRow],
          _type: 'edits',
        },
      },
    });
  }

  // ...........................................................................
  /**
   * Get the edit protocol of a table
   * @param table - The table to get the edit protocol for
   * @throws {Error} If the edits table does not exist
   */
  async getProtocol(
    table: string,
    options?: { sorted?: boolean; ascending?: boolean },
  ): Promise<Rljson> {
    const protocolTable = table + 'Edits';
    const hasTable = await this.hasTable(protocolTable);
    if (!hasTable) {
      throw new Error(`Table ${table} does not exist`);
    }

    if (options === undefined) {
      options = { sorted: false, ascending: true };
    }

    if (options.sorted) {
      const dumpedTable = await this._io.dumpTable({ table: protocolTable });
      const tableData = dumpedTable[protocolTable]
        ._data as EditProtocolRow<any>[];

      //Sort table
      tableData.sort((a, b) => {
        const aTime = a.timeId.split(':')[1];
        const bTime = b.timeId.split(':')[1];
        if (options.ascending) {
          return aTime.localeCompare(bTime);
        } else {
          return bTime.localeCompare(aTime);
        }
      });

      return { [protocolTable]: { _data: tableData, _type: 'edits' } };
    }

    return this._io.dumpTable({ table: protocolTable });
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
