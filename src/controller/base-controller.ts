// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Json, JsonValue } from '@rljson/json';
import {
  EditProtocolRow,
  Ref,
  Rljson,
  TableKey,
  TableType,
} from '@rljson/rljson';

import { Core } from '../core.ts';

import {
  Controller,
  ControllerCommands,
  ControllerRefs,
} from './controller.ts';

export class BaseController<T extends TableType>
  implements Controller<any, any>
{
  constructor(
    protected readonly _core: Core,
    protected readonly _tableKey: TableKey,
  ) {}

  async run(
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    command: ControllerCommands,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    value: Json,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    origin?: Ref,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    refs?: ControllerRefs,
  ): Promise<EditProtocolRow<any>> {
    throw new Error('Method not implemented.');
  }
  init(): Promise<void> {
    throw new Error('Method not implemented.');
  }

  // ...........................................................................
  /**
   * Retrieves the current state of the table.
   * @returns A promise that resolves to the current state of the table.
   */
  async table(): Promise<T> {
    const rljson = await this._core.dumpTable(this._tableKey);
    if (!rljson[this._tableKey]) {
      throw new Error(`Table ${this._tableKey} does not exist.`);
    }
    return rljson[this._tableKey] as T;
  }

  // ...........................................................................
  /**
   * Fetches a specific entry from the table by its reference or by a partial match.
   * @param where A string representing the reference of the entry to fetch, or an object representing a partial match.
   * @returns A promise that resolves to an array of entries matching the criteria, or null if no entries are found.
   */
  async get(where: string | Json): Promise<Rljson> {
    if (typeof where === 'string') {
      return this._getByHash(where);
    } else if (typeof where === 'object' && where !== null) {
      // If where is an object, we assume it's a partial match
      return this._getByWhere(where);
    } else {
      return Promise.resolve({});
    }
  }

  // ...........................................................................
  /**
   * Fetches a specific entry from the table by its reference.
   * @param hash A string representing the reference of the entry to fetch.
   * @returns A promise that resolves to the entry matching the reference, or null if no entry is found.
   */
  protected async _getByHash(hash: string): Promise<Rljson> {
    const row = await this._core.readRow(this._tableKey, hash);
    if (!row || !row[this._tableKey] || !row[this._tableKey]._data) {
      return {};
    }
    return row;
  }

  // ...........................................................................
  /**
   * Fetches entries from the table that match the specified criteria.
   * @param where An object representing the criteria to match.
   * @returns A promise that resolves to an array of entries matching the criteria, or null if no entries are found.
   */
  protected async _getByWhere(where: Json): Promise<Rljson> {
    const rows = await this._core.readRows(
      this._tableKey,
      where as { [column: string]: JsonValue },
    );
    if (!rows || !rows[this._tableKey] || !rows[this._tableKey]._data) {
      return {};
    }
    return rows;
  }
}
