// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { equals, Json, JsonValue } from '@rljson/json';
import {
  ContentType,
  InsertHistoryRow,
  Ref,
  Rljson,
  TableKey,
  TableType,
} from '@rljson/rljson';

import { Core } from '../core.ts';

import { CakeControllerCommands } from './cake-controller.ts';
import {
  Controller,
  ControllerChildProperty,
  ControllerRefs,
} from './controller.ts';

export abstract class BaseController<T extends TableType, C extends JsonValue>
  implements Controller<any, any, any>
{
  protected _contentType?: ContentType;

  constructor(
    protected readonly _core: Core,
    protected readonly _tableKey: TableKey,
  ) {}

  // ...........................................................................
  abstract insert(
    command: CakeControllerCommands,
    value: C,
    origin?: Ref,
    refs?: ControllerRefs,
  ): Promise<InsertHistoryRow<any>[]>;

  // ...........................................................................
  abstract init(): Promise<void>;

  // ...........................................................................
  abstract getChildRefs(
    where: string | Json,
    filter?: Json,
  ): Promise<ControllerChildProperty[]>;

  // ...........................................................................
  abstract filterRow(
    row: Json,
    key: string,
    value: JsonValue,
  ): Promise<boolean>;

  // ...........................................................................
  /**
   * Retrieves the current state of the table.
   * @returns A promise that resolves to the current state of the table.
   */
  async table(): Promise<T> {
    const rljson = await this._core.dumpTable(this._tableKey);
    return rljson[this._tableKey] as T;
  }

  // ...........................................................................
  /**
   * Fetches a specific entry from the table by its reference or by a partial match.
   * @param where A string representing the reference of the entry to fetch, or an object representing a partial match.
   * @returns A promise that resolves to an array of entries matching the criteria, or null if no entries are found.
   */
  async get(where: string | Json, filter?: Json): Promise<Rljson> {
    if (typeof where === 'string') {
      return this._getByHash(where, filter);
    } else if (typeof where === 'object' && where !== null) {
      // If where is an object with only _hash property
      if (
        Object.keys(where).length === 1 &&
        '_hash' in where &&
        typeof where['_hash'] === 'string'
      ) {
        return this._getByHash(where['_hash'], filter);
      }

      // If where is an object, we assume it's a partial match
      return this._getByWhere(where, filter);
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
  protected async _getByHash(hash: string, filter?: Json): Promise<Rljson> {
    let result: Rljson = {};

    if (!filter || equals(filter, {})) {
      result = await this._core.readRow(this._tableKey, hash);
    } else {
      result = await this._core.readRows(this._tableKey, {
        _hash: hash,
        ...filter,
      } as { [column: string]: JsonValue });
    }

    return result;
  }

  // ...........................................................................
  /**
   * Fetches entries from the table that match the specified criteria.
   * @param where An object representing the criteria to match.
   * @returns A promise that resolves to an array of entries matching the criteria, or null if no entries are found.
   */
  protected async _getByWhere(where: Json, filter?: Json): Promise<Rljson> {
    const rows = await this._core.readRows(this._tableKey, {
      ...where,
      ...filter,
    } as { [column: string]: JsonValue });
    return rows;
  }

  // ...........................................................................
  /**
   * Gets the content type of the controller.
   * @returns The content type managed by the controller.
   */
  contentType(): ContentType {
    return this._contentType ?? 'components';
  }
}
