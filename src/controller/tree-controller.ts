// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
import { hsh } from '@rljson/hash';
import { Json } from '@rljson/json';
// found in the LICENSE file in the root of this package.
import {
  InsertCommand,
  InsertHistoryRow,
  Ref,
  Rljson,
  TableKey,
  timeId,
  Tree,
  TreesTable,
} from '@rljson/rljson';

import { Core } from '../core.ts';

import { BaseController } from './base-controller.ts';
import { Controller, ControllerChildProperty } from './controller.ts';

export class TreeController<N extends string, C extends Tree>
  extends BaseController<TreesTable, C>
  implements Controller<TreesTable, C, N>
{
  constructor(
    protected readonly _core: Core,
    protected readonly _tableKey: TableKey,
  ) {
    super(_core, _tableKey);
    this._contentType = 'trees';
  }

  async init() {
    // Validate Table

    // TableKey must end with 'Tree'
    if (this._tableKey.endsWith('Tree') === false) {
      throw new Error(
        `Table ${this._tableKey} is not supported by TreeController.`,
      );
    }

    // Table must be of type trees
    const contentType = await this._core.contentType(this._tableKey);
    /* v8 ignore next -- @preserve */
    if (contentType !== 'trees') {
      throw new Error(`Table ${this._tableKey} is not of type trees.`);
    }

    //Get TableCfg
    this._tableCfg = await this._core.tableCfg(this._tableKey);
  }

  async insert(
    command: InsertCommand,
    value: Tree,
    origin?: Ref,
  ): Promise<InsertHistoryRow<any>[]> {
    // Validate command
    if (!command.startsWith('add') && !command.startsWith('remove')) {
      throw new Error(`Command ${command} is not supported by TreeController.`);
    }

    const rlJson = { [this._tableKey]: { _data: [value] } } as Rljson;

    //Write component to io
    await this._core.import(rlJson);

    //Create InsertHistoryRow
    const result = {
      //Ref to component
      [this._tableKey + 'Ref']: hsh(value as Json)._hash as string,

      //Data from edit
      route: '',
      origin,

      //Unique id/timestamp
      timeId: timeId(),
    } as InsertHistoryRow<any>;

    return [result];
  }

  async get(where: string | Json, filter?: Json): Promise<Rljson> {
    if (typeof where === 'string') {
      return this._getByHash(where, filter);
    } else {
      return this._getByWhere(where, filter);
    }
  }

  /* v8 ignore next -- @preserve */
  async getChildRefs(): Promise<ControllerChildProperty[]> {
    return [];
  }

  /* v8 ignore next -- @preserve */
  async filterRow(): Promise<boolean> {
    return false;
  }
}
