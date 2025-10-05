// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
import { hsh } from '@rljson/hash';
import { Json } from '@rljson/json';
// found in the LICENSE file in the root of this package.
import {
  Cake,
  CakesTable,
  EditProtocolRow,
  LayerRef,
  Ref,
  Rljson,
  SliceIdsRef,
  TableKey,
  timeId,
} from '@rljson/rljson';

import { Db } from '../db.ts';

import {
  Controller,
  ControllerCommands,
  ControllerRefs,
} from './controller.ts';

export interface CakeValue extends Partial<Cake> {
  layers: {
    [layerTable: TableKey]: LayerRef;
  };
  id?: string;
}

export type CakeControllerCommands = ControllerCommands;

export interface CakeControllerRefs extends ControllerRefs {
  sliceIdsTable: TableKey;
  sliceIdsRow: SliceIdsRef;
}

export class CakeController<N extends string>
  implements Controller<CakesTable, N>
{
  constructor(
    private readonly _db: Db,
    private readonly _tableKey: TableKey,
    private _refs?: CakeControllerRefs,
  ) {}

  async init() {
    // Validate Table

    // TableKey must end with 'Cake'
    if (this._tableKey.endsWith('Cake') === false) {
      throw new Error(
        `Table ${this._tableKey} is not supported by CakeController.`,
      );
    }

    // Table must be of type cakes
    const rljson = await this._db.core.dumpTable(this._tableKey);
    const table = rljson[this._tableKey] as CakesTable;
    if (!table) {
      throw new Error(`Table ${this._tableKey} does not exist.`);
    }
    if (table._type !== 'cakes') {
      throw new Error(`Table ${this._tableKey} is not of type cakes.`);
    }

    // Validate Refs
    if (this._refs) {
      if (!this._refs.sliceIdsTable || !this._refs.sliceIdsRow) {
        throw new Error(
          'Refs are not complete on CakeController. Required: sliceIdsTable, sliceIdsRow',
        );
      }
    } else {
      // Try to read refs from first row of cakes table
      const cake = table._data[0] as CakeControllerRefs;
      this._refs = {
        sliceIdsTable: cake.sliceIdsTable,
        sliceIdsRow: cake.sliceIdsRow,
      };
    }
  }

  async add(
    value: CakeValue,
    origin?: Ref,
    previous?: string[],
    refs?: CakeControllerRefs,
  ): Promise<EditProtocolRow<N>> {
    return this.run('add', value, origin, previous, refs || this._refs);
  }

  async remove(): Promise<EditProtocolRow<N>> {
    throw new Error(`Remove is not supported on CakeController.`);
  }

  async get(ref: string): Promise<Cake | null> {
    const row = await this._db.core.readRow(this._tableKey, ref);
    if (!row || !row[this._tableKey] || !row[this._tableKey]._data) {
      return null;
    }
    return row[this._tableKey]._data[0] as Cake;
  }

  async table(): Promise<CakesTable> {
    const rljson = await this._db.core.dumpTable(this._tableKey);
    if (!rljson[this._tableKey]) {
      throw new Error(`Table ${this._tableKey} does not exist.`);
    }
    return rljson[this._tableKey] as CakesTable;
  }

  async run(
    command: CakeControllerCommands,
    value: CakeValue,
    origin?: Ref,
    previous?: string[],
    refs?: CakeControllerRefs,
  ): Promise<EditProtocolRow<any>> {
    // Validate command
    if (command !== 'add') {
      throw new Error(`Command ${command} is not supported by CakeController.`);
    }

    // Cake to add
    const cake = {
      ...value,
      ...(refs || this._refs),
    };

    const rlJson = { [this._tableKey]: { _data: [cake] } } as Rljson;

    //Write component to io
    await this._db.core.import(rlJson);

    //Create EditProtocolRow
    const result = {
      //Ref to component
      [this._tableKey + 'Ref']: hsh(cake as Json)._hash as string,

      //Data from edit
      route: '',
      origin,
      previous,

      //Unique id/timestamp
      timeId: timeId(),
    } as EditProtocolRow<any>;

    return result;
  }
}
