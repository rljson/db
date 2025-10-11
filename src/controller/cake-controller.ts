// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
import { hsh, rmhsh } from '@rljson/hash';
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

import { Core } from '../core.ts';

import { BaseController } from './base-controller.ts';
import {
  Controller,
  ControllerCommands,
  ControllerRefs,
} from './controller.ts';

export interface CakeValue extends Json {
  layers: {
    [layerTable: TableKey]: LayerRef;
  };
  id?: string;
}

export type CakeControllerCommands = ControllerCommands | `add@${string}`;

export interface CakeControllerRefs extends ControllerRefs {
  sliceIdsTable?: TableKey;
  sliceIdsRow?: SliceIdsRef;
}

export class CakeController<N extends string>
  extends BaseController<CakesTable>
  implements Controller<CakesTable, N>
{
  constructor(
    protected readonly _core: Core,
    protected readonly _tableKey: TableKey,
    private _refs?: CakeControllerRefs,
  ) {
    super(_core, _tableKey);
  }

  async init() {
    // Validate Table

    // TableKey must end with 'Cake'
    if (this._tableKey.endsWith('Cake') === false) {
      throw new Error(
        `Table ${this._tableKey} is not supported by CakeController.`,
      );
    }

    // Table must be of type cakes
    const rljson = await this._core.dumpTable(this._tableKey);
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

  async run(
    command: CakeControllerCommands,
    value: Json,
    origin?: Ref,
    refs?: CakeControllerRefs,
  ): Promise<EditProtocolRow<any>> {
    // Validate command
    if (!command.startsWith('add')) {
      throw new Error(`Command ${command} is not supported by CakeController.`);
    }

    // Merge with given base ref
    const baseCakeRef = command.split('@')[1];
    let baseCakeLayers: {
      [layerTable: string]: string;
    } = {};
    if (!!baseCakeRef) {
      //Get base cake
      const { [this._tableKey]: baseCakesTable } = await this._core.readRow(
        this._tableKey,
        baseCakeRef,
      );
      const baseCake = baseCakesTable._data?.[0] as Cake;
      baseCakeLayers = rmhsh(baseCake.layers) || {};
    }

    // Cake to add
    const cake = {
      layers: { ...baseCakeLayers, ...value },
      ...(refs || this._refs),
    };

    const rlJson = { [this._tableKey]: { _data: [cake] } } as Rljson;

    //Write component to io
    await this._core.import(rlJson);

    //Create EditProtocolRow
    const result = {
      //Ref to component
      [this._tableKey + 'Ref']: hsh(cake as Json)._hash as string,

      //Data from edit
      route: '',
      origin,

      //Unique id/timestamp
      timeId: timeId(),
    } as EditProtocolRow<any>;

    return result;
  }

  async get(where: string | Json): Promise<Rljson> {
    if (typeof where === 'string') {
      return this._getByHash(where);
    } else if (typeof where === 'object' && where !== null) {
      // If where is an object, we assume it's a partial match
      const keys = Object.keys(where);
      if (keys.length === 1 && keys[0].endsWith('Ref')) {
        // If the only key is the tableRef, we can use the _getByRef method
        const tableKey = keys[0].replace('Ref', '') as TableKey;
        return this._getByRef(tableKey, where[keys[0]] as Ref);
      } else {
        return this._getByWhere(where);
      }
    } else {
      return Promise.resolve({});
    }
  }

  protected async _getByRef(tableKey: TableKey, ref: Ref): Promise<Rljson> {
    const table = await this._core.dumpTable(this._tableKey);
    const cakes = [];
    for (const row of table[this._tableKey]._data) {
      const cake = row as Cake;
      const layers = cake.layers || {};

      for (const layerTable of Object.keys(layers)) {
        if (layerTable === tableKey && layers[layerTable] === ref) {
          cakes.push(cake);
        }
      }
    }
    return {
      [this._tableKey]: { _data: cakes, _type: 'cakes' } as CakesTable,
    };
  }
}
