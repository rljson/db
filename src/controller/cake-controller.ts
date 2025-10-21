// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
import { hsh, rmhsh } from '@rljson/hash';
import { Json, JsonValue } from '@rljson/json';
// found in the LICENSE file in the root of this package.
import {
  Cake, CakesTable, HistoryRow, LayerRef, Ref, Rljson, SliceIdsRef, TableKey, timeId
} from '@rljson/rljson';

import { Core } from '../core.ts';

import { BaseController } from './base-controller.ts';
import { Controller, ControllerCommands, ControllerRefs } from './controller.ts';


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

  private _baseLayers: { [layerTable: string]: string } = {};

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

    if (table._type !== 'cakes') {
      throw new Error(`Table ${this._tableKey} is not of type cakes.`);
    }

    // Validate refs or try to read them from the first row of the table
    if (this._refs && this._refs.base) {
      // Validate base cake exists
      const {
        [this._tableKey]: { _data: baseCakes },
      } = await this._core.readRow(this._tableKey, this._refs.base);

      // Base cake must exist
      if (baseCakes.length === 0) {
        throw new Error(`Base cake ${this._refs.base} does not exist.`);
      }

      const baseCake = baseCakes[0] as Cake;

      // Store base layers from base cake
      this._baseLayers = rmhsh(baseCake.layers);

      // Try to read sliceIds from base cake if not provided
      if (!this._refs.sliceIdsTable || !this._refs.sliceIdsRow) {
        this._refs = {
          sliceIdsTable: baseCake.sliceIdsTable,
          sliceIdsRow: baseCake.sliceIdsRow,
        };
      }
    } else {
      // Try to read refs from first row of cakes table (Fallback)
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
  ): Promise<HistoryRow<any>> {
    // Validate command
    if (!command.startsWith('add')) {
      throw new Error(`Command ${command} is not supported by CakeController.`);
    }

    // Overwrite base layers with given layers
    const cake = {
      layers: { ...this._baseLayers, ...value },
      ...(refs || this._refs),
    };

    const rlJson = { [this._tableKey]: { _data: [cake] } } as Rljson;

    //Write component to io
    await this._core.import(rlJson);

    //Create HistoryRow
    const result = {
      //Ref to component
      [this._tableKey + 'Ref']: hsh(cake as Json)._hash as string,

      //Data from edit
      route: '',
      origin,

      //Unique id/timestamp
      timeId: timeId(),
    } as HistoryRow<any>;

    return result;
  }

  async get(where: string | Json, filter?: Json): Promise<Rljson> {
    if (typeof where === 'string') {
      return this._getByHash(where, filter);
    } else if (typeof where === 'object' && where !== null) {
      // If where is an object, we assume it's a partial match
      const keys = Object.keys(where);
      if (keys.length === 1 && keys[0].endsWith('Ref')) {
        // If the only key is the tableRef, we can use the _getByRef method
        const tableKey = keys[0].replace('Ref', '') as TableKey;
        return this._getByRef(tableKey, where[keys[0]] as Ref, filter);
      } else {
        return this._getByWhere(where, filter);
      }
    } else {
      return Promise.resolve({});
    }
  }

  protected async _getByRef(
    tableKey: TableKey,
    ref: Ref,
    filter?: Json,
  ): Promise<Rljson> {
    //Prefilter if filter is set
    let table: Rljson = {};
    if (!!filter && Object.keys(filter).length > 0) {
      table = await this._core.readRows(
        this._tableKey,
        filter as { [column: string]: JsonValue },
      );
    } else {
      table = await this._core.dumpTable(this._tableKey);
    }

    const cakes = [];
    for (const row of table[this._tableKey]._data) {
      const cake = row as Cake;
      const layers = cake.layers;

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
