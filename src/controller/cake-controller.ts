// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
import { hsh, rmhsh } from '@rljson/hash';
import { Json, JsonValue } from '@rljson/json';
// found in the LICENSE file in the root of this package.
import {
  Cake,
  CakesTable,
  InsertHistoryRow,
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

export interface CakeControllerRefs extends Partial<Cake> {
  sliceIdsTable: TableKey;
  sliceIdsRow: SliceIdsRef;
  base?: Ref;
}

export class CakeController<N extends string>
  extends BaseController<CakesTable>
  implements Controller<CakesTable, N>
{
  private _table: CakesTable | null = null;

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
    this._table = rljson[this._tableKey] as CakesTable;

    if (this._table._type !== 'cakes') {
      throw new Error(`Table ${this._tableKey} is not of type cakes.`);
    }

    // Validate refs or try to read them from the first row of the table
    if (this._refs && this._refs.base && this._refs.base.length > 0) {
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
    } else {
      // Try to read refs from first row of cakes table (Fallback)
      const cake = this._table._data[0] as CakeControllerRefs;
      this._refs = {
        sliceIdsTable: cake.sliceIdsTable,
        sliceIdsRow: cake.sliceIdsRow,
      };
    }
  }

  async getChildRefs(
    where: string | Json,
    filter?: Json,
  ): Promise<Array<{ tableKey: TableKey; ref: Ref }>> {
    /* v8 ignore next -- @preserve */
    if (!this._table) {
      throw new Error(`Controller not initialized.`);
    }

    const childRefs: Array<{ tableKey: TableKey; ref: Ref }> = [];
    const { [this._tableKey]: table } = await this.get(where, filter);

    const cakes = table._data as Cake[];
    for (const cake of cakes) {
      for (const layerTable of Object.keys(cake.layers)) {
        if (layerTable.startsWith('_')) continue; // Skip internal keys
        childRefs.push({
          tableKey: layerTable as TableKey,
          ref: cake.layers[layerTable],
        });
      }
    }

    return childRefs;
  }

  async insert(
    command: CakeControllerCommands,
    value: Json,
    origin?: Ref,
    refs?: ControllerRefs,
  ): Promise<InsertHistoryRow<any>[]> {
    // Validate command
    if (!command.startsWith('add')) {
      throw new Error(`Command ${command} is not supported by CakeController.`);
    }

    if (this._refs?.base) delete this._refs.base; // Remove base ref to avoid conflicts

    const normalizedValue: { [layerTable: string]: string } = {};
    for (const [layerTable, layerRef] of Object.entries(
      value as { [layerTable: string]: string },
    )) {
      if (Array.isArray(layerRef) && layerRef.length > 1) {
        throw new Error(
          `CakeController insert: Layer ref for table ${layerTable} cannot be an array of size > 1. No 1:n relations supported.`,
        );
      }

      normalizedValue[layerTable] = Array.isArray(layerRef)
        ? layerRef[0]
        : layerRef;
    }

    // Overwrite base layers with given layers
    const cake = {
      layers: { ...this._baseLayers, ...normalizedValue },
      ...(refs || this._refs),
    };

    const rlJson = { [this._tableKey]: { _data: [cake] } } as Rljson;

    //Write component to io
    await this._core.import(rlJson);

    //Create InsertHistoryRow
    const result = {
      //Ref to component
      [this._tableKey + 'Ref']: hsh(cake as Json)._hash as string,

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
    } else if (typeof where === 'object' && where !== null) {
      return this._getByWhere(where, filter);
    } else {
      return Promise.resolve({});
    }
  }

  filterRow(row: Json, key: string, value: JsonValue): boolean {
    const cake = row as Cake;
    for (const [layerKey, layerRef] of Object.entries(cake.layers)) {
      if (layerKey === key && layerRef === value) {
        return true;
      }
    }
    return false;
  }
}
