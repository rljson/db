// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
import { hsh } from '@rljson/hash';
import { Json, JsonValue } from '@rljson/json';
// found in the LICENSE file in the root of this package.
import {
  ComponentRef,
  InsertCommand,
  InsertHistoryRow,
  Layer,
  LayerRef,
  LayersTable,
  Ref,
  Rljson,
  SliceId,
  SliceIdsRef,
  TableKey,
  timeId,
} from '@rljson/rljson';

import { Core } from '../core.ts';

import { BaseController } from './base-controller.ts';
import { Controller, ControllerRefs } from './controller.ts';

export interface LayerControllerRefs extends Partial<Layer> {
  base?: LayerRef;
  sliceIdsTable: TableKey;
  sliceIdsTableRow: SliceIdsRef;
  componentsTable: TableKey;
}

export class LayerController<N extends string, C extends Record<string, string>>
  extends BaseController<LayersTable, C>
  implements Controller<LayersTable, C, N>
{
  constructor(
    protected readonly _core: Core,
    protected readonly _tableKey: TableKey,
    private _refs?: LayerControllerRefs,
  ) {
    super(_core, _tableKey);
  }

  async init() {
    // Validate Table

    // TableKey must end with 'Layer'
    if (this._tableKey.endsWith('Layer') === false) {
      throw new Error(
        `Table ${this._tableKey} is not supported by LayerController.`,
      );
    }

    // Table must be of type layers
    const rljson = await this._core.dumpTable(this._tableKey);
    const table = rljson[this._tableKey] as LayersTable;
    if (table._type !== 'layers') {
      throw new Error(`Table ${this._tableKey} is not of type layers.`);
    }

    // Validate refs or try to read them from the first row of the table
    if (this._refs && this._refs.base) {
      // Validate base layer exists
      const {
        [this._tableKey]: { _data: baseLayers },
      } = await this._core.readRow(this._tableKey, this._refs.base);

      // Base layer must exist
      if (baseLayers.length === 0) {
        throw new Error(`Base layer ${this._refs.base} does not exist.`);
      }

      const baseLayer = baseLayers[0] as Layer;

      // Try to read sliceIds from base layer if not provided directly
      /* v8 ignore next if -- @preserve */
      if (
        !this._refs.sliceIdsTable ||
        !this._refs.sliceIdsRow ||
        !this._refs.componentsTable
      ) {
        this._refs = {
          sliceIdsTable: baseLayer.sliceIdsTable,
          sliceIdsTableRow: baseLayer.sliceIdsTableRow,
          componentsTable: baseLayer.componentsTable,
        };
      }
    } else {
      // Try to read refs from first row of layers table (Fallback)
      // TODO: THIS MUST BE TIME CONSIDERED!!!
      const layer = table._data[0] as LayerControllerRefs;
      if (!!layer) {
        this._refs = {
          sliceIdsTable: layer.sliceIdsTable,
          sliceIdsTableRow: layer.sliceIdsTableRow,
          componentsTable: layer.componentsTable,
        };
      }
    }
  }

  async insert(
    command: InsertCommand,
    value: C,
    origin?: Ref,
    refs?: ControllerRefs,
  ): Promise<InsertHistoryRow<any>[]> {
    // Validate command
    if (!command.startsWith('add') && !command.startsWith('remove')) {
      throw new Error(
        `Command ${command} is not supported by LayerController.`,
      );
    }

    const normalizedValue: Record<SliceId, ComponentRef> = {};
    for (const [sliceId, compRef] of Object.entries(
      value as Record<string, any>,
    )) {
      /* v8 ignore next -- @preserve */
      if (Array.isArray(compRef) && compRef.length > 1) {
        throw new Error(
          `LayerController insert: Component ref for slice ${sliceId} cannot be an array of size > 1. No 1:n relations supported.`,
        );
      }
      normalizedValue[sliceId] = Array.isArray(compRef)
        ? compRef[0]
        : (compRef as ComponentRef);
    }

    // layer to add/remove
    const layer =
      command.startsWith('add') === true
        ? {
            add: normalizedValue as Record<SliceId, ComponentRef>,
            ...(refs || this._refs),
          }
        : ({
            add: {},
            remove: normalizedValue as Record<SliceId, ComponentRef>,
            ...(refs || this._refs),
          } as Layer & { _hash?: string });

    const rlJson = { [this._tableKey]: { _data: [layer] } } as Rljson;

    //Write component to io
    await this._core.import(rlJson);

    //Create InsertHistoryRow
    const result = {
      //Ref to component
      [this._tableKey + 'Ref']: hsh(layer as Json)._hash as string,

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

  async getChildRefs(
    where: string | Json,
    filter?: Json,
  ): Promise<Array<{ tableKey: TableKey; ref: Ref }>> {
    const { [this._tableKey]: table } = await this.get(where, filter);
    const childRefs: Array<{ tableKey: TableKey; ref: Ref }> = [];

    //TODO: Implement layer sorted loop by timeId of InsertHistory
    for (const row of table._data) {
      const layer = row as Layer;

      for (const [sliceId, compRef] of Object.entries(layer.add)) {
        if (sliceId.startsWith('_')) continue;

        childRefs.push({
          tableKey: layer.componentsTable,
          ref: compRef,
        });
      }

      // TODO: CONTINUE HERE
      // USE BASE PROPERTY
      // const baseChildRefs = await this.getBaseChildRefs(layer._hash as string);
      // debugger;
    }

    return childRefs;
  }

  filterRow(row: Json, _: string, value: JsonValue): boolean {
    const layer = row as Layer;
    const compRef = value as ComponentRef;

    for (const componentRef of Object.values(layer.add)) {
      if (componentRef === compRef) {
        return true;
      }
    }

    return false;
  }
}
