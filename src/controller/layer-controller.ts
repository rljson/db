// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
import { hsh, rmhsh } from '@rljson/hash';
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
  SliceIds,
  SliceIdsRef,
  TableKey,
  timeId,
} from '@rljson/rljson';

import { Core } from '../core.ts';

import { BaseController } from './base-controller.ts';
import {
  Controller,
  ControllerChildProperty,
  ControllerRefs,
} from './controller.ts';
import { SliceIdController } from './slice-id-controller.ts';

export interface LayerControllerRefs extends Partial<Layer> {
  base?: LayerRef;
  sliceIdsTable: TableKey;
  sliceIdsTableRow: SliceIdsRef;
  componentsTable: TableKey;
}

export class LayerController<N extends string, C extends Layer>
  extends BaseController<LayersTable, C>
  implements Controller<LayersTable, C, N>
{
  constructor(
    protected readonly _core: Core,
    protected readonly _tableKey: TableKey,
    private _refs?: LayerControllerRefs,
  ) {
    super(_core, _tableKey);
    this._contentType = 'layers';
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
    const contentType = await this._core.contentType(this._tableKey);
    if (contentType !== 'layers') {
      throw new Error(`Table ${this._tableKey} is not of type layers.`);
    }

    //Get TableCfg
    this._tableCfg = await this._core.tableCfg(this._tableKey);

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
    const isAdd = command.startsWith('add');

    const normalizedValue: Record<SliceId, ComponentRef> = {};
    for (const [sliceId, compRef] of isAdd
      ? Object.entries(value.add as Record<string, any>)
      : Object.entries(value.remove as Record<string, any>)) {
      /* v8 ignore next -- @preserve */
      if (Array.isArray(compRef) && compRef.length > 1) {
        throw new Error(
          `LayerController insert: Component ref for slice ${sliceId} cannot be an array of size > 1. No 1:n relations supported.`,
        );
      }
      /* v8 ignore next -- @preserve */
      normalizedValue[sliceId] = Array.isArray(compRef)
        ? compRef[0]
        : (compRef as ComponentRef);
    }

    // layer to add/remove
    const layer = isAdd
      ? {
          ...value,
          ...{
            add: normalizedValue as Record<SliceId, ComponentRef>,
            remove: {},
          },
          ...refs,
        }
      : {
          ...value,
          ...{
            remove: normalizedValue as Record<SliceId, ComponentRef>,
            add: {},
          },
          ...refs,
        };

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

  async resolveBaseLayer(layer: Layer): Promise<{
    add: Record<string, string>;
    sliceIds: SliceId[];
  }> {
    const add = new Map<string, string>();
    const sliceIds: Set<SliceId> = new Set<SliceId>();

    if (!!layer.base) {
      // Get base layer first
      const baseLayer = await this.get(layer.base);

      /* v8 ignore next -- @preserve */
      if (!baseLayer[this._tableKey]?._data?.[0]) {
        throw new Error(`Base layer ${layer.base} does not exist.`);
      }
      /* v8 ignore next -- @preserve */
      if (baseLayer[this._tableKey]._data.length > 1) {
        throw new Error(
          `Base layer ${layer.base} resolving not possible. Not unique.`,
        );
      }

      // Get base layer chained layers recursively
      const baseLayerData = rmhsh(baseLayer[this._tableKey]._data[0]) as Layer;
      const baseLayerResolved = await this.resolveBaseLayer(baseLayerData);

      // Merge base layer's add components
      for (const [sliceId, compRef] of Object.entries(baseLayerResolved.add)) {
        /* v8 ignore next -- @preserve */
        if (sliceId.startsWith('_')) continue;

        add.set(sliceId, compRef);
      }

      // Merge base layer's sliceIds
      for (const sliceId of baseLayerResolved.sliceIds) {
        sliceIds.add(sliceId);
      }

      // Get sliceIds from base layer's sliceIds
      const baseLayerSliceIdsTable = baseLayerData.sliceIdsTable;
      const baseLayerSliceIdsRow = baseLayerData.sliceIdsTableRow;

      // Get sliceIds from base layer's sliceIds table
      const {
        [baseLayerSliceIdsTable]: { _data: baseLayerSliceIds },
      } = await this._core.readRow(
        baseLayerSliceIdsTable,
        baseLayerSliceIdsRow,
      );

      // Resolve base layer sliceIds recursively
      for (const sIds of baseLayerSliceIds as SliceIds[]) {
        //Resolve base SliceIds
        const sliceIdController = new SliceIdController(
          this._core,
          baseLayerSliceIdsTable,
        );
        const resolvedSliceIds = await sliceIdController.resolveBaseSliceIds(
          sIds,
        );

        // Merge resolved sliceIds
        for (const sId of resolvedSliceIds.add) {
          /* v8 ignore next -- @preserve */
          if (sId.startsWith('_')) continue;

          sliceIds.add(sId);
        }
      }
    }

    // Get sliceIds from current layer's sliceIds table
    const {
      [layer.sliceIdsTable]: { _data: layerSliceIds },
    } = await this._core.readRow(layer.sliceIdsTable, layer.sliceIdsTableRow);

    /* v8 ignore next -- @preserve */
    if (!layerSliceIds || layerSliceIds.length === 0) {
      throw new Error(
        `Layer sliceIds ${layer.sliceIdsTableRow} does not exist.`,
      );
    }
    /* v8 ignore next -- @preserve */
    if (layerSliceIds.length > 1) {
      throw new Error(
        `Layer sliceIds ${layer.sliceIdsTableRow} has more than one entry.`,
      );
    }

    const layerSliceId = layerSliceIds[0] as SliceIds;

    for (const sId of layerSliceId.add) {
      /* v8 ignore next -- @preserve */
      if (sId.startsWith('_')) continue;

      sliceIds.add(sId);
    }

    /* v8 ignore next -- @preserve */
    if (!!layerSliceId.remove)
      for (const sId of Object.keys(layerSliceId.remove)) {
        if (sliceIds.has(sId)) {
          sliceIds.delete(sId);
        }
      }

    for (const [sliceId, compRef] of Object.entries(layer.add)) {
      if (sliceId.startsWith('_')) continue;

      add.set(sliceId, compRef);
    }

    // Remove sliceIds that are both in add and remove
    /* v8 ignore next -- @preserve */
    if (!!layer.remove)
      for (const sliceId of Object.keys(layer.remove)) {
        if (add.has(sliceId)) {
          add.delete(sliceId);
        }
      }

    return { add: Object.fromEntries(add), sliceIds: Array.from(sliceIds) };
  }

  async getChildRefs(
    where: string | Json,
    filter?: Json,
  ): Promise<ControllerChildProperty[]> {
    const { [this._tableKey]: table } = await this.get(where, filter);
    const childRefs: ControllerChildProperty[] = [];

    for (const row of table._data) {
      const layer = row as Layer;
      const resolvedLayer = await this.resolveBaseLayer(layer);

      for (const [sliceId, ref] of Object.entries(resolvedLayer.add)) {
        /* v8 ignore next -- @preserve */ /* v8 ignore next -- @preserve */
        if (sliceId.startsWith('_')) continue;

        childRefs.push({
          tableKey: layer.componentsTable,
          ref,
          sliceIds: [sliceId],
        });
      }
    }

    return childRefs;
  }

  async filterRow(row: Json, _: string, value: JsonValue): Promise<boolean> {
    const layer = row as Layer;
    const compRef = value as ComponentRef;
    const resolvedLayer = await this.resolveBaseLayer(layer);

    for (const componentRef of Object.values(resolvedLayer.add)) {
      if (componentRef === compRef) {
        return true;
      }
    }

    return false;
  }
}
