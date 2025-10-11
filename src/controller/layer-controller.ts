// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
import { hsh } from '@rljson/hash';
import { Json } from '@rljson/json';
// found in the LICENSE file in the root of this package.
import {
  ComponentRef,
  EditCommand,
  EditProtocolRow,
  Layer,
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

export interface LayerControllerRefs extends ControllerRefs {
  sliceIdsTable?: TableKey;
  sliceIdsTableRow?: SliceIdsRef;
  componentsTable?: TableKey;
}

export class LayerController<N extends string>
  extends BaseController<LayersTable>
  implements Controller<LayersTable, N>
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
    if (!table) {
      throw new Error(`Table ${this._tableKey} does not exist.`);
    }
    if (table._type !== 'layers') {
      throw new Error(`Table ${this._tableKey} is not of type layers.`);
    }

    // Validate Refs
    if (this._refs) {
      if (
        !this._refs.sliceIdsTable ||
        !this._refs.componentsTable ||
        !this._refs.sliceIdsTableRow
      ) {
        throw new Error(
          'LayerController refs are not complete. Please provide sliceIdsTable, sliceIdsTableRow and componentsTable.',
        );
      }
    } else {
      // Try to read refs from first row of layers table
      const layer = table._data[0] as LayerControllerRefs;
      this._refs = {
        sliceIdsTable: layer.sliceIdsTable,
        sliceIdsTableRow: layer.sliceIdsTableRow,
        componentsTable: layer.componentsTable,
      };
    }
  }

  async run(
    command: EditCommand,
    value: Json,
    origin?: Ref,
    refs?: LayerControllerRefs,
  ): Promise<EditProtocolRow<any>> {
    // Validate command
    if (!command.startsWith('add') && !command.startsWith('remove')) {
      throw new Error(
        `Command ${command} is not supported by LayerController.`,
      );
    }

    // layer to add/remove
    const layer =
      command.startsWith('add') === true
        ? {
            add: value as Record<SliceId, ComponentRef>,
            ...(refs || this._refs),
          }
        : ({
            add: {},
            remove: value as Record<SliceId, ComponentRef>,
            ...(refs || this._refs),
          } as Layer & { _hash?: string });

    const rlJson = { [this._tableKey]: { _data: [layer] } } as Rljson;

    //Write component to io
    await this._core.import(rlJson);

    //Create EditProtocolRow
    const result = {
      //Ref to component
      [this._tableKey + 'Ref']: hsh(layer as Json)._hash as string,

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
    const layers = [];
    for (const row of table[this._tableKey]._data) {
      const layer = row as Layer;
      // Only consider layers that belong to the requested components table
      if (layer.componentsTable !== tableKey) {
        continue;
      }
      // Check if the layer added the requested ref
      for (const layerRef of Object.values(layer.add)) {
        if (layerRef === ref) {
          layers.push(layer);
        }
      }
    }
    return {
      [this._tableKey]: { _data: layers, _type: 'layers' } as LayersTable,
    };
  }
}
