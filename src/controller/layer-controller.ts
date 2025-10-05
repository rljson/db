// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
import { hsh } from '@rljson/hash';
import { Json } from '@rljson/json';
// found in the LICENSE file in the root of this package.
import {
  ComponentRef,
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

import { Db } from '../db.ts';

import {
  Controller,
  ControllerCommands,
  ControllerRefs,
} from './controller.ts';

export type LayerControllerCommands = (ControllerCommands & 'add') | 'remove';

export interface LayerControllerRefs extends ControllerRefs {
  sliceIdsTable: TableKey;
  sliceIdsTableRow: SliceIdsRef;
  componentsTable: TableKey;
}

export class LayerController<N extends string>
  implements Controller<LayersTable, N>
{
  constructor(
    private readonly _db: Db,
    private readonly _tableKey: TableKey,
    private _refs?: LayerControllerRefs,
  ) {}

  async init() {
    // Validate Table

    // TableKey must end with 'Layer'
    if (this._tableKey.endsWith('Layer') === false) {
      throw new Error(
        `Table ${this._tableKey} is not supported by LayerController.`,
      );
    }

    // Table must be of type layers
    const rljson = await this._db.core.dumpTable(this._tableKey);
    const table = rljson[this._tableKey];
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
      const layersTable = table[this._tableKey] as LayersTable;
      const layer = layersTable._data[0] as LayerControllerRefs;
      this._refs = {
        sliceIdsTable: layer.sliceIdsTable,
        sliceIdsTableRow: layer.sliceIdsTableRow,
        componentsTable: layer.componentsTable,
      };
    }
  }

  async add(
    value: Record<SliceId, ComponentRef>,
    origin?: Ref,
    previous?: string[],
    refs?: LayerControllerRefs,
  ): Promise<EditProtocolRow<N>> {
    return this.run('add', value, origin, previous, refs || this._refs);
  }

  async remove(
    value: Record<SliceId, ComponentRef>,
    origin?: Ref,
    previous?: string[],
    refs?: LayerControllerRefs,
  ): Promise<EditProtocolRow<N>> {
    return this.run('remove', value, origin, previous, refs || this._refs);
  }

  async get(ref: string): Promise<Layer | null> {
    const row = await this._db.core.readRow(this._tableKey, ref);
    if (!row || !row[this._tableKey] || !row[this._tableKey]._data) {
      return null;
    }
    return row[this._tableKey]._data[0] as Layer;
  }

  async table(): Promise<LayersTable> {
    const rljson = await this._db.core.dumpTable(this._tableKey);
    if (!rljson[this._tableKey]) {
      throw new Error(`Table ${this._tableKey} does not exist.`);
    }
    return rljson[this._tableKey] as LayersTable;
  }

  async run(
    command: LayerControllerCommands,
    value: Record<SliceId, ComponentRef>,
    origin?: Ref,
    previous?: string[],
    refs?: LayerControllerRefs,
  ): Promise<EditProtocolRow<any>> {
    // Validate command
    if (command !== 'add' && command !== 'remove') {
      throw new Error(
        `Command ${command} is not supported by LayerController.`,
      );
    }

    // layer to add/remove
    const layer =
      command === 'add'
        ? {
            add: value,
            ...(refs || this._refs),
          }
        : ({
            add: {},
            remove: value,
            ...(refs || this._refs),
          } as Layer & { _hash?: string });

    const rlJson = { [this._tableKey]: { _data: [layer] } } as Rljson;

    //Write component to io
    await this._db.core.import(rlJson);

    //Create EditProtocolRow
    const result = {
      //Ref to component
      [this._tableKey + 'Ref']: hsh(layer as Json)._hash as string,

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
