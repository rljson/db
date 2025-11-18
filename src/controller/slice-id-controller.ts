// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
import { hsh } from '@rljson/hash';
import { Json, JsonValue } from '@rljson/json';
// found in the LICENSE file in the root of this package.
import {
  InsertCommand,
  InsertHistoryRow,
  Ref,
  Rljson,
  SliceId,
  SliceIds,
  SliceIdsRef,
  SliceIdsTable,
  TableKey,
  timeId,
} from '@rljson/rljson';

import { Core } from '../core.ts';

import { BaseController } from './base-controller.ts';
import { Controller, ControllerRefs } from './controller.ts';

export interface SliceIdControllerRefs extends Partial<SliceIds> {
  base?: SliceIdsRef;
}

export class SliceIdController<N extends string, C extends SliceId[]>
  extends BaseController<SliceIdsTable, C>
  implements Controller<SliceIdsTable, C, N>
{
  constructor(
    protected readonly _core: Core,
    protected readonly _tableKey: TableKey,
    private _refs?: SliceIdControllerRefs,
  ) {
    super(_core, _tableKey);
    this._contentType = 'sliceIds';
  }

  async init() {
    // Validate Table

    // TableKey must end with 'SliceId'
    if (this._tableKey.endsWith('SliceId') === false) {
      throw new Error(
        `Table ${this._tableKey} is not supported by SliceIdController.`,
      );
    }

    // Table must be of type sliceIds
    const rljson = await this._core.dumpTable(this._tableKey);
    const table = rljson[this._tableKey] as SliceIdsTable;
    if (table._type !== 'sliceIds') {
      throw new Error(`Table ${this._tableKey} is not of type sliceIds.`);
    }

    // Validate refs or try to read them from the first row of the table
    if (this._refs && this._refs.base) {
      // Validate base sliceId exists
      const {
        [this._tableKey]: { _data: SliceIds },
      } = await this._core.readRow(this._tableKey, this._refs.base);

      // Base sliceId must exist
      if (SliceIds.length === 0) {
        throw new Error(`Base sliceId ${this._refs.base} does not exist.`);
      }
    } else {
      // Try to read refs from first row of sliceIds table (Fallback)
      // TODO: THIS MUST BE TIME CONSIDERED!!!
      const sliceId = table._data[0] as SliceIdControllerRefs;
      if (!!sliceId) {
        this._refs = {
          base: sliceId.base,
        };
      }
    }
  }

  async insert(
    command: InsertCommand,
    value: SliceId[],
    origin?: Ref,
    refs?: ControllerRefs,
  ): Promise<InsertHistoryRow<any>[]> {
    // Validate command
    if (!command.startsWith('add') && !command.startsWith('remove')) {
      throw new Error(
        `Command ${command} is not supported by SliceIdController.`,
      );
    }

    // sliceIds to add/remove
    const sliceIds =
      command.startsWith('add') === true
        ? ({
            add: value,
            ...(refs || this._refs),
          } as SliceIds & { _hash?: string })
        : ({
            add: [],
            remove: value,
            ...(refs || this._refs),
          } as SliceIds & { _hash?: string });

    const rlJson = { [this._tableKey]: { _data: [sliceIds] } } as Rljson;

    //Write component to io
    await this._core.import(rlJson);

    //Create InsertHistoryRow
    const result = {
      //Ref to component
      [this._tableKey + 'Ref']: hsh(sliceIds as Json)._hash as string,

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

  async resolveBaseSliceIds(sliceIds: SliceIds): Promise<{
    add: SliceId[];
  }> {
    const add = new Set<SliceId>();
    const remove = new Set<SliceId>();

    if (!!sliceIds.base) {
      const baseSliceIds = await this.get(sliceIds.base);

      if (!baseSliceIds[this._tableKey]?._data?.[0]) {
        throw new Error(`Base sliceIds ${sliceIds.base} does not exist.`);
      }
      if (baseSliceIds[this._tableKey]._data.length > 1) {
        throw new Error(
          `Base sliceIds ${sliceIds.base} has more than one entry.`,
        );
      }

      const baseSliceId = baseSliceIds[this._tableKey]._data[0] as SliceIds;
      const resolvedBaseSliceIds = await this.resolveBaseSliceIds(baseSliceId);

      for (const sliceId of resolvedBaseSliceIds.add) {
        add.add(sliceId);
      }
    }

    for (const sliceId of sliceIds.add) {
      add.add(sliceId);
    }

    if (!!sliceIds.remove)
      for (const sliceId of sliceIds.remove) {
        remove.add(sliceId);
      }

    // Remove sliceIds that are both in add and remove
    for (const sliceId of remove.values()) {
      if (add.has(sliceId)) {
        add.delete(sliceId);
      }
    }

    return { add: Array.from(add) };
  }

  async getChildRefs(): Promise<Array<{ tableKey: TableKey; ref: Ref }>> {
    return [];
  }

  async filterRow(row: Json, _: string, value: JsonValue): Promise<boolean> {
    const sliceIds = row as SliceIds;
    const sliceId = value as SliceId;

    for (const sId of Object.values(sliceIds.add)) {
      if (sliceId === sId) {
        return true;
      }
    }

    return false;
  }
}
