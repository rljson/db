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
import {
  Controller,
  ControllerChildProperty,
  ControllerRefs,
} from './controller.ts';

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
    const contentType = await this._core.contentType(this._tableKey);
    /* v8 ignore next -- @preserve */
    if (contentType !== 'sliceIds') {
      throw new Error(`Table ${this._tableKey} is not of type sliceIds.`);
    }

    //Get TableCfg
    this._tableCfg = await this._core.tableCfg(this._tableKey);

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

  /**
   * Resolved sliceIds by content hash. SliceIds rows are immutable,
   * so a resolved result stays valid for the controller's lifetime.
   */
  private readonly _resolvedSliceIds = new Map<string, { add: SliceId[] }>();

  async resolveBaseSliceIds(sliceIds: SliceIds): Promise<{
    add: SliceId[];
  }> {
    const sliceIdsHash = (sliceIds as any)._hash as string | undefined;
    if (sliceIdsHash) {
      const cached = this._resolvedSliceIds.get(sliceIdsHash);
      if (cached) {
        return cached;
      }
    }

    const add = new Set<SliceId>();
    const remove = new Set<SliceId>();

    if (!!sliceIds.base) {
      const baseSliceIds = await this.get(sliceIds.base);

      /* v8 ignore next -- @preserve */
      if (!baseSliceIds[this._tableKey]?._data?.[0]) {
        throw new Error(`Base sliceIds ${sliceIds.base} does not exist.`);
      }
      /* v8 ignore next -- @preserve */
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

    /* v8 ignore next -- @preserve */
    if (!!sliceIds.remove)
      for (const sliceId of sliceIds.remove) {
        remove.add(sliceId);
      }

    // Remove sliceIds that are both in add and remove
    /* v8 ignore next -- @preserve */
    for (const sliceId of remove.values()) {
      if (add.has(sliceId)) {
        add.delete(sliceId);
      }
    }

    const result = { add: Array.from(add) };

    if (sliceIdsHash) {
      this._resolvedSliceIds.set(sliceIdsHash, result);
    }

    return result;
  }

  /* v8 ignore next -- @preserve */
  async getChildRefs(): Promise<ControllerChildProperty[]> {
    return [];
  }

  /* v8 ignore next -- @preserve */
  async getChildRefsOfRow(): Promise<ControllerChildProperty[]> {
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
