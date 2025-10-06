// @license
// Copyright (c) 2025 Rljson
//
import { hsh } from '@rljson/hash';
// Use of this source code is governed by terms that can be
import { Json, JsonValue } from '@rljson/json';
// found in the LICENSE file in the root of this package.
import {
  ComponentsTable,
  EditProtocolRow,
  Ref,
  Rljson,
  TableKey,
  timeId,
} from '@rljson/rljson';

import { Core } from '../core.ts';

import {
  Controller,
  ControllerCommands,
  ControllerRefs,
} from './controller.ts';

export class ComponentController<N extends string, T extends Json>
  implements Controller<ComponentsTable<T>, N>
{
  constructor(
    private readonly _core: Core,
    private readonly _tableKey: TableKey,
    private _refs?: ControllerRefs,
  ) {}

  async init() {
    // Validate Table
    if (!!this._refs) {
      // No specific refs required for components table
      throw new Error(`Refs are not required on ComponentController.`);
    }

    // Table must be of type components
    const rljson = await this._core.dumpTable(this._tableKey);
    const table = rljson[this._tableKey];
    if (table._type !== 'components') {
      throw new Error(`Table ${this._tableKey} is not of type components.`);
    }
  }

  async add(
    value: T,
    origin?: Ref,
    previous?: string[],
    refs?: ControllerRefs,
  ): Promise<EditProtocolRow<N>> {
    if (!!refs)
      throw new Error(`Refs are not supported on ComponentController.`);

    return this.run('add', value, origin, previous);
  }

  async remove(): Promise<EditProtocolRow<N>> {
    throw new Error(`Remove is not supported on ComponentController.`);
  }

  async run(
    command: ControllerCommands,
    value: Json,
    origin?: Ref,
    previous?: string[],
    refs?: ControllerRefs,
  ): Promise<EditProtocolRow<N>> {
    // Validate command
    if (command !== 'add') {
      throw new Error(
        `Command ${command} is not supported by ComponentController.`,
      );
    }
    if (!!refs) {
      throw new Error(`Refs are not supported on ComponentController.`);
    }

    //Value to add
    const component = value as JsonValue & { _hash?: string };
    const rlJson = { [this._tableKey]: { _data: [component] } } as Rljson;

    //Write component to io
    await this._core.import(rlJson);

    return {
      //Ref to component
      [this._tableKey + 'Ref']: hsh(component as Json)._hash as string,

      //Data from edit
      route: '',
      origin,
      previous,

      //Unique id/timestamp
      timeId: timeId(),
    } as EditProtocolRow<N>;
  }

  async get(ref: string): Promise<T | null> {
    const row = await this._core.readRow(this._tableKey, ref);
    if (!row || !row[this._tableKey] || !row[this._tableKey]._data) {
      return null;
    }
    return row[this._tableKey]._data[0] as T;
  }

  async table(): Promise<ComponentsTable<T>> {
    const rljson = await this._core.dumpTable(this._tableKey);
    if (!rljson[this._tableKey]) {
      throw new Error(`Table ${this._tableKey} does not exist.`);
    }
    return rljson[this._tableKey] as ComponentsTable<T>;
  }
}
