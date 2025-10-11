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

import { BaseController } from './base-controller.ts';
import {
  Controller,
  ControllerCommands,
  ControllerRefs,
} from './controller.ts';

export class ComponentController<N extends string, T extends Json>
  extends BaseController<ComponentsTable<T>>
  implements Controller<ComponentsTable<T>, N>
{
  constructor(
    protected readonly _core: Core,
    protected readonly _tableKey: TableKey,
    private _refs?: ControllerRefs,
  ) {
    super(_core, _tableKey);
  }

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

  async run(
    command: ControllerCommands,
    value: Json,
    origin?: Ref,
    refs?: ControllerRefs,
  ): Promise<EditProtocolRow<N>> {
    // Validate command
    if (!command.startsWith('add')) {
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
      //Unique id/timestamp
      timeId: timeId(),
    } as any as EditProtocolRow<N>;
  }
}
