// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Json } from '@rljson/json';
import {
  Cake,
  ContentType,
  EditProtocolRow,
  Layer,
  Ref,
  TableKey,
  TableType,
} from '@rljson/rljson';

import { Core } from '../core.ts';

import { CakeController, CakeControllerRefs } from './cake-controller.ts';
import { ComponentController } from './component-controller.ts';
import { LayerController, LayerControllerRefs } from './layer-controller.ts';

export type ControllerRefs = Partial<Layer> & Partial<Cake>;
export type ControllerCommands = 'add' | 'remove';

export type ControllerRunFn<N extends string> = (
  command: ControllerCommands,
  value: Json,
  origin?: Ref,
  previous?: string[],
  refs?: Partial<ControllerRefs>,
) => Promise<EditProtocolRow<N>>;

// ...........................................................................
/**
 * Generic interface for a controller that manages a specific table in the database.
 * @template T The type of the table being managed.
 * @template N The name of the table being managed.
 * @method add Adds a new entry to the table.
 * @method remove Removes an entry from the table.
 * @method run Executes a command on the table.
 * @method init Initializes the controller, performing any necessary setup or validation.
 * @method table Retrieves the current state of the table.
 * @method get Fetches a specific entry from the table by its reference.
 * @throws {Error} If the data is invalid.
 */
export interface Controller<T extends TableType, N extends string> {
  add(
    value: Json,
    origin?: Ref,
    previous?: string[],
    refs?: ControllerRefs,
  ): Promise<EditProtocolRow<N>>;
  remove(
    value: Json,
    origin?: Ref,
    previous?: string[],
    refs?: ControllerRefs,
  ): Promise<EditProtocolRow<N>>;
  run: ControllerRunFn<N>;
  init(): Promise<void>;
  table(): Promise<T>;
  get(ref: string): Promise<(Json & { _hash?: string }) | null>;
}

export const createController = async (
  type: ContentType,
  core: Core,
  tableKey: TableKey,
  refs?: ControllerRefs,
): Promise<Controller<any, string>> => {
  let ctrl: Controller<any, string>;
  switch (type) {
    case 'layers':
      ctrl = new LayerController(core, tableKey, refs as LayerControllerRefs);
      break;
    case 'components':
      ctrl = new ComponentController(core, tableKey, refs);
      break;
    case 'cakes':
      ctrl = new CakeController(core, tableKey, refs as CakeControllerRefs);
      break;
    default:
      throw new Error(`Controller for type ${type} is not implemented yet.`);
  }
  await ctrl.init();
  return ctrl;
};
