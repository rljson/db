// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Json } from '@rljson/json';
import {
  Cake,
  ContentType,
  EditCommand,
  EditProtocolRow,
  Layer,
  Ref,
  Rljson,
  TableKey,
  TableType,
} from '@rljson/rljson';

import { Core } from '../core.ts';

import { CakeController, CakeControllerRefs } from './cake-controller.ts';
import { ComponentController } from './component-controller.ts';
import { LayerController, LayerControllerRefs } from './layer-controller.ts';

export type ControllerRefs = Partial<Layer> & Partial<Cake>;
export type ControllerCommands = EditCommand;

export type ControllerRunFn<N extends string> = (
  command: ControllerCommands,
  value: Json,
  origin?: Ref,
  refs?: Partial<ControllerRefs>,
) => Promise<EditProtocolRow<N>>;

// ...........................................................................
/**
 * Generic interface for a controller that manages a specific table in the database.
 * @template T The type of the table being managed.
 * @template N The name of the table being managed.
 * @property {ControllerRunFn<N>} run - Function to execute a command on the table.
 * @property {() => Promise<void>} init - Initializes the controller.
 * @property {() => Promise<T>} table - Retrieves the current state of the table.
 * @property {(where: string | { [column: string]: JsonValue }) => Promise<Rljson>} get - Fetches data from the table based on a condition.
 * @param {string | Json }} where - The condition to filter the data.
 * @returns {Promise<Json[] | null>} A promise that resolves to an array of JSON objects or null if no data is found.
 * @throws {Error} If the data is invalid.
 */
export interface Controller<T extends TableType, N extends string> {
  run: ControllerRunFn<N>;
  init(): Promise<void>;
  table(): Promise<T>;
  get(where: string | Json): Promise<Rljson>;
}

// ...........................................................................
/**
 * Factory function to create a controller based on the content type.
 * @param {ContentType} type - The type of content (e.g., 'layers', 'components', 'cakes').
 * @param {Core} core - The core instance managing the database.
 * @param {TableKey} tableKey - The key identifying the table to be managed.
 * @param {ControllerRefs} [refs] - Optional references for the controller.
 * @returns {Promise<Controller<any, string>>} A promise that resolves to the created controller.
 * @throws {Error} If the controller for the specified type is not implemented.
 */
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
