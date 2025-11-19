// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Json, JsonValue } from '@rljson/json';
import {
  ContentType,
  InsertCommand,
  InsertHistoryRow,
  Ref,
  Rljson,
  SliceId,
  TableKey,
  TableType,
} from '@rljson/rljson';

import { Core } from '../core.ts';

import { CakeController, CakeControllerRefs } from './cake-controller.ts';
import { ComponentController } from './component-controller.ts';
import { LayerController, LayerControllerRefs } from './layer-controller.ts';
import {
  SliceIdController,
  SliceIdControllerRefs,
} from './slice-id-controller.ts';

export type ControllerRefs = CakeControllerRefs | LayerControllerRefs;

export type ControllerCommands = InsertCommand;

export type ControllerRunFn<N extends string, C extends JsonValue> = (
  command: ControllerCommands,
  value: C,
  origin?: Ref,
  refs?: ControllerRefs,
) => Promise<InsertHistoryRow<N>[]>;

export type ControllerChildProperty = {
  tableKey: TableKey;
  columnKey?: string;
  ref: Ref;
  sliceId?: SliceId;
};

// ...........................................................................
/**
 * Generic interface for a controller that manages a specific table in the database.
 * @template T The type of the table being managed.
 * @template N The name of the table being managed.
 * @property {ControllerRunFn<N>} insert - Function to execute a command on the table.
 * @property {() => Promise<void>} init - Initializes the controller.
 * @property {() => Promise<T>} table - Retrieves the current state of the table.
 * @property {(where: string | { [column: string]: JsonValue }) => Promise<Rljson>} get - Fetches data from the table based on a condition.
 * @property {(where: string | Json, filter?: Json) => Promise<Array<{ tableKey: TableKey; ref: Ref }>>} getChildRefs - Retrieves references to child entries in related tables based on a condition.
 * @param {string | Json }} where - The condition to filter the data.
 * @returns {Promise<Json[] | null>} A promise that resolves to an array of JSON objects or null if no data is found.
 * @throws {Error} If the data is invalid.
 */
export interface Controller<
  T extends TableType,
  C extends JsonValue,
  N extends string,
> {
  insert: ControllerRunFn<N, C>;
  init(): Promise<void>;
  table(): Promise<T>;
  get(where: string | Json, filter?: Json): Promise<Rljson>;
  getChildRefs(
    where: string | Json,
    filter?: Json,
  ): Promise<ControllerChildProperty[]>;
  filterRow(row: Json, key: string, value: JsonValue): Promise<boolean>;
  contentType(): ContentType;
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
): Promise<Controller<any, string, any>> => {
  let ctrl: Controller<any, any, string>;
  switch (type) {
    case 'layers':
      ctrl = new LayerController(core, tableKey, refs as LayerControllerRefs);
      break;
    case 'components':
    case 'edits':
    case 'editHistory':
    case 'multiEdits':
      ctrl = new ComponentController(core, tableKey, refs as ControllerRefs);
      break;
    case 'cakes':
      ctrl = new CakeController(core, tableKey, refs as CakeControllerRefs);
      break;
    case 'sliceIds':
      ctrl = new SliceIdController(
        core,
        tableKey,
        refs as SliceIdControllerRefs,
      );
      break;
    default:
      throw new Error(`Controller for type ${type} is not implemented yet.`);
  }
  await ctrl.init();
  return ctrl;
};
