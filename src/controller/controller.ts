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

import { Db } from '../db.ts';

import { CakeController, CakeControllerRefs } from './cake-controller.ts';
import { ComponentController } from './component-controller.ts';
import { LayerController, LayerControllerRefs } from './layer-controller.ts';

export type ControllerRefs = Partial<Layer> & Partial<Cake>;
export type ControllerCommands = 'add';

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
  run(
    command: ControllerCommands,
    value: Json,
    origin?: Ref,
    previous?: string[],
    refs?: ControllerRefs,
  ): Promise<EditProtocolRow<N>>;
  init(): Promise<void>;
  table(): Promise<T>;
  get(ref: string): Promise<(Json & { _hash?: string }) | null>;
}

export const createController = async (
  type: ContentType,
  db: Db,
  tableKey: TableKey,
  refs?: ControllerRefs,
): Promise<Controller<any, string>> => {
  let ctrl: Controller<any, string>;
  switch (type) {
    case 'layers':
      ctrl = new LayerController(db, tableKey, refs as LayerControllerRefs);
      break;
    case 'components':
      ctrl = new ComponentController(db, tableKey, refs);
      break;
    case 'cakes':
      ctrl = new CakeController(db, tableKey, refs as CakeControllerRefs);
      break;
    default:
      throw new Error(`Controller for type ${type} is not implemented yet.`);
  }
  await ctrl.init();
  return ctrl;
};
