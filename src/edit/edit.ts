// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Json } from '@rljson/json';
import { ColumnCfg, TableCfg } from '@rljson/rljson';

import { EditAction, exampleEditActionColumnSelection } from './edit-action.ts';

export interface Edit extends Json {
  name: string;
  action: EditAction;
  _hash: string;
}

export const exampleEditColumnSelection = (): Edit => ({
  name: 'Select: brand, type, serviceIntervals, isElectric, height, width, length, engine, repairedByWorkshop',
  action: exampleEditActionColumnSelection(),
  _hash: '',
});

export const createEditTableCfg = (cakeKey: string): TableCfg =>
  ({
    key: `${cakeKey}Edits`,
    type: 'components',
    columns: [
      {
        key: '_hash',
        type: 'string',
        titleLong: 'Hash',
        titleShort: 'Hash',
      },
      {
        key: 'name',
        type: 'string',
        titleLong: 'Edit Name',
        titleShort: 'Name',
      },
      {
        key: 'action',
        type: 'json',
        titleLong: 'Edit Action',
        titleShort: 'Action',
      },
    ] as ColumnCfg[],
  } as TableCfg);
