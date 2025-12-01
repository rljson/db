// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Edit } from '@rljson/rljson';

import {
  EditActionColumnSelection,
  EditActionRowFilter,
  EditActionRowSort,
  EditActionSetValue,
  exampleEditActionColumnSelection,
} from './edit-action.ts';

export interface EditColumnSelection extends Edit {
  name: string;
  action: EditActionColumnSelection;
  _hash: string;
}

export interface EditRowFilter extends Edit {
  name: string;
  action: EditActionRowFilter;
  _hash: string;
}

export interface EditSetValue extends Edit {
  name: string;
  action: EditActionSetValue;
  _hash: string;
}

export interface EditRowSort extends Edit {
  name: string;
  action: EditActionRowSort;
  _hash: string;
}

export const exampleEditColumnSelection = (): Edit => ({
  name: 'Select: brand, type, serviceIntervals, isElectric, height, width, length, engine, repairedByWorkshop',
  action: exampleEditActionColumnSelection(),
  _hash: '',
});
