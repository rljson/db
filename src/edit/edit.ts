// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Json } from '@rljson/json';

import { EditAction, EditActionColumnSelection } from './edit-action.ts';

export type EditColumnSelection = EditActionColumnSelection;

export interface Edit extends Json {
  name: string;
  action: EditAction;
  _hash: string;
}
