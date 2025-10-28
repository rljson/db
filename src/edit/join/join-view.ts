// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { JsonValueType } from '@rljson/json';

import { View } from '../view/view.ts';

import { Join } from './join.ts';

export class JoinView extends View {
  rows: any[][];
  columnTypes: JsonValueType[];

  constructor(private _join: Join) {
    super(_join.columnSelection);

    this.rows = this._join.rows;
    this.columnTypes = this._join.columnTypes;

    this._updateRowIndices();
    this._updateRowHashes();
    this.check();
  }

  // ######################
  // Protected
  // ######################

  protected _updateRowHashes() {
    this._updateAllRowHashes();
  }
}
