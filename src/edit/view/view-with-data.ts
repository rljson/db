// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { JsonValueType } from '@rljson/json';

import { ColumnSelection } from '../selection/column-selection.ts';

import { View } from './view.ts';

export class ViewWithData extends View {
  /**
   * constructor
   * @param columnSelection The column selection of the view
   * @param rows The rows of the view
   * @throws If the number of columns in the data does not match the column
   * selection
   */
  constructor(
    public readonly columnSelection: ColumnSelection,
    public readonly rows: any[][],
  ) {
    super(columnSelection);
    this._updateRowIndices();
    this._updateRowHashes();
    this.check();
    this.columnTypes = View.calcColumnTypes(rows, false);
  }

  /**
   * @returns an array of column types for each column
   */
  readonly columnTypes: JsonValueType[];

  /**
   * An example view with data
   */
  static example() {
    return new ViewWithData(ColumnSelection.example(), View.exampleData());
  }

  /**
   * Returns an empty view
   */
  static empty() {
    return new ViewWithData(ColumnSelection.example(), []);
  }

  // ######################
  // Protected
  // ######################

  protected _updateRowHashes() {
    this._updateAllRowHashes();
  }
}
