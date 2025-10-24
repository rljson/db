// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { JsonValueType } from '@rljson/json';

import { ColumnSelection } from '../selection/column-selection.ts';

import { ViewWithData } from './view-with-data.ts';
import { View } from './view.ts';

/**
 * Offers a selection of columns of a given view
 *
 * Selects a subset of columns of the master view.
 */
export class ViewSelected extends View {
  // ...........................................................................
  /**
   * Constructor
   * @param columnSelection Columns selected from the master view
   * @param view The master view to select from
   */
  constructor(public readonly columnSelection: ColumnSelection, view: View) {
    super(columnSelection);
    view.validateSelection(this.columnSelection);
    this._initRows(view);
    this._updateRowHashes();
    this._updateColumnTypes(view);
  }

  get rows() {
    return this._rows;
  }

  /**
   * The column types of the view.
   */
  get columnTypes() {
    return this._columnTypes;
  }

  // ...........................................................................
  static get example(): ViewSelected {
    // Delete some columns from the example column selection
    const columnSelection = new ColumnSelection(
      ColumnSelection.example().columns.slice(0, 2),
    );

    return new ViewSelected(columnSelection, ViewWithData.example());
  }

  // ######################
  // Protected
  // ######################

  protected _updateRowHashes() {
    this._updateAllRowHashes();
  }

  // ######################
  // Private
  // ######################

  private _rows: any[][] = [];
  private _masterColumnIndices: number[] = [];
  private _columnTypes!: JsonValueType[];

  private _updateColumnTypes(view: View) {
    this._columnTypes = this._masterColumnIndices.map(
      (index) => view.columnTypes[index],
    );
  }

  private _initRows(view: View) {
    const columnCount = this.columnSelection.count;
    const masterColumnIndices = new Array(columnCount);
    const rowsSelected = new Array(view.rowCount);

    let i = 0;
    for (const hash of this.columnSelection.routeHashes) {
      const index = view.columnSelection.columnIndex(hash);
      masterColumnIndices[i] = index;
      i++;
    }

    for (let i = 0; i < view.rowCount; i++) {
      const row = new Array(columnCount);
      for (let j = 0; j < columnCount; j++) {
        row[j] = view.value(i, masterColumnIndices[j]);
      }
      rowsSelected[i] = row;
    }

    this._rows = rowsSelected;
    this._rowIndices = view.rowIndices;
    this._masterColumnIndices = masterColumnIndices;
  }
}
