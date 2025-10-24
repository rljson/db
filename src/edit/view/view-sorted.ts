// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { JsonValueType } from '@rljson/json';

import { RowSort } from '../sort/row-sort.ts';

import { ViewSelected } from './view-selected.ts';
import { View } from './view.ts';

/**
 * Provides a sorted version of an input view.
 */
export class ViewSorted extends View {
  // ...........................................................................
  /**
   * Constructor
   * @param view  The master view to select from
   * @param sort The sort to apply to the view
   */
  constructor(
    view: View,

    public readonly sort: RowSort,
  ) {
    super(view.columnSelection);
    this._sortView(view);
    this._updateRowHashes(view);
    this.columnTypes = view.columnTypes;
  }

  /**
   * The column types of the view.
   */
  columnTypes: JsonValueType[];

  get rows() {
    return this._rows;
  }

  // ...........................................................................
  static example(sort?: RowSort): ViewSorted {
    sort ??= new RowSort({ 'basicTypes/numbersRef/intsRef/value': 'desc' });
    return new ViewSorted(ViewSelected.example, sort);
  }

  // ######################
  // Protected
  // ######################

  protected _updateRowHashes(original: View) {
    // This view has just been sorted.
    // Thus take over the row hashes from the original view.
    const rowHashes = new Array(this._rows.length);
    for (let i = 0; i < this._rows.length; i++) {
      rowHashes[i] = original.rowHashes[this._sortedRowIndices[i]];
    }
    this._rowHashes = rowHashes;
  }

  // ######################
  // Private
  // ######################

  private _rows: any[][] = [];
  private _sortedRowIndices: number[] = [];

  private _sortView(view: View) {
    this._rowIndices = view.rowIndices;
    this._rows = view.rows;

    const sortIsEmpty = Object.keys(this.sort.columnSorts).length === 0;
    if (sortIsEmpty) {
      return;
    }

    this._sortRowIndices();
  }

  private _sortRowIndices() {
    const sortedIndices = this.sort!.applyTo(this);
    const sortedRows = new Array(this._rows.length);
    for (let i = 0; i < sortedIndices.length; i++) {
      sortedRows[i] = this._rows[sortedIndices[i]];
    }
    this._sortedRowIndices = sortedIndices;
    this._rows = sortedRows;
  }
}
