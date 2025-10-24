// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { JsonValueType } from '@rljson/json';

import { RowFilterProcessor } from '../filter/row-filter-processor.ts';
import { StringFilterProcessor } from '../filter/string-filter-processor.ts';

import { ViewSelected } from './view-selected.ts';
import { View } from './view.ts';

/**
 * Provides a filtered version of an input view.
 */
export class ViewFiltered extends View {
  // ...........................................................................
  /**
   * Constructor
   * @param view  The master view to select from
   * @param filter The filter to apply to the view
   */
  constructor(
    view: View,

    public readonly filter: RowFilterProcessor,
  ) {
    super(view.columnSelection);
    this._filterView(view);
    this._updateRowIndices();
    this._updateRowHashes(view);
    this.columnTypes = view.columnTypes;
  }

  get rows() {
    return this._rows;
  }

  columnTypes: JsonValueType[];

  // ...........................................................................
  static example(filter?: RowFilterProcessor): ViewFiltered {
    filter ??= new RowFilterProcessor({
      'basicTypes/stringsRef/value': new StringFilterProcessor(
        'startsWith',
        'a',
      ),
    });
    return new ViewFiltered(ViewSelected.example, filter);
  }

  // ######################
  // Protected
  // ######################

  protected _updateRowHashes(original: View) {
    // This view has just been sorted.
    // Thus take over the row hashes from the original view.
    const rowHashes = new Array(this._rows.length);
    for (let i = 0; i < this._rows.length; i++) {
      rowHashes[i] = original.rowHashes[this._filteredRowIndices[i]];
    }
    this._rowHashes = rowHashes;
  }

  // ######################
  // Private
  // ######################

  private _rows: any[][] = [];

  private _filteredRowIndices: number[] = [];

  private _filterView(view: View) {
    const noFilter = Object.keys(this.filter.processors).length === 0;
    this._rows = view.rows;
    this._rowIndices = view.rowIndices;
    this._filteredRowIndices = view.rowIndices;

    if (noFilter) {
      return;
    }

    const filteredIndices = this.filter.applyTo(view);
    const filteredRows = new Array(filteredIndices.length);
    for (let i = 0; i < filteredIndices.length; i++) {
      filteredRows[i] = view.row(filteredIndices[i]);
    }
    this._filteredRowIndices = filteredIndices;
    this._rows = filteredRows;
  }
}
