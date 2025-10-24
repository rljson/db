// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { JsonValueType } from '@rljson/json';

import { MultiEditResolved } from '../edit/multi-edit-resolved.ts';

import { ViewCache } from './view-cache.ts';
import { ViewEdited } from './view-edited.ts';
import { ViewWithData } from './view-with-data.ts';
import { View } from './view.ts';

/**
 * An view with multiple edits applied to it.
 */
export class ViewEditedMultiple extends View {
  /**
   * Constructor
   * @param master The master view to which the multiEdit is applied
   * @param multiEdit The multiEdit to be applied to the view
   * @param cache The cache to be used
   */
  constructor(
    master: View,
    public readonly multiEdit: MultiEditResolved,
    public readonly cache: ViewCache = new ViewCache(1),
  ) {
    super(master.columnSelection);
    this._applyMultiEdit(master);
    this._updateAllRowHashes();
    this.columnTypes = master.columnTypes;
  }

  // ...........................................................................
  get rows() {
    return this._rows;
  }

  /**
   * The column types of the view.
   */
  columnTypes: JsonValueType[];

  // ...........................................................................
  static get example(): ViewEditedMultiple {
    // Get the master view
    const masterView = ViewWithData.example();

    const multiEdit = MultiEditResolved.example;

    // Create and return the result object
    const result = new ViewEditedMultiple(masterView, multiEdit);
    return result;
  }

  editCount = 0;

  // ######################
  // Protected
  // ######################

  // ...........................................................................
  /* v8 ignore start */
  protected _updateRowHashes(): void {
    // Hashes are already updated within _applyMultiEdit
  }
  /* v8 ignore stop */

  // ######################
  // Private
  // ######################

  private _rows: any[][] = [];

  private _applyMultiEdit(master: View) {
    // If edits are empty, return the view as is
    const isEmpty =
      !this.multiEdit.previous && this.multiEdit.edit.actions.length === 0;

    if (isEmpty) {
      this._rows = master.rows;
      return;
    }

    // Create a list of edits to be applied
    const edits = this._editChain(master);

    // Apply the edits to the view
    let resultView: View = master;
    let cachedResult: View | undefined = this.cache.get(master, edits[0]);

    for (const edit of edits) {
      resultView = cachedResult ?? new ViewEdited(resultView, edit.edit);
      if (!cachedResult) {
        this.cache.set(master, edit, resultView);
        this.editCount++;
      }
      cachedResult = undefined;
    }

    // Take the result view as the final view
    this._rows = resultView.rows;
    this._rowHashes = resultView.rowHashes;
  }

  // ...........................................................................
  private _editChain(master: View): MultiEditResolved[] {
    const result: MultiEditResolved[] = [];

    // Start with the head and follow the chain
    let current: MultiEditResolved | undefined = this.multiEdit;
    while (current) {
      result.push(current);

      // If a cached item is available, we don't need to follow the chain
      if (this.cache.get(master, current)) {
        break;
      }

      // Move to the previous item
      current = current.previous;
    }

    // Return the result in reverse order
    return result.reverse();
  }
}
