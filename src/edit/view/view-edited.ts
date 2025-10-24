// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Hash, hip } from '@rljson/hash';
import { JsonValueType } from '@rljson/json';

import { EditAction } from '../edit/edit-action.ts';
import { Edit } from '../edit/edit.ts';
import { NumberFilter } from '../filter/number-filter.ts';
import { RowFilterProcessor } from '../filter/row-filter-processor.ts';
import { StringFilter } from '../filter/string-filter.ts';

import { ViewWithData } from './view-with-data.ts';
import { View } from './view.ts';

/**
 * An view with an Edit applied to.
 */
export class ViewEdited extends View {
  /**
   * Constructor
   * @param view The view to be edited
   * @param edit The edit to be applied to the view
   */
  constructor(view: View, public readonly edit: Edit) {
    super(view.columnSelection);
    this._actions = this._initActions;
    this._applyEdit(view);
    this._updateRowHashes(view);
    this.columnTypes = view.columnTypes;
  }

  /**
   * The rows of the view.
   */
  get rows() {
    return this._rows;
  }

  /**
   * The column types of the view.
   */
  columnTypes: JsonValueType[];

  static get example(): ViewEdited {
    return ViewEdited._example;
  }

  // ######################
  // Protected
  // ######################

  protected _updateRowHashes(view: View) {
    this._rowHashes = [...view.rowHashes];
    for (const i of this._editedRows) {
      const row = this._rows[i];
      this._rowHashes[i] = Hash.default.calcHash(row);
    }
  }

  // ######################
  // Private
  // ######################

  private _rows: any[][] = [];
  private _actions: _EditActionWithIndex[];
  private _editedRows: number[] = [];

  private _applyEdit(view: View) {
    const rows = view.rows;

    // If edit actions are empty, the view is not changed
    if (this.edit.actions.length === 0) {
      this._rows = rows;
      return;
    }

    // Create a filter processor from the edit
    const rowFilter = RowFilterProcessor.fromModel(this.edit.filter);

    // Get the rows matching the filter
    const matchingRows = rowFilter.applyTo(view);

    // No rows match the filter? Keep the unchanged view.
    if (matchingRows.length === 0) {
      this._rows = rows;
      return;
    }

    // Get column indices for the edit actions

    // Create a copy of all rows
    const result = [...rows];

    // Iterate all matching rows and apply the edit actions
    for (const i of matchingRows) {
      const originalRow = result[i];
      const processedRow = this._applyActions(originalRow);
      result[i] = processedRow;
    }

    this._editedRows = matchingRows;

    this._rows = result;
  }

  // ...........................................................................
  private _applyActions(row: any[]): any[] {
    const result = [...row];

    for (const action of this._actions) {
      result[action.index] = action.setValue;
    }
    return result;
  }

  // ...........................................................................
  private get _initActions(): _EditActionWithIndex[] {
    const cs = this.columnSelection;

    const result = this.edit.actions.map((action) => {
      const route = action.route;
      const index = cs.columnIndex(route, false);
      if (index === -1) {
        this._throwColumnNotFoundError(route);
      }

      return {
        ...action,
        index,
      };
    });

    return result;
  }

  // ...........................................................................
  private _throwColumnNotFoundError(column: string) {
    throw new Error(
      `ViewEdited: Error while applying an Edit to a view: ` +
        `One of the edit actions refers to the column "${column}" ` +
        ` that does not exist in the view. ` +
        `Please make sure that alle columns referred in any action ` +
        `are available in the view.`,
    );
  }

  // ...........................................................................
  static get _example() {
    const view = ViewWithData.example();
    const stringEndsWithO: StringFilter = hip({
      _hash: '',
      type: 'string',
      column: 'basicTypes/stringsRef/value',
      operator: 'endsWith',
      search: 'o',
    });

    const intIsGreater1: NumberFilter = hip({
      _hash: '',
      type: 'number',
      column: 'basicTypes/numbersRef/intsRef/value',
      operator: 'greaterThan',
      search: 1,
    });

    const edit: Edit = hip<Edit>({
      _hash: '',
      name: [
        'Set bool to true ',
        'and done to true',
        'of all items that ',
        '1.) end with "o" and ',
        '2.) have an int column greater > 1.',
      ].join(' '),
      filter: {
        _hash: '',
        operator: 'and',
        columnFilters: [stringEndsWithO, intIsGreater1],
      },
      actions: [
        {
          _hash: '',
          route: 'basicTypes/booleansRef/value',
          setValue: true,
        },
        {
          _hash: '',
          route: 'complexTypes/jsonObjectsRef/value',
          setValue: { done: true },
        },
      ],
    });

    return new ViewEdited(view, edit);
  }
}

/// An edit action with the column index
interface _EditActionWithIndex extends EditAction {
  index: number;
}
