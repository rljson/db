// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Hash } from '@rljson/hash';
import { JsonValueType } from '@rljson/json';
import { ContentType, Ref, Route, SliceId } from '@rljson/rljson';

import { EditAction, EditActionWithIndex } from '../edit/edit-action.ts';
import { Edit } from '../edit/edit.ts';
import { RowFilterProcessor } from '../filter/row-filter-processor.ts';
import { RowFilter } from '../filter/row-filter.ts';
import { ColumnSelection } from '../selection/column-selection.ts';
import { ViewEdited } from '../view/view-edited.ts';
import { ViewFiltered } from '../view/view-filtered.ts';
import { View } from '../view/view.ts';

import { JoinView } from './join-view.ts';

export type JoinProcessType = 'filter' | 'edit' | 'selection';

export type JoinProcess = {
  type: JoinProcessType;
  instance: RowFilter | Edit | ColumnSelection;
  result: {
    view: View;
    data: JoinRowsHashed;
  };
};

export interface JoinColumn<T extends ContentType> {
  route: Route;
  value: T;
}

export type JoinRow = JoinColumn<any>[];
export type JoinRows = Record<SliceId, JoinRow>;

export type JoinRowHashed = {
  rowHash: Ref;
  columns: JoinColumn<any>[];
};
export type JoinRowsHashed = Record<SliceId, JoinRowHashed>;

export class Join {
  private _data: JoinRowsHashed = {};
  private _processes: JoinProcess[] = [];

  constructor(rows: JoinRows, private _columnSelection: ColumnSelection) {
    // Hash the rows
    this._data = this._hashedRows(rows);
  }

  // ...........................................................................
  /**
   * Applies a filter to the join and returns the filtered view
   *
   * @param filter The filter to apply
   */
  filter(filter: RowFilter) {
    const proc = RowFilterProcessor.fromModel(filter);

    const baseView = this.view;
    const filteredView = new ViewFiltered(baseView, proc);
    const filteredRowHashes = filteredView.rowHashes;

    // Filter the data
    const filteredData: JoinRowsHashed = {};
    for (const filteredRowHash of filteredRowHashes) {
      for (const sliceId of Object.keys(this._data)) {
        const rowData = this._data[sliceId];
        if (rowData.rowHash === filteredRowHash) {
          filteredData[sliceId] = rowData;
        }
      }
    }

    // Create the process entry
    const process: JoinProcess = {
      type: 'filter',
      instance: filter,
      result: {
        view: filteredView,
        data: filteredData,
      },
    };

    // Store the process
    this._processes.push(process);

    return this;
  }

  // ...........................................................................
  /**
   * Applies an edit to the join and returns the edited view
   *
   * @param edit The edit to apply
   */
  edit(edit: Edit) {
    // First filter the join according to the edit filter
    const filtered = this.filter(edit.filter);

    // Create the edited view
    const editedView = new ViewEdited(filtered.view, edit);

    // Create the edit actions with indices
    const editActions = this._indexingActions(edit.actions);

    // Apply the edit actions to the join data
    const editedData = this._applyAction(editActions);

    // Create the process entry
    const process: JoinProcess = {
      type: 'edit',
      instance: edit,
      result: {
        view: editedView,
        data: editedData,
      },
    };

    // Store the process
    this._processes.push(process);

    return this;
  }

  // ...........................................................................
  /**
   * Applies the given edit actions to the join data
   *
   * @param actions The edit actions to apply
   * @returns The edited join data
   */
  private _applyAction(actions: EditActionWithIndex[]) {
    const result: JoinRowsHashed = {};

    for (const [sliceId, joinRowH] of Object.entries(this.data)) {
      const cols = [...joinRowH.columns];
      for (const action of actions) {
        cols[action.index].value = action.setValue;
      }
      result[sliceId] = {
        rowHash: Hash.default.calcHash(cols.map((c) => c.value) as any[]),
        columns: cols,
      };
    }

    return result;
  }

  // ...........................................................................
  /**
   * Hashes the given join rows
   *
   * @param rows The join rows to hash
   * @returns The hashed join rows
   */
  private _hashedRows(rows: JoinRows) {
    const sliceIds = Object.keys(rows);
    const hashedRows: JoinRowsHashed = {};
    for (const sliceId of sliceIds) {
      const columns = rows[sliceId];
      const rowHash = Hash.default.calcHash(
        columns.map((col) => col.value) as any[],
      );
      hashedRows[sliceId] = {
        rowHash,
        columns,
      };
    }
    return hashedRows;
  }

  // ...........................................................................
  /**
   * Indexes the edit actions to column indices
   *
   * @param actions The edit actions
   * @returns The edit actions with column indices
   */
  private _indexingActions(actions: EditAction[]): EditActionWithIndex[] {
    const cs = this.columnSelection;

    const result = actions.map((action) => {
      const route = action.route;
      const index = cs.columnIndex(route, false);
      if (index === -1) {
        throw new Error(
          `Join: Cannot find column for route "${route.toString()}"`,
        );
      }

      return {
        ...action,
        index,
      };
    });

    return result;
  }

  // ...........................................................................
  /**
   * Returns a view representation of the join
   */
  get view(): View {
    if (this._processes.length > 0) {
      return this._processes[this._processes.length - 1].result.view;
    }
    return new JoinView(this);
  }

  // ...........................................................................
  /**
   * Returns the data of the join
   */
  get data(): JoinRowsHashed {
    if (this._processes.length > 0) {
      return this._processes[this._processes.length - 1].result.data;
    }
    return this._data;
  }

  // ...........................................................................
  /**
   * Returns the column types of the join
   */
  get columnTypes(): JsonValueType[] {
    return this._columnSelection.columns.map((col) => col.type);
  }

  // ...........................................................................
  /**
   * Returns the column selection of the join
   */
  get columnSelection(): ColumnSelection {
    return this._columnSelection;
  }

  // ...........................................................................
  /**
   * Returns all rows of the join w/ nulled missing values
   *
   * @return The rows of the join
   */
  get rows(): any[][] {
    const result: any[][] = [];
    const sliceIds = Object.keys(this._data);
    for (const sliceId of sliceIds) {
      const dataColumns = (this._data[sliceId] as JoinRowHashed).columns;
      const row: any[] = [];
      for (const colInfo of this._columnSelection.columns) {
        const joinCol = dataColumns.find((dataCol) => {
          const colInfoRoute = Route.fromFlat(colInfo.route);
          const dataColRoute = dataCol.route;
          return colInfoRoute.equalsWithoutRefs(dataColRoute);
        });
        row.push(joinCol ? joinCol.value : null);
      }
      result.push(row);
    }
    return result;
  }
}
