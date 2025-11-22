// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Hash } from '@rljson/hash';
import { Json, JsonValueType } from '@rljson/json';
import { ContentType, Insert, Ref, Route, SliceId } from '@rljson/rljson';

import { RowFilterProcessor } from './filter/row-filter-processor.ts';
import { RowFilter } from './filter/row-filter.ts';
import { ColumnSelection } from './selection/column-selection.ts';
import { SetValue } from './set-value/set-value.ts';
import { RowSort } from './sort/row-sort.ts';

export type JoinProcessType = 'filter' | 'setValue' | 'selection' | 'sort';

export type JoinProcess = {
  type: JoinProcessType;
  instance: RowFilter | SetValue | ColumnSelection | RowSort;
  data: JoinRowsHashed;
  columnSelection: ColumnSelection;
};

export interface JoinColumn<T extends ContentType> {
  route: Route;
  tree: Json;
  value: T | null;
  shadow: T | null;
  insert: T | null;
}

export type JoinRow = JoinColumn<any>[];
export type JoinRows = Record<SliceId, JoinRow>;

export type JoinRowHashed = {
  rowHash: Ref;
  columns: JoinColumn<any>[];
};
export type JoinRowsHashed = Record<SliceId, JoinRowHashed>;

export class Join {
  private _base: JoinRowsHashed = {};
  private _baseColumnSelection: ColumnSelection;

  private _processes: JoinProcess[] = [];

  constructor(rows: JoinRows, columnSelection: ColumnSelection) {
    // Hash the rows
    this._base = this._hashedRows(rows);

    this._baseColumnSelection = columnSelection;
  }

  // ...........................................................................
  /**
   * Applies a filter to the join and returns the filtered view
   *
   * @param filter The filter to apply
   */
  filter(filter: RowFilter): Join {
    const proc = RowFilterProcessor.fromModel(filter);

    const data = proc.applyTo(this);

    // Create the process entry
    const process: JoinProcess = {
      type: 'filter',
      instance: filter,
      data,
      columnSelection: this.columnSelection,
    };

    // Store the process
    this._processes.push(process);

    return this;
  }

  // ...........................................................................
  /**
   * Applies a set value action to the join and returns the edited join
   *
   * @param setValue The set value action to apply
   */
  setValue(setValue: SetValue): Join {
    const data: JoinRowsHashed = {};

    for (const [sliceId, joinRowH] of Object.entries(this.data)) {
      const cols = [...joinRowH.columns];

      for (const col of cols) {
        /*v8 ignore else -- @preserve */
        if (Route.fromFlat(setValue.route).equalsWithoutRefs(col.route))
          col.insert = setValue.value;
        else continue;
      }

      data[sliceId] = {
        rowHash: Hash.default.calcHash(cols.map((c) => c.value) as any[]),
        columns: cols,
      };
    }

    // Create the process entry
    const process: JoinProcess = {
      type: 'setValue',
      instance: setValue,
      data,
      columnSelection: this.columnSelection,
    };

    // Store the process
    this._processes.push(process);

    return this;
  }

  // ...........................................................................
  /**
   * Applies multiple set value actions to the join and returns the edited join
   *
   * @param setValues The set value actions to apply
   */
  setValues(setValues: SetValue[]): Join {
    // Apply the set values one by one to a copy of this join
    let result: Join = this.clone();

    for (const setValue of setValues) {
      result = result.setValue(setValue);
    }

    return result;
  }

  // ...........................................................................
  /**
   * Selects columns from the join and returns the resulting join
   *
   * @param columnSelection The column selection to apply
   */
  select(columnSelection: ColumnSelection): Join {
    const columnCount = columnSelection.count;
    const masterColumnIndices = new Array(columnCount);

    // Map selected columns to master column indices
    let i = 0;
    for (const hash of columnSelection.routeHashes) {
      const index = this.columnSelection.columnIndex(hash);
      masterColumnIndices[i] = index;
      i++;
    }

    // Select the columns
    const data: JoinRowsHashed = {};
    for (let i = 0; i < this.rowCount; i++) {
      const [sliceId, row] = Object.entries(this.data)[i];
      const selectedColumns: JoinColumn<any>[] = [];
      // Select only the requested columns
      for (let j = 0; j < masterColumnIndices.length; j++) {
        selectedColumns.push(row.columns[masterColumnIndices[j]]);
      }
      // Store the selected columns
      data[sliceId] = {
        rowHash: Hash.default.calcHash(
          selectedColumns.map((c) => c.value) as any[],
        ),
        columns: selectedColumns,
      };
    }

    // Create the process entry
    const process: JoinProcess = {
      type: 'selection',
      instance: columnSelection,
      data,
      columnSelection,
    };

    // Store the process
    this._processes.push(process);

    return this;
  }

  // ...........................................................................
  /**
   * Sorts the join rows and returns the sorted join
   *
   * @param rowSort The row sort to apply
   */
  sort(rowSort: RowSort): Join {
    const sortedIndices = rowSort.applyTo(this);

    const data: JoinRowsHashed = {};
    for (let i = 0; i < sortedIndices.length; i++) {
      const sliceId = sortedIndices[i];
      data[sliceId] = this.data[sliceId];
    }

    // Create the process entry
    const process: JoinProcess = {
      type: 'sort',
      instance: rowSort,
      data,
      columnSelection: this.columnSelection,
    };

    // Store the process
    this._processes.push(process);

    return this;
  }

  // ...........................................................................
  /**
   * Returns insert Object of the join
   */
  insert(): Insert<any>[] {
    return [] as Insert<any>[];
  }

  // ...........................................................................
  /**
   * Returns the value at the given row and column index
   *
   * @param row The row index
   * @param column The column index
   * @returns The value at the given row and column
   */
  value(row: number, column: number): any {
    return this.rows[row][column];
  }

  // ...........................................................................
  /**
   * Clones the join
   *
   * @returns The cloned join
   */
  clone(): Join {
    const cloned = Object.create(this);
    cloned._data = this._base;
    cloned._processes = [...this._processes];
    return cloned;
  }

  // ...........................................................................
  /**
   * Returns all component routes of the join
   */
  get componentRoutes(): Route[] {
    return Array.from(
      new Set(
        Object.values(this.columnSelection.columns).map(
          (c) => Route.fromFlat(c.route).upper().flatWithoutRefs,
        ),
      ),
    ).map((r) => Route.fromFlat(r));
  }

  // ...........................................................................
  /**
   * Returns all layer routes of the join
   */
  get layerRoutes(): Route[] {
    return Array.from(
      new Set(
        Object.values(this.columnSelection.columns)
          .map((c) => [
            Route.fromFlat(c.route).top,
            Route.fromFlat(c.route).deeper(1).top,
          ])
          .map((segments) => new Route(segments).flat),
      ),
    ).map((r) => Route.fromFlat(r));
  }

  // ...........................................................................
  /**
   * Returns the cake route of the join
   */
  get cakeRoute(): Route {
    const cakeRoute = Array.from(
      new Set(
        Object.values(this.columnSelection.columns).map(
          (c) => Route.fromFlat(c.route).top.tableKey,
        ),
      ),
    ).map((r) => Route.fromFlat(r));

    /* v8 ignore if -- @preserve */
    if (cakeRoute.length !== 1) {
      throw new Error(
        `Join: Error while getting cake route: ` +
          `The join has ${cakeRoute.length} different cake routes. ` +
          `Cannot determine a single cake route.`,
      );
    }
    return cakeRoute[0];
  }

  // ...........................................................................
  /**
   * Returns the number of rows in the join
   */
  get rowCount(): number {
    return Object.keys(this.data).length;
  }

  // ...........................................................................
  /**
   * Returns the number of columns in the join
   */
  get columnCount(): number {
    return this.columnSelection.count;
  }

  // ...........................................................................
  /**
   * Returns the row indices (sliceIds) of the join
   */
  get rowIndices(): SliceId[] {
    return Object.keys(this.data);
  }

  // ...........................................................................
  /**
   * Returns the join row for the given slice id
   *
   * @param sliceId - The slice id
   * @returns The join row
   */
  row(sliceId: SliceId): JoinRow {
    return this.data[sliceId].columns;
  }

  // ...........................................................................
  /**
   * Returns the data of the join
   */
  get data(): JoinRowsHashed {
    if (this._processes.length > 0) {
      return this._processes[this._processes.length - 1].data;
    }
    return this._base;
  }

  // ...........................................................................
  /**
   * Returns the column types of the join
   */
  get columnTypes(): JsonValueType[] {
    return this.columnSelection.columns.map((col) => col.type);
  }

  // ...........................................................................
  /**
   * Returns the column selection of the join
   */
  get columnSelection(): ColumnSelection {
    if (this._processes.length > 0) {
      return this._processes[this._processes.length - 1].columnSelection;
    }
    return this._baseColumnSelection;
  }

  // ...........................................................................
  /**
   * Returns all rows of the join w/ nulled missing values
   *
   * @return The rows of the join
   */
  get rows(): any[][] {
    const result: any[][] = [];
    const sliceIds = Object.keys(this.data);
    for (const sliceId of sliceIds) {
      const dataColumns = (this.data[sliceId] as JoinRowHashed).columns;
      const row: any[] = [];
      for (const colInfo of this.columnSelection.columns) {
        const joinCol = dataColumns.find((dataCol) => {
          const colInfoRoute = Route.fromFlat(colInfo.route);
          const dataColRoute = dataCol.route;

          return colInfoRoute.equalsWithoutRefs(dataColRoute);
        });
        /* v8 ignore next -- @preserve */
        row.push(joinCol ? joinCol.insert ?? joinCol.value : null);
      }
      result.push(row);
    }
    return result;
  }

  static empty(): Join {
    return new Join({}, ColumnSelection.empty());
  }

  // ...........................................................................
  /**
   * Hashes the given join rows. If insert value is present, it is used for hashing.
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
        columns.map((col) => col.insert ?? col.value) as any[],
      );
      hashedRows[sliceId] = {
        rowHash,
        columns,
      };
    }
    return hashedRows;
  }
}
