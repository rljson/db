// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Hash } from '@rljson/hash';
import { Json, JsonValue, JsonValueType } from '@rljson/json';
import { Ref, Route, SliceId } from '@rljson/rljson';

import { traverse } from 'object-traversal';

import { Container } from '../db.ts';
import { inject } from '../tools/inject.ts';
import { isolate } from '../tools/isolate.ts';
import { mergeTrees } from '../tools/merge-trees.ts';

import { RowFilterProcessor } from './filter/row-filter-processor.ts';
import { RowFilter } from './filter/row-filter.ts';
import { ColumnSelection } from './selection/column-selection.ts';
import { SetValue } from './set-value/set-value.ts';
import { RowSort } from './sort/row-sort.ts';

export const joinPreserveKeys = [
  'sliceIdsTable',
  'sliceIdsRow',
  /*'base',*/
  'sliceIdsTable',
  'sliceIdsTableRow',
  'componentsTable',
];

export type JoinProcessType = 'filter' | 'setValue' | 'selection' | 'sort';

export type JoinProcess = {
  type: JoinProcessType;
  instance: RowFilter | SetValue | ColumnSelection | RowSort;
  data: JoinRowsHashed;
  columnSelection: ColumnSelection;
};

export interface JoinColumn {
  route: Route;
  value: Container;
  inserts: Container[] | null;
}

export type JoinRow = JoinColumn[];
export type JoinRows = Record<SliceId, JoinRow>;

export type JoinRowHashed = {
  rowHash: Ref;
  columns: JoinColumn[];
};
export type JoinRowsHashed = Record<SliceId, JoinRowHashed>;

export class Join {
  private _base: JoinRowsHashed = {};
  private _baseColumnSelection: ColumnSelection;

  private _processes: JoinProcess[] = [];

  /**
   * Materialized row matrices per data object. Data objects are created
   * once per process step and never mutated, so the matrix can be reused
   * for every subsequent access.
   */
  private static readonly _rowsCache = new WeakMap<JoinRowsHashed, any[][]>();

  constructor(rows: JoinRows, columnSelection: ColumnSelection) {
    // Hash the rows
    this._base = this._hashedRows(rows);

    this._baseColumnSelection = columnSelection;
  }

  // ...........................................................................
  /**
   * Creates a JoinRowHashed whose rowHash is computed lazily on first
   * access. Nothing on the hot path reads rowHash, so eager SHA hashing
   * of every row on every process step is avoided.
   *
   * @param columns The columns of the row
   * @param hashInput Produces the values to hash
   * @returns The hashed join row
   */
  private static _lazyHashedRow(
    columns: JoinColumn[],
    hashInput: () => any[],
  ): JoinRowHashed {
    const row = { columns } as JoinRowHashed;
    let cached: Ref | undefined;
    Object.defineProperty(row, 'rowHash', {
      enumerable: true,
      configurable: true,
      get: () => (cached ??= Hash.default.calcHash(hashInput()) as Ref),
    });
    return row;
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

    // Parse the target route once instead of per column per row
    const setValueRoute = Route.fromFlat(setValue.route);

    for (const [sliceId, joinRowH] of Object.entries(this.data)) {
      const cols = [...joinRowH.columns];
      const insertCols: JoinColumn[] = [];
      for (const col of cols) {
        const insertCol = {
          ...col,
          //inserts: col.inserts ? [...col.inserts] : [],
        };

        /*v8 ignore else -- @preserve */
        if (setValueRoute.equalsWithoutRefs(col.route)) {
          for (const cell of col.value.cell) {
            /* v8 ignore next -- @preserve */
            if (cell.path.length === 0) {
              throw new Error(
                `Join: Error while applying SetValue: ` +
                  `Cannot set value for column without paths. ` +
                  `Route: ${setValue.route.toString()}.`,
              );
            }

            /* v8 ignore next -- @preserve */
            if (cell.path.length > 1) {
              throw new Error(
                `Join: Error while applying SetValue: ` +
                  `Cannot set value for multiple paths in one cell. ` +
                  `Found paths: [${cell.path.join(', ')}] for route: ` +
                  `${setValue.route.toString()}.`,
              );
            }

            const cellInsertTree = isolate(
              { ...col.value.tree },
              cell.path[0],
              joinPreserveKeys,
            );
            inject(cellInsertTree, cell.path[0], setValue.value);

            const propertyKey = cell.path[0].slice(-1)[0];
            const insert: Container = {
              cell: [
                {
                  ...cell,
                  ...{ value: setValue.value },
                  ...{
                    row: {
                      ...(cell.row as Json),
                      ...{ [propertyKey]: setValue.value },
                    } as any,
                  },
                },
              ],
              tree: cellInsertTree,
              rljson: col.value.rljson,
            };

            /* v8 ignore next -- @preserve */
            if (insert) {
              if (insertCol.inserts) insertCol.inserts.push(insert);
              else insertCol.inserts = [insert];
            }
          }
        }
        insertCols.push(insertCol);
      }

      data[sliceId] = Join._lazyHashedRow(
        insertCols,
        () =>
          insertCols.map((col) =>
            col.value.cell.flatMap((c) => c.value),
          ) as any[],
      );
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
    for (const [sliceId, row] of Object.entries(this.data)) {
      const cols: JoinColumn[] = [];
      // Select only the requested columns
      for (let j = 0; j < masterColumnIndices.length; j++) {
        cols.push(row.columns[masterColumnIndices[j]]);
      }
      // Store the selected columns
      data[sliceId] = Join._lazyHashedRow(
        cols,
        () =>
          cols.map((col) => col.value.cell.flatMap((c) => c.value)) as any[],
      );
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
  insert(): {
    route: Route;
    tree: Json;
  }[] {
    const inserts: {
      route: Route;
      tree: Json;
    }[] = [];

    for (let i = 0; i < this.columnCount; i++) {
      const colInserts: {
        route: Route;
        tree: Json;
        path: Array<string | number>;
      }[] = [];
      for (const row of Object.values(this.data)) {
        const col = row.columns[i];
        if (col.inserts && col.inserts.length > 0) {
          for (const insert of col.inserts) {
            for (const cell of insert.cell) {
              const tree = insert.tree;
              const path = cell.path;

              // Inject the value at the path, inject complete row
              inject(tree, path[0].slice(0, -1), cell.row);

              colInserts.push({
                route: col.route,
                tree,
                path: path[0],
              });
            }
          }
        }
      }

      if (colInserts.length === 0) continue;

      //Merge all insert trees into one
      const routes = colInserts.map((ins) => ins.route.flat);
      const uniqueRoute = Array.from(new Set(routes));

      /* v8 ignore if -- @preserve */
      if (uniqueRoute.length > 1) {
        throw new Error(
          `Join: Error while generating insert: ` +
            `Multiple different routes found in inserts: ` +
            `${uniqueRoute.map((r) => r.toString()).join(', ')}. ` +
            `Cannot generate single insert object.`,
        );
      }

      const merged = mergeTrees(
        colInserts.map((ins) => ({
          tree: ins.tree,
          path: ins.path.slice(0, -1),
        })),
      );

      //Delete _hash and filter null values from _data arrays
      traverse(merged, ({ parent, key, value }) => {
        if (key == '_hash') {
          //delete parent![key];
        }
        if (key == '_data' && Array.isArray(value) && value.length > 0) {
          parent![key] = value.filter((v) => !!v);
        }
      });

      inserts.push({
        route: Route.fromFlat(uniqueRoute[0]).toRouteWithProperty(),
        tree: merged,
      });
    }

    return inserts;
  }

  // ...........................................................................
  /**
   * Returns the value at the given row and column index
   *
   * @param row The row index
   * @param column The column index
   * @returns The value at the given row and column
   */
  value(row: number, column: number): JsonValue[] {
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
   * Route caches per column selection. Column selections are immutable,
   * so derived routes can be memoized for the lifetime of the selection.
   */
  private static readonly _derivedRoutes = new WeakMap<
    ColumnSelection,
    { component?: Route[]; layer?: Route[]; cake?: Route[] }
  >();

  private _derivedRoutesFor(selection: ColumnSelection): {
    component?: Route[];
    layer?: Route[];
    cake?: Route[];
  } {
    let entry = Join._derivedRoutes.get(selection);
    if (!entry) {
      entry = {};
      Join._derivedRoutes.set(selection, entry);
    }
    return entry;
  }

  // ...........................................................................
  /**
   * Returns all component routes of the join
   */
  get componentRoutes(): Route[] {
    const cached = this._derivedRoutesFor(this.columnSelection);
    cached.component ??= Array.from(
      new Set(
        Object.values(this.columnSelection.columns).map(
          (c) => Route.fromFlat(c.route).upper().flatWithoutRefs,
        ),
      ),
    ).map((r) => Route.fromFlat(r));
    return cached.component;
  }

  // ...........................................................................
  /**
   * Returns all layer routes of the join
   */
  get layerRoutes(): Route[] {
    const cached = this._derivedRoutesFor(this.columnSelection);
    cached.layer ??= Array.from(
      new Set(
        Object.values(this.columnSelection.columns)
          .map((c) => [
            Route.fromFlat(c.route).top,
            Route.fromFlat(c.route).deeper(1).top,
          ])
          .map((segments) => new Route(segments).flat),
      ),
    ).map((r) => Route.fromFlat(r));
    return cached.layer;
  }

  // ...........................................................................
  /**
   * Returns the cake route of the join
   */
  get cakeRoute(): Route {
    const cached = this._derivedRoutesFor(this.columnSelection);
    const cakeRoute = (cached.cake ??= Array.from(
      new Set(
        Object.values(this.columnSelection.columns).map(
          (c) => Route.fromFlat(c.route).top.tableKey,
        ),
      ),
    ).map((r) => Route.fromFlat(r)));

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
    const data = this.data;
    const cached = Join._rowsCache.get(data);
    if (cached) {
      return cached;
    }

    const result: any[][] = [];
    const sliceIds = Object.keys(data);

    if (sliceIds.length === 0) {
      Join._rowsCache.set(data, result);
      return result;
    }

    // The column order is uniform across all rows: resolve each selected
    // column's index once against the first row instead of route-parsing
    // inside a find() per cell.
    const firstRowColumns = (data[sliceIds[0]] as JoinRowHashed).columns;
    const routeToIndex = new Map<string, number>();
    for (let i = 0; i < firstRowColumns.length; i++) {
      routeToIndex.set(firstRowColumns[i].route.flatWithoutRefs, i);
    }
    const columnIndices = this.columnSelection.columns.map((colInfo) => {
      const index = routeToIndex.get(Route.fromFlat(colInfo.route).flatWithoutRefs);
      /* v8 ignore next -- @preserve */
      return index === undefined ? -1 : index;
    });

    for (const sliceId of sliceIds) {
      const dataColumns = (data[sliceId] as JoinRowHashed).columns;
      const row: any[] = [];
      for (const index of columnIndices) {
        /* v8 ignore next -- @preserve */
        const joinCol = index >= 0 ? dataColumns[index] : undefined;
        /* v8 ignore next -- @preserve */
        const insertValue =
          joinCol && joinCol.inserts
            ? joinCol.inserts.flatMap((con) =>
                con.cell.flatMap((c) => c.value),
              ) ?? null
            : null;
        /* v8 ignore next -- @preserve */
        const baseValue =
          joinCol && joinCol.value.cell
            ? joinCol.value.cell.flatMap((c) => c.value) ?? null
            : null;

        row.push(insertValue ?? baseValue);
      }
      result.push(row);
    }

    Join._rowsCache.set(data, result);
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
      const cols = rows[sliceId];
      /* v8 ignore next -- @preserve */
      hashedRows[sliceId] = Join._lazyHashedRow(
        cols,
        () =>
          cols.map((col) =>
            col.inserts?.flatMap((con) => con.cell.flatMap((c) => c.value)) ??
            col.value.cell
              ? col.value.cell.flatMap((c) => c.value)
              : [],
          ) as any[],
      );
    }
    return hashedRows;
  }
}
