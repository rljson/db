// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Route, RouteRef, SliceId } from '@rljson/rljson';

import { Join } from '../join.ts';
import { ColumnSelection } from '../selection/column-selection.ts';

export type RowSortOrder = 'asc' | 'desc';
export type RowSortType = Record<RouteRef, RowSortOrder>;

/// Sort configuration for catalog data
export class RowSort {
  constructor(columnSorts: Record<string, 'asc' | 'desc'>) {
    this._columnSorts = this._initColumnSorts(columnSorts);
  }

  // ...........................................................................
  /**
   * Sorts the rows of a join according to the sort configuration.
   * @param join - The join to be sorted
   * @returns Returns the row indices in a sorted manner
   */
  applyTo(join: Join): SliceId[] {
    if (join.rowCount === 0) {
      return join.rowIndices;
    }

    // Throw when filter specifies non existent column routes
    this._throwOnWrongRoutes(join);

    const routeHashes = join.columnSelection.routeHashes;

    // Generate an array of sort operators
    const sortIndices: number[] = [];
    const sortOrders: Array<'asc' | 'desc'> = [];

    let hasSorts = false;
    for (const item of this._columnSorts) {
      const index = routeHashes.indexOf(item.routeHash);
      sortIndices.push(index);
      sortOrders.push(item.order);

      hasSorts = true;
    }

    // No filters set? Return unchanged rows.
    if (!hasSorts) {
      return join.rowIndices;
    }

    // Apply the filters
    return this._sortRows(join, sortIndices, sortOrders);
  }

  // ...........................................................................
  /* v8 ignore next -- @preserve */
  get columnSorts(): Record<string, 'asc' | 'desc'> {
    const result: Record<string, 'asc' | 'desc'> = {};
    for (const sort of this._columnSorts) {
      result[sort.route] = sort.order;
    }

    return result;
  }

  // ######################
  // Private
  // ######################

  private readonly _columnSorts: _SortItem[];

  // ...........................................................................
  private _initColumnSorts(
    columnSorts: Record<string, 'asc' | 'desc'>,
  ): _SortItem[] {
    const result: _SortItem[] = [];
    const columns = Object.keys(columnSorts);
    const columnSelection = ColumnSelection.fromRoutes(
      columns.map((c) => Route.fromFlat(c)),
    );

    const routes = columnSelection.routes;
    const routeHashes = columnSelection.routeHashes;

    for (let i = 0; i < routes.length; i++) {
      const route = routes[i];
      const routeHash = routeHashes[i];
      result.push({
        route,
        routeHash,
        order: columnSorts[route],
      });
    }

    return result;
  }

  // ...........................................................................
  private _sortRows(
    join: Join,
    sortIndices: number[],
    sortOrders: Array<'asc' | 'desc'>,
  ): SliceId[] {
    const rowIndices = join.rowIndices;

    // Precompute the sort key of every row once (one pass) instead of
    // re-deriving the values inside the O(n log n) comparator
    //TODO: Make cells holding several values sortable
    const sortKeys = new Map<SliceId, Array<unknown>>();
    for (const sliceId of rowIndices) {
      const row = join.row(sliceId);
      const keys: Array<unknown> = [];
      for (const index of sortIndices) {
        const col = row[index];

        /* v8 ignore next -- @preserve */
        const insertValues = col.inserts
          ? Array.isArray(col.inserts[0].cell[0].value)
            ? col.inserts[0].cell[0].value!
            : [col.inserts[0].cell[0].value!]
          : null;
        /* v8 ignore next -- @preserve */
        const baseValues = col.value.cell[0].value
          ? Array.isArray(col.value?.cell[0].value)
            ? col.value.cell[0].value!
            : [col.value.cell[0].value!]
          : null;
        /* v8 ignore next -- @preserve */
        keys.push(
          insertValues && insertValues[0]
            ? insertValues[0]
            : baseValues
              ? baseValues[0]
              : null,
        );
      }
      sortKeys.set(sliceId, keys);
    }

    const result = [...rowIndices];

    // Sort
    return result.sort((a, b) => {
      const keysA = sortKeys.get(a)!;
      const keysB = sortKeys.get(b)!;

      for (let i = 0; i < sortIndices.length; i++) {
        const sort = sortOrders[i];
        const vA = keysA[i];
        const vB = keysB[i];

        if (vA === vB) {
          continue;
        }
        if (sort === 'asc') {
          return vA! < vB! ? -1 : 1;
        } else {
          return vA! < vB! ? 1 : -1;
        }
      }

      return 0;
    });
  }

  // ...........................................................................
  private _throwOnWrongRoutes(join: Join) {
    const availableRoutes = join.columnSelection.routes;
    for (const item of Object.values(this._columnSorts)) {
      const route = item.route;
      if (availableRoutes.includes(route) === false) {
        throw new Error(
          `RowFilterProcessor: Error while applying sort to join: ` +
            `There is a sort entry for route "${route}", but the join ` +
            `does not have a column with this route.\n\nAvailable routes:\n` +
            `${availableRoutes.map((a: string) => `- ${a}`).join('\n')}`,
        );
      }
    }
  }
}

// #############################################################################
interface _SortItem {
  order: 'asc' | 'desc';
  routeHash: string;

  route: string;
}
