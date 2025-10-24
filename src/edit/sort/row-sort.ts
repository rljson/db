// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Route } from '@rljson/rljson';

import { ColumnSelection } from '../selection/column-selection.ts';
import { View } from '../view/view.ts';

/// Sort configuration for catalog data
export class RowSort {
  constructor(columnSorts: Record<string, 'asc' | 'desc'>) {
    this._columnSorts = this._initColumnSorts(columnSorts);
  }

  // ...........................................................................
  /**
   * Returns an empty RowSort object.
   */
  static get empty(): RowSort {
    return new RowSort({});
  }

  // ...........................................................................
  /**
   * Sorts the rows of a view according to the sort configuration.
   * @param view - The view to be sorted
   * @returns Returns the row indices in a sorted manner
   */
  applyTo(view: View): number[] {
    if (view.rowCount === 0) {
      return view.rowIndices;
    }

    // Throw when filter specifies non existent column routes
    this._throwOnWrongRoutes(view);

    const routeHashes = view.columnSelection.routeHashes;

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
      return view.rowIndices;
    }

    // Apply the filters
    return this._sortRows(view, sortIndices, sortOrders);
  }

  // ...........................................................................
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
    view: View,
    sortIndices: number[],
    sortOrders: Array<'asc' | 'desc'>,
  ): number[] {
    const result = [...view.rowIndices];

    // Sort
    return result.sort((a, b) => {
      const rowA = view.row(a);
      const rowB = view.row(b);

      let i = 0;
      for (const index of sortIndices) {
        const sort = sortOrders[i++];
        const vA = rowA[index];
        const vB = rowB[index];
        if (vA === vB) {
          continue;
        }
        if (sort === 'asc') {
          return vA < vB ? -1 : 1;
        } else {
          return vA < vB ? 1 : -1;
        }
      }

      return 0;
    });
  }

  // ...........................................................................
  private _throwOnWrongRoutes(view: View) {
    const availableRoutes = view.columnSelection.routes;
    for (const item of Object.values(this._columnSorts)) {
      const route = item.route;
      if (availableRoutes.includes(route) === false) {
        throw new Error(
          `RowFilterProcessor: Error while applying sort to view: ` +
            `There is a sort entry for route "${route}", but the view ` +
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
