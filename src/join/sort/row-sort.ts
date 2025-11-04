// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Route, SliceId } from '@rljson/rljson';

import { Join } from '../join.ts';
import { ColumnSelection } from '../selection/column-selection.ts';


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
    const result = [...join.rowIndices];

    // Sort
    return result.sort((a, b) => {
      const rowA = join.row(a);
      const rowB = join.row(b);

      let i = 0;
      for (const index of sortIndices) {
        const sort = sortOrders[i++];
        const vA = rowA[index].insert ?? rowA[index].value;
        const vB = rowB[index].insert ?? rowB[index].value;
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
