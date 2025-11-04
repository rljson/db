// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Route } from '@rljson/rljson';

import { ColumnSelection } from '../join/selection/column-selection.ts';

import { MultiEditResolved } from './multi-edit-resolved.ts';

/**
 * Takes an multi edit and estimates all columns needed to apply the multi edit.
 */
export class MultiEditColumnEstimator {
  /**
   * Constructor
   * @param multiEdit The multi edit to get the used columns from.
   * @param cache  The cache to store the results for later reuse.
   * @param cacheSize The size of the cache.
   */
  constructor(
    public readonly multiEdit: MultiEditResolved,
    public readonly cache: Map<string, string[]> = new Map(),
    public readonly cacheSize = 3,
  ) {
    this._columnSelection = this._initColumnSelection();
  }

  /**
   * Returns the column selection that is needed to apply the multi edit.
   */
  get columnSelection(): ColumnSelection {
    return this._columnSelection;
  }

  /**
   * Returns the number of cache usages.
   */
  get cacheUsages(): number {
    return this._cacheUsages;
  }

  static get example() {
    const multiEdit = MultiEditResolved.example;
    return new MultiEditColumnEstimator(multiEdit);
  }

  // ######################
  // Private
  // ######################

  private _columnSelection: ColumnSelection;
  private _cacheUsages: number = 0;

  // ...........................................................................
  private _initColumnSelection(): ColumnSelection {
    const routes: string[] = [];
    this._collectRoutes(this.multiEdit, routes);
    return ColumnSelection.fromRoutes(
      Array.from(routes.map((r) => Route.fromFlat(r))),
    );
  }

  // ...........................................................................
  private _collectRoutes(multiEdit: MultiEditResolved, routes: string[]) {
    // Use cached result when available
    const cachedResult = this.cache.get(multiEdit._hash);
    if (cachedResult) {
      this._cacheUsages++;
      routes.push(...cachedResult);
      return;
    }

    if (multiEdit.previous) {
      this._collectRoutes(multiEdit.previous, routes);
    }

    // Collect columns from filters
    for (const filter of multiEdit.edit.filter.columnFilters) {
      routes.push(filter.column);
    }

    // Collect columns from all actions
    for (const action of multiEdit.edit.actions) {
      routes.push(action.route);
    }

    // Cache result
    this._cacheResult(multiEdit, routes);

    // Return result
    return routes;
  }

  // ...........................................................................
  private _cacheResult(multiEdit: MultiEditResolved, result: string[]) {
    result = Array.from(new Set(result));
    this.cache.set(multiEdit._hash, result);
    if (this.cache.size > this.cacheSize) {
      this.cache.delete(this.cache.keys().next().value!);
    }
  }
}
