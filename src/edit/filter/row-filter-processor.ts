// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Route } from '@rljson/rljson';

import { ColumnSelection } from '../selection/column-selection.ts';
import { View } from '../view/view.ts';

import { ColumnFilterProcessor } from './column-filter-processor.ts';
import { RowFilter } from './row-filter.ts';

// #############################################################################
export class RowFilterProcessor {
  // ...........................................................................
  constructor(
    columnFilters: Record<string, ColumnFilterProcessor>,
    public readonly operator: 'and' | 'or' = 'and',
  ) {
    this._columnFilters = this._initColumnFilters(columnFilters);
  }

  // ...........................................................................
  static fromModel(model: RowFilter) {
    const operator = model.operator;
    const columnFilters: Record<string, ColumnFilterProcessor> = {};
    for (const columnFilter of model.columnFilters) {
      const key = columnFilter.column;
      const processor = ColumnFilterProcessor.fromModel(columnFilter);
      columnFilters[key] = processor;
    }

    return new RowFilterProcessor(columnFilters, operator);
  }

  // ...........................................................................
  get processors(): ColumnFilterProcessor[] {
    return Object.values(this._columnFilters).map((item) => item.processor);
  }

  // ...........................................................................
  /// Returns an empty filter
  static get empty(): RowFilterProcessor {
    return new RowFilterProcessor({}, 'and');
  }

  // ...........................................................................
  /// Checks if two filters are equal
  equals(other: RowFilterProcessor): boolean {
    if (this.operator !== other.operator) {
      return false;
    }

    const thisKeys = Object.keys(this._columnFilters);
    const otherKeys = Object.keys(other._columnFilters);

    if (thisKeys.length !== otherKeys.length) {
      return false;
    }

    for (const key of thisKeys) {
      const a = this._columnFilters[key];
      const b = other._columnFilters[key];
      if (a?.processor.equals(b?.processor) === false) {
        return false;
      }
    }

    return true;
  }

  // ...........................................................................
  /// Returns the indices of the rows that match the filter
  ///
  /// Returns null if no filters are set or match the given row aliases.
  applyTo(view: View): number[] {
    if (view.rowCount === 0) {
      return view.rowIndices;
    }

    // Throw when filter specifies non existent column routes
    this._throwOnWrongRoutes(view);

    // Generate an array of filters
    const columnCount = view.columnCount;
    const columnHashes = view.columnSelection.routeHashes;

    const filterArray: ColumnFilterProcessor[] = new Array(columnCount).fill(
      null,
    );
    let hasFilters = false;
    for (let c = 0; c < columnCount; c++) {
      const hash = columnHashes[c];
      const filter = this._columnFilters[hash];
      if (filter) {
        filterArray[c] = filter.processor;
        hasFilters = true;
      }
    }

    // No filters set? Return unchanged indices.
    if (!hasFilters) {
      return view.rowIndices;
    }

    // Apply the filters
    switch (this.operator) {
      case 'and':
        return this._filterRowsAnd(view, filterArray);
      case 'or':
        return this._filterRowsOr(view, filterArray);
    }
  }

  // ######################
  // Private
  // ######################

  // ...........................................................................
  private readonly _columnFilters: Record<string, _ColumnFilterItem>;

  // ...........................................................................
  private _initColumnFilters(
    columnFilters: Record<string, ColumnFilterProcessor>,
  ) {
    const result: Record<string, _ColumnFilterItem> = {};

    const columnKeys = Object.keys(columnFilters);
    const columnRoutes = columnKeys.map((k) => Route.fromFlat(k));
    const columnSelection = ColumnSelection.fromRoutes(columnRoutes);

    const { routeHashes, routes } = columnSelection;

    for (let i = 0; i < routeHashes.length; i++) {
      const routeHash = routeHashes[i];
      const route = routes[i];
      const processor = columnFilters[route]!;

      result[routeHash] = {
        processor,
        routeHash,
        route,
      };
    }

    return result;
  }

  // ...........................................................................
  private _filterRowsAnd(
    view: View,
    filters: ColumnFilterProcessor[],
  ): number[] {
    // Fill the array with all row indices
    const rowCount = view.rowCount;
    let remainingIndices: number[] = new Array(rowCount);
    for (let i = 0; i < rowCount; i++) {
      remainingIndices[i] = i;
    }

    const columnCount = view.columnCount;

    for (let c = 0; c < columnCount; c++) {
      remainingIndices = this._filterColumnAnd(
        view,
        c,
        remainingIndices,
        filters,
      );
    }

    return remainingIndices;
  }

  // ...........................................................................
  private _filterColumnAnd(
    view: View,
    columnIndex: number,
    remainingIndices: number[],
    filters: ColumnFilterProcessor[],
  ): number[] {
    const result: number[] = [];
    const filter = filters[columnIndex];
    if (filter == null) {
      return remainingIndices;
    }

    for (const i of remainingIndices) {
      const cellValue = view.value(i, columnIndex);

      if (filter.matches(cellValue)) {
        result.push(i);
      }
    }

    return result;
  }

  // ...........................................................................
  private _filterRowsOr(
    view: View,
    filters: ColumnFilterProcessor[],
  ): number[] {
    // Fill the array with all row indices
    const applyTo: boolean[] = new Array(view.rowCount).fill(false);

    const columnCount = view.columnCount;

    for (let c = 0; c < columnCount; c++) {
      this._filterColumnOr(view, c, applyTo, filters);
    }

    let rowCount = 0;
    for (let r = 0; r < applyTo.length; r++) {
      if (applyTo[r]) {
        rowCount++;
      }
    }

    const result: number[] = new Array(rowCount);
    let resultIndex = 0;
    for (let r = 0; r < applyTo.length; r++) {
      if (applyTo[r]) {
        result[resultIndex] = r;
        resultIndex++;
      }
    }

    return result;
  }

  // ...........................................................................
  private _filterColumnOr(
    view: View,
    columnIndex: number,
    applyTo: boolean[],
    filters: ColumnFilterProcessor[],
  ) {
    const filter = filters[columnIndex];
    if (filter == null) {
      return;
    }

    for (let r = 0; r < view.rowCount; r++) {
      if (applyTo[r]) {
        continue;
      }

      const cellValue = view.value(r, columnIndex);

      if (filter.matches(cellValue)) {
        applyTo[r] = true;
      }
    }
  }

  // ...........................................................................
  private _throwOnWrongRoutes(view: View) {
    const availableRoutes = view.columnSelection.routes;
    for (const item of Object.values(this._columnFilters)) {
      const route = item.route;
      if (availableRoutes.includes(route) === false) {
        throw new Error(
          `RowFilterProcessor: Error while applying filter to view: ` +
            `There is a column filter for route "${route}", but the view ` +
            `does not have a column with this route.\n\nAvailable routes:\n` +
            `${availableRoutes.map((a) => `- ${a}`).join('\n')}`,
        );
      }
    }
  }
}

// #############################################################################
interface _ColumnFilterItem {
  processor: ColumnFilterProcessor;
  routeHash: string;

  route: string;
}
