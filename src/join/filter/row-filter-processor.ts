// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Route } from '@rljson/rljson';

import { Join, JoinRowsHashed } from '../join.ts';
import { ColumnSelection } from '../selection/column-selection.ts';

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
  applyTo(join: Join): JoinRowsHashed {
    if (join.rowCount === 0) {
      return join.data;
    }

    // Throw when filter specifies non existent column routes
    this._throwOnWrongRoutes(join.columnSelection);

    // Generate an array of filters
    const columnCount = join.columnCount;
    const columnHashes = join.columnSelection.routeHashes;

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
      return join.data;
    }

    // Apply the filters
    let rowIndices: number[] = [];
    switch (this.operator) {
      case 'and':
        rowIndices = this._filterRowsAnd(join, filterArray);
        break;
      case 'or':
        rowIndices = this._filterRowsOr(join, filterArray);
        break;
    }

    // Build the resulting data
    const result: JoinRowsHashed = {};
    const rowIndexSet = new Set(rowIndices);
    let idx = 0;
    for (const [sliceId, row] of Object.entries(join.data)) {
      if (rowIndexSet.has(idx)) {
        result[sliceId] = row;
      }
      idx++;
    }
    return result;
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
    join: Join,
    filters: ColumnFilterProcessor[],
  ): number[] {
    // Fill the array with all row indices
    const rowCount = join.rowCount;
    let remainingIndices: number[] = new Array(rowCount);
    for (let i = 0; i < rowCount; i++) {
      remainingIndices[i] = i;
    }

    const columnCount = join.columnCount;

    for (let c = 0; c < columnCount; c++) {
      remainingIndices = this._filterColumnAnd(
        join,
        c,
        remainingIndices,
        filters,
      );
    }

    return remainingIndices;
  }

  // ...........................................................................
  private _filterColumnAnd(
    join: Join,
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
      const cellValues = join.value(i, columnIndex);

      for (const cellValue of cellValues) {
        if (filter.matches(cellValue as string)) {
          result.push(i);
        }
      }
    }

    return result;
  }

  // ...........................................................................
  private _filterRowsOr(
    join: Join,
    filters: ColumnFilterProcessor[],
  ): number[] {
    // Fill the array with all row indices
    const applyTo: boolean[] = new Array(join.rowCount).fill(false);

    const columnCount = join.columnCount;

    for (let c = 0; c < columnCount; c++) {
      this._filterColumnOr(join, c, applyTo, filters);
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
    join: Join,
    columnIndex: number,
    applyTo: boolean[],
    filters: ColumnFilterProcessor[],
  ) {
    const filter = filters[columnIndex];
    if (filter == null) {
      return;
    }

    for (let r = 0; r < join.rowCount; r++) {
      if (applyTo[r]) {
        continue;
      }

      const cellValues = join.value(r, columnIndex);

      for (const cellValue of cellValues) {
        if (filter.matches(cellValue as string)) {
          applyTo[r] = true;
        }
      }
    }
  }

  // ...........................................................................
  private _throwOnWrongRoutes(columnSelection: ColumnSelection) {
    const availableRoutes = columnSelection.routes;
    for (const item of Object.values(this._columnFilters)) {
      const route = item.route;
      if (availableRoutes.includes(route) === false) {
        throw new Error(
          `RowFilterProcessor: Error while applying filter to join: ` +
            `There is a column filter for route "${route}", but the join ` +
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
