// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { compileExpression } from 'filtrex';

import { ColumnFilterProcessor } from './column-filter-processor.ts';
import { exampleNumberFilter, NumberFilter } from './number-filter.ts';

// #############################################################################
export class NumberFilterProcessor implements ColumnFilterProcessor {
  constructor(
    public readonly operator: NumberOperator,

    search: number | string,
  ) {
    if (operator === 'filtrex') {
      this.search = search;
      this._initFiltrex();
    } else {
      this.search = typeof search == 'string' ? parseFloat(search) : search;
    }
  }

  // ...........................................................................
  static fromModel(model: NumberFilter): NumberFilterProcessor {
    return new NumberFilterProcessor(model.operator, model.search);
  }

  equals(other: ColumnFilterProcessor): boolean {
    return (
      other instanceof NumberFilterProcessor &&
      this.operator === other.operator &&
      this.search === other.search
    );
  }

  // ...........................................................................
  static readonly allOperators: NumberOperator[] = [
    'equals',
    'notEquals',
    'greaterThan',
    'greaterThanOrEquals',
    'lessThan',
    'lessThanOrEquals',
    'filtrex',
  ];

  // ...........................................................................
  matches(cellValue: any): boolean {
    if (!this.search && this.search !== 0) {
      return true;
    }

    if (cellValue === null || cellValue === undefined) {
      return false;
    }

    switch (this.operator) {
      case 'equals':
        return cellValue === this.search;
      case 'notEquals':
        return cellValue !== this.search;
      case 'greaterThan':
        return cellValue > this.search;
      case 'lessThan':
        return cellValue < this.search;
      case 'greaterThanOrEquals':
        return cellValue >= this.search;
      case 'lessThanOrEquals':
        return cellValue <= this.search;
      case 'filtrex':
        return this?._evalExpression(cellValue);
    }
  }

  search: number | string = '';

  static get example(): NumberFilterProcessor {
    return NumberFilterProcessor.fromModel(exampleNumberFilter());
  }

  // ######################
  // Private
  // ######################

  private _expression: any = null;

  // ..........................................................................
  private _initFiltrex() {
    // If search is empty, no expression is needed
    if (this.search === '') {
      return;
    }

    // If search is a number, we can use it directly
    if (typeof this.search === 'number') {
      return;
    }

    // Check if this contains only numbers
    const isNumber = /^\d+$/.test(this.search);
    if (isNumber) {
      this.search = parseInt(this.search);
      return;
    }

    // Try to compile the expression
    try {
      this._expression = compileExpression(this.search);
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
    } catch (_) {
      // As long as the expression is not valid,
      // the searched value must equal the cell value
      this._expression = '';
    }
  }

  // ...........................................................................
  private _evalExpression(cellValue: any): boolean {
    return this._expression
      ? // result can also contain errors which could be sent to the outside here
        this._expression({ v: cellValue }) == true
      : cellValue == this.search;
  }
}

export type NumberOperator =
  | 'equals'
  | 'notEquals'
  | 'greaterThan'
  | 'greaterThanOrEquals'
  | 'lessThan'
  | 'lessThanOrEquals'
  | 'filtrex';
