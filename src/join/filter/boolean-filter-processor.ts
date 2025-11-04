// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import {
  BooleanFilter,
  exampleBooleanFilter,
  parseBooleanSearch,
} from './boolean-filter.ts';
import { ColumnFilterProcessor } from './column-filter-processor.ts';

export class BooleanFilterProcessor implements ColumnFilterProcessor {
  constructor(
    public readonly operator: BoolOperator,

    search: boolean | string | number | null,
  ) {
    this.search =
      search === null
        ? null
        : typeof search === 'boolean'
        ? search
        : parseBooleanSearch(search);
  }

  search: boolean | null;

  // ...........................................................................
  static fromModel(model: BooleanFilter): BooleanFilterProcessor {
    return new BooleanFilterProcessor(model.operator, model.search);
  }

  // ...........................................................................
  equals(other: ColumnFilterProcessor): boolean {
    return (
      other instanceof BooleanFilterProcessor &&
      this.operator === other.operator &&
      this.search === other.search
    );
  }

  // ...........................................................................
  static readonly allOperators: BoolOperator[] = ['equals', 'notEquals'];

  // ...........................................................................
  matches(cellValue: any): boolean {
    if (this.search === null || this.search === undefined) {
      return true;
    }

    if (cellValue === null || cellValue === undefined) {
      return false;
    }

    if (typeof cellValue == 'number') {
      cellValue = cellValue != 0;
    }

    if (typeof cellValue == 'string') {
      const val = cellValue.toLowerCase();
      cellValue = val === 'true' || val === 'yes' || '1';
    }

    switch (this.operator) {
      case 'equals':
        return cellValue === this.search;
      case 'notEquals':
        return cellValue !== this.search;
    }
  }

  // ...........................................................................
  static get example(): BooleanFilterProcessor {
    const model: BooleanFilter = exampleBooleanFilter();
    const filterProcessor = BooleanFilterProcessor.fromModel(model);
    return filterProcessor;
  }
}

export type BoolOperator = 'equals' | 'notEquals';
