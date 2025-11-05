// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { ColumnFilterProcessor } from './column-filter-processor.ts';
import { exampleStringFilter, StringFilter } from './string-filter.ts';

export class StringFilterProcessor implements ColumnFilterProcessor {
  constructor(
    public readonly operator: StringOperator,

    search: string,
    public readonly matchCase: boolean = false,
  ) {
    this.search = (matchCase ? search : search.toLowerCase()).replaceAll(
      ' ',
      '',
    );

    /* v8 ignore start */
    try {
      this.regExp = operator === 'regExp' ? new RegExp(search) : null;
    } catch {}
    /* v8 ignore stop */
  }

  // ...........................................................................
  static fromModel(model: StringFilter): StringFilterProcessor {
    return new StringFilterProcessor(
      model.operator,
      model.search,
      model.matchCase,
    );
  }

  // ...........................................................................
  search: string;
  regExp: RegExp | null = null;

  // ...........................................................................
  equals(other: ColumnFilterProcessor): boolean {
    return (
      other instanceof StringFilterProcessor &&
      this.operator === other.operator &&
      this.search === other.search &&
      this.matchCase === other.matchCase
    );
  }

  // ...........................................................................
  static readonly allOperators: StringOperator[] = [
    'contains',
    'equals',
    'notEquals',
    'startsWith',
    'notContains',
    'endsWith',
    'regExp',
  ];

  // ...........................................................................
  matches(cellValue: any): boolean {
    if (!this.search) {
      return true;
    }

    if (cellValue === null || cellValue === undefined) {
      return false;
    }

    if (typeof cellValue !== 'string') {
      cellValue = `${cellValue}`;
    }

    if (!this.matchCase) {
      cellValue = cellValue.toLowerCase();
    }

    cellValue = cellValue.replaceAll(' ', '');

    switch (this.operator) {
      case 'equals':
        return cellValue === this.search;
      case 'notEquals':
        return cellValue !== this.search;
      case 'startsWith':
        return cellValue.startsWith(this.search);
      case 'contains':
        return cellValue.includes(this.search);
      case 'endsWith':
        return cellValue.endsWith(this.search);
      /* v8 ignore next -- @preserve */
      case 'regExp':
        return this.regExp?.test(cellValue) ?? false;

      case 'notContains':
        return !cellValue.includes(this.search);
    }
  }

  // ...........................................................................
  static get example(): StringFilterProcessor {
    const result = StringFilterProcessor.fromModel(exampleStringFilter());
    return result;
  }
}

export type StringOperator =
  | 'startsWith'
  | 'contains'
  | 'endsWith'
  | 'equals'
  | 'notEquals'
  | 'notContains'
  | 'regExp';
