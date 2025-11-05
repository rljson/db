// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';

import { BoolOperator } from './boolean-filter-processor.ts';
import { ColumnFilter } from './column-filter.ts';

export interface BooleanFilter extends ColumnFilter<boolean> {
  type: 'boolean';
  operator: BoolOperator;
}

export const trueValues = ['t', 'j', 'y'];
export const falseValues = ['n', 'f'];

/**
 * Parses a boolean search
 * @param search The search value to be parsed
 * @returns the parse result
 */
export const parseBooleanSearch = (search: any): boolean | null => {
  if (typeof search == 'undefined' || search == null) {
    return null;
  }

  if (typeof search == 'boolean') {
    return search;
  }

  if (typeof search == 'number') {
    return search != 0;
  }

  if (typeof search == 'string') {
    const val = search.toLowerCase();

    for (const trueValue of trueValues) {
      if (val.startsWith(trueValue)) {
        return true;
      }
    }

    for (const falseValue of falseValues) {
      if (val.startsWith(falseValue)) {
        return false;
      }
    }

    const containsOnlyNumbers = /^\d+$/.test(search);
    /* v8 ignore if -- @preserve */
    if (containsOnlyNumbers) {
      return parseInt(search) != 0;
    }
  }

  return false;
};

// ..............................................................................

export const exampleBooleanFilter = (): BooleanFilter =>
  hip<BooleanFilter>({
    column: 'basicTypes/booleansRef/value',
    operator: 'equals',
    type: 'boolean',
    search: true,
    _hash: '',
  });
