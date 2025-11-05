// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';

import { ColumnFilter } from './column-filter.ts';
import { NumberOperator } from './number-filter-processor.ts';

export interface NumberFilter extends ColumnFilter<number> {
  type: 'number';
  operator: NumberOperator;
}

// ..............................................................................

export const exampleNumberFilter = (): NumberFilter =>
  hip<NumberFilter>({
    column: 'basicTypes/numbersRef/intsRef/value',
    operator: 'greaterThanOrEquals',
    type: 'number',
    search: 1000,
    _hash: '',
  });
