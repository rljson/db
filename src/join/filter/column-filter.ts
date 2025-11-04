// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';
import { Json, JsonValue } from '@rljson/json';

import { BoolOperator } from './boolean-filter-processor.ts';
import { NumberOperator } from './number-filter-processor.ts';
import { StringOperator } from './string-filter-processor.ts';

/** Describes a particularly filter setting  */
export interface ColumnFilter<T extends JsonValue> extends Json {
  /** The data type of the filter value. */
  type: 'number' | 'string' | 'boolean';

  /** The address of the column, the filter is applied to.  */
  column: string;

  /** The operator the filter is using.  */
  operator: NumberOperator | StringOperator | BoolOperator;

  /** The value the data are filtered / searched for. */
  search: T;

  /** The json hash of the filter */
  _hash: string;
}

// .............................................................................
/// An example filter model for test purposes
export const exampleColumnFilter = (): ColumnFilter<number> =>
  hip<ColumnFilter<number>>({
    column: 'basicTypes/numbersRef/intsRef/value',
    operator: 'equals',
    type: 'number',
    search: 24,
    _hash: '',
  });
