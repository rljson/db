// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';
import { Json } from '@rljson/json';

import { exampleBooleanFilter } from './boolean-filter.ts';
import { ColumnFilter } from './column-filter.ts';
import { exampleNumberFilter } from './number-filter.ts';
import { exampleStringFilter } from './string-filter.ts';

/** Describes the data of a row filter */
export interface RowFilter extends Json {
  /**
   * The filters applied to specific columns of the table.
   */
  columnFilters: ColumnFilter<any>[];

  /**
   * The operator the column filters are combined with.
   */
  operator: 'and' | 'or';

  /** The json hash of the object */
  _hash: string;
}

// .............................................................................
/** An example row filter for test purposes */
export const exampleRowFilter = (): RowFilter =>
  hip<RowFilter>({
    columnFilters: [
      exampleBooleanFilter(),
      exampleStringFilter(),
      exampleNumberFilter(),
    ],
    operator: 'and',
    _hash: '',
  });

/** En empty row filter doing nothing */
export const emptyRowFilter: RowFilter = hip({
  columnFilters: [],
  operator: 'and',
  _hash: '',
});
