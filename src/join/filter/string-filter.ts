// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';

import { ColumnFilter } from './column-filter.ts';
import { StringOperator } from './string-filter-processor.ts';

export interface StringFilter extends ColumnFilter<string> {
  type: 'string';
  operator: StringOperator;
  matchCase?: boolean;
}

// ..............................................................................

export const exampleStringFilter = (): StringFilter =>
  hip<StringFilter>({
    type: 'string',
    column: 'basicTypes/stringsRef/value',
    operator: 'startsWith',
    search: 't',
    matchCase: false,
    _hash: '',
  });
