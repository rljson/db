// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Hash } from '@rljson/hash';

import { describe, expect, it } from 'vitest';

import { exampleBooleanFilter } from '../../../src/join/filter/boolean-filter';
import { exampleNumberFilter } from '../../../src/join/filter/number-filter';
import { emptyRowFilter, exampleRowFilter } from '../../../src/join/filter/row-filter';
import { exampleStringFilter } from '../../../src/join/filter/string-filter';
import { ColumnSelection } from '../../../src/join/selection/column-selection';


describe('RowFilter', () => {
  const filter = exampleRowFilter();

  it('column filters match example column selection', () => {
    const exampleColumnSelection = ColumnSelection.example();
    for (const columnFilter of filter.columnFilters) {
      const columnIndex = exampleColumnSelection.columnIndex(
        columnFilter.column,
      );
      expect(columnIndex).toBeGreaterThanOrEqual(0);
    }
  });

  it('columnFilters', () => {
    expect(filter.columnFilters).toEqual([
      exampleBooleanFilter(),
      exampleStringFilter(),
      exampleNumberFilter(),
    ]);
  });

  it('operator', () => {
    expect(filter.operator).toBe('and');
  });

  it('_hash', () => {
    const jh = Hash.default;
    jh.validate(filter);
    expect(filter._hash).toHaveLength(22);
  });

  it('emptyRowFilter', () => {
    expect(emptyRowFilter).toEqual({
      _hash: emptyRowFilter._hash,
      columnFilters: [],
      operator: 'and',
    });
  });
});
