// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Hash } from '@rljson/hash';

import { describe, expect, it } from 'vitest';

import { exampleNumberFilter } from '../../src/edit/filter/number-filter';
import { ColumnSelection } from '../../src/edit/selection/column-selection';

describe('NumberFilter', function () {
  const filter = exampleNumberFilter();

  it('matches example column selection', () => {
    const exampleColumnSelection = ColumnSelection.example();
    const columnIndex = exampleColumnSelection.columnIndex(filter.column);
    expect(columnIndex).toBe(1);
  });

  it('type', () => {
    expect(filter.type).toEqual(typeof 24);
  });

  it('column', () => {
    expect(filter.column).toEqual('basicTypes/numbersRef/intsRef/value');
  });

  it('operator', () => {
    expect(filter.operator).toEqual('greaterThanOrEquals');
  });

  it('value', () => {
    expect(filter.search).toEqual(1000);
  });

  it('_hash', () => {
    const jh = Hash.default;
    jh.validate(filter);
    expect(filter._hash).toEqual('SykliIdQ7CiN6VJ51DoRXS');
  });
});
