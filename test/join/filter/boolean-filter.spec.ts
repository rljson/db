// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, expect, it } from 'vitest';

import { exampleBooleanFilter } from '../../../src/join/filter/boolean-filter';
import { ColumnSelection } from '../../../src/join/selection/column-selection';


describe('BooleanFilter', function () {
  const columnFilter = exampleBooleanFilter();

  it('matches example column selection', () => {
    const exampleColumnSelection = ColumnSelection.example();
    const columnIndex = exampleColumnSelection.columnIndex(columnFilter.column);
    expect(columnIndex).toBe(3);
  });

  it('type', () => {
    expect(columnFilter.type).toEqual(typeof true);
  });

  it('column', () => {
    expect(columnFilter.column).toEqual('basicTypes/booleansRef/value');
  });

  it('operator', () => {
    expect(columnFilter.operator).toEqual('equals');
  });

  it('value', () => {
    expect(columnFilter.search).toEqual(true);
  });

  it('_hash', () => {
    expect(columnFilter._hash).toEqual('wU_6AhjGDTC-g5PJWEioV2');
  });
});
