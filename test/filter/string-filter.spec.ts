// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Hash } from '@rljson/hash';

import { describe, expect, it } from 'vitest';

import { exampleStringFilter } from '../../src/edit/filter/string-filter';
import { ColumnSelection } from '../../src/edit/selection/column-selection';

describe('StringFilter', function () {
  const filter = exampleStringFilter();

  it('matches example column selection', () => {
    const exampleColumnSelection = ColumnSelection.example();
    const columnIndex = exampleColumnSelection.columnIndex(filter.column);
    expect(columnIndex).toBe(0);
  });

  it('type', () => {
    expect(filter.type).toEqual(typeof 'Hello');
  });

  it('column', () => {
    expect(filter.column).toEqual('basicTypes/stringsRef/value');
  });

  it('operator', () => {
    expect(filter.operator).toEqual('startsWith');
  });

  it('value', () => {
    expect(filter.search).toEqual('t');
  });

  it('matchCase', () => {
    expect(filter.matchCase).toEqual(false);
  });

  it('_hash', () => {
    const jh = Hash.default;
    jh.validate(filter);
    expect(filter._hash).toHaveLength(22);
  });
});
