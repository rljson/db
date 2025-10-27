// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Hash } from '@rljson/hash';

import { describe, expect, it } from 'vitest';

import { exampleColumnFilter } from '../../src/edit/filter/column-filter';

describe('ColumnFilter', function () {
  const columnFilter = exampleColumnFilter();

  it('type', () => {
    expect(columnFilter.type).toEqual('number');
  });

  it('column', () => {
    expect(columnFilter.column).toEqual('basicTypes/numbersRef/intsRef/value');
  });

  it('operator', () => {
    expect(columnFilter.operator).toEqual('equals');
  });

  it('value', () => {
    expect(columnFilter.search).toEqual(24);
  });

  it('_hash', () => {
    const jh = Hash.default;
    jh.validate(columnFilter);
    expect(columnFilter._hash).toEqual('ef-3rgSpwOvMyMeQCmB5vV');
  });
});
