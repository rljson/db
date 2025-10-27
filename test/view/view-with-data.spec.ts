// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, expect, it } from 'vitest';

import { ViewWithData } from '../../src/edit/view/view-with-data';

import { expectGolden } from '../setup/goldens';

describe('ViewWithData', () => {
  const view = ViewWithData.example();

  it('rowCount', () => {
    expect(view.rowCount).toBe(5);
  });

  it('columnCount', () => {
    expect(view.columnCount).toBe(7);
  });

  it('row', () => {
    expectGolden('view-with-data/row-0.json').toBe(view.row(0));
    expectGolden('view-with-data/row-1.json').toBe(view.row(1));
  });

  it('rowIndices', () => {
    expectGolden('view-with-data/row-indices.json').toBe(view.rowIndices);
  });

  it('rowHashes', () => {
    expectGolden('view-with-data/row-hashes.json').toBe(view.rowHashes);
  });

  it('columnTypes', () => {
    expectGolden('view-with-data/column-types.json').toBe(view.columnTypes);
  });

  it('_hash', () => {
    expectGolden('view-with-data/view-hash.json').toBe(view._hash);
  });
});
