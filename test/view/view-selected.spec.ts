// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, expect, it } from 'vitest';

import { ViewSelected } from '../../src/edit/view/view-selected';

describe.skip('ViewSelected', () => {
  const viewSelected = ViewSelected.example;

  describe('row', () => {
    it('returns the row data for the given index', () => {
      expect(viewSelected.rows).toEqual([
        ['Zero', 0],
        ['OneA', 1],
        ['Two', 2],
        ['OneB', 11],
        ['True', 12],
      ]);
    });
  });

  describe('rowCount', () => {
    it('returns the number of rows in the view', () => {
      expect(viewSelected.rowCount).toBe(5);
    });
  });

  describe('rowIndices', () => {
    it('returns the row indices of the view', () => {
      expect(viewSelected.rowIndices).toEqual([0, 1, 2, 3, 4]);
    });
  });

  describe('value(row, col)', () => {
    it('returns the value of the cell at the given row and column', () => {
      expect(viewSelected.value(0, 0)).toBe('Zero');
      expect(viewSelected.value(0, 1)).toBe(0);
      expect(viewSelected.value(1, 0)).toBe('OneA');
      expect(viewSelected.value(1, 1)).toBe(1);
      expect(viewSelected.value(2, 0)).toBe('Two');
      expect(viewSelected.value(2, 1)).toBe(2);
      expect(viewSelected.value(3, 0)).toBe('OneB');
      expect(viewSelected.value(3, 1)).toBe(11);
    });
  });

  it('columnTypes', () => {
    expect(ViewSelected.example.columnTypes).toEqual(['string', 'number']);
  });
});
