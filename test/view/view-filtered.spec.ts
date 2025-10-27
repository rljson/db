// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { beforeEach, describe, expect, it } from 'vitest';

import { RowFilterProcessor } from '../../src/edit/filter/row-filter-processor';
import { StringFilterProcessor } from '../../src/edit/filter/string-filter-processor';
import { ViewFiltered } from '../../src/edit/view/view-filtered';
import { ViewSelected } from '../../src/edit/view/view-selected';

describe.skip('ViewFiltered', () => {
  describe('wth an empty filter', () => {
    let original: ViewSelected;
    let filtered: ViewFiltered;

    beforeEach(() => {
      original = ViewSelected.example;
      filtered = new ViewFiltered(original, RowFilterProcessor.empty);
    });

    describe('row', () => {
      it('returns the row data for the given index', () => {
        for (let i = 0; i < original.rowCount; i++) {
          expect(filtered.row(i)).toEqual(original.row(i));
        }
      });
    });

    it('rows', () => {
      expect(filtered.rows).toBe(original.rows);
    });

    describe('rowCount', () => {
      it('returns the number of rows in the view', () => {
        expect(filtered.rowCount).toBe(original.rowCount);
      });
    });

    describe('rowIndices', () => {
      it('returns the row indices of the view', () => {
        expect(filtered.rowIndices).toEqual([0, 1, 2, 3, 4]);
      });
    });

    describe('value(row, col)', () => {
      it('returns the value of the cell at the given row and column', () => {
        for (let i = 0; i < original.rowCount; i++) {
          for (let j = 0; j < original.columnCount; j++) {
            expect(filtered.value(i, j)).toBe(original.value(i, j));
          }
        }
      });
    });
  });

  describe('with a filter applied', () => {
    let filter: RowFilterProcessor;
    let filtered: ViewFiltered;

    beforeEach(() => {
      filter = new RowFilterProcessor(
        {
          'basicTypes/stringsRef/value': new StringFilterProcessor(
            'startsWith',
            'O',
          ),
        },
        'and',
      );
      filtered = ViewFiltered.example(filter);
    });

    describe('row', () => {
      it('returns the row data for the given index', () => {
        expect(filtered.row(0)).toEqual(['OneA', 1]);
        expect(filtered.row(1)).toEqual(['OneB', 11]);
      });
    });

    describe('rowCount', () => {
      it('returns the number of rows in the view', () => {
        expect(filtered.rowCount).toBe(2);
      });
    });

    describe('rowIndices', () => {
      it('returns the row indices of the view', () => {
        expect(filtered.rowIndices).toEqual([0, 1]);
      });
    });

    describe('value(row, col)', () => {
      it('returns the value of the cell at the given row and column', () => {
        expect(filtered.value(0, 0)).toBe('OneA');
        expect(filtered.value(0, 1)).toBe(1);
        expect(filtered.value(1, 0)).toBe('OneB');
        expect(filtered.value(1, 1)).toBe(11);
      });
    });

    describe('rowHashes', () => {
      it('should be the same as the original view', () => {
        const original = ViewSelected.example;

        const filter = new RowFilterProcessor({
          'basicTypes/stringsRef/value': new StringFilterProcessor(
            'startsWith',
            'O',
          ),
        });
        const filtered = new ViewFiltered(original, filter);

        let i = 0;
        for (const row of filtered.rows) {
          const originalIndex = original.rows.indexOf(row);
          const originalHash = original.rowHashes[originalIndex];
          const ownHash = filtered.rowHashes[i];
          expect(ownHash).toBe(originalHash);
          i++;
        }
      });
    });
  });

  it('columnTypes', () => {
    const original = ViewSelected.example;
    const filtered = new ViewFiltered(original, RowFilterProcessor.empty);

    expect(filtered.columnTypes).toEqual(original.columnTypes);
  });
});
