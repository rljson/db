// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { JsonValueType } from '@rljson/json';

import { beforeEach, describe, expect, it } from 'vitest';

import { ColumnSelection } from '../../src/edit/selection/column-selection';
import { View } from '../../src/edit/view/view';
import { ViewWithData } from '../../src/edit/view/view-with-data';

import { expectGolden } from '../setup/goldens';

describe.skip('View', () => {
  const view = ViewWithData.example();

  it('rowCount', () => {
    expect(view.rowCount).toBe(5);
  });

  it('columnCount', () => {
    expect(view.columnCount).toBe(7);
  });

  it('row', () => {
    expectGolden('view/row.json').toBe(view.row(0));
  });

  it('value', () => {
    expect(view.value(0, 0)).toBe('Zero');
    expect(view.value(1, 0)).toBe('OneA');
    expect(view.value(2, 0)).toBe('Two');
    expect(view.value(3, 0)).toBe('OneB');

    expect(view.value(0, 1)).toBe(0);
    expect(view.value(1, 1)).toBe(1);
    expect(view.value(2, 1)).toBe(2);
    expect(view.value(3, 1)).toBe(11);
  });

  describe('check', () => {
    it('throws when row cell count does not match column count', () => {
      let message = '';

      try {
        new ViewWithData(ColumnSelection.example(), [
          ['Three', 3], // Too less columns
          ['Four', 4],
        ]);
      } catch (e: any) {
        message = e.message;
      }

      expect(message).toBe(
        'Number of columns in data and in columnSelection do not match: ' +
          'Column count in "columnSelection" is "7" and in row "0" is "2".',
      );
    });
  });

  describe('rowHashes()', () => {
    it('are provided for each row', () => {
      expectGolden('view/row-hashes.json').toBe(view.rowHashes);
    });

    it('throws when called before the row hashes are calculated', () => {
      const view = ViewWithData.example();
      (view as any)._rowHashes = [];

      let message = '';
      try {
        // eslint-disable-next-line @typescript-eslint/no-unused-expressions
        view.rowHashes;
      } catch (e: any) {
        message = e.message;
      }

      expect(message).toBe(
        'Row hashes have not been calculated yet. ' +
          'Please make sure _rowHashes contains a valid hash for each row. ' +
          'Use _updateAllRowHashes() or write an optimized row hash calc method.',
      );
    });
  });

  describe('calcColumnTypes()', () => {
    it('returns an empty array when no rows are provided', () => {
      const view = ViewWithData.empty();
      expect(view.columnTypes).toEqual([]);
    });

    describe('with deep', () => {
      let columnSelection: ColumnSelection;
      const deep = true;
      let result: JsonValueType[];

      beforeEach(() => {
        columnSelection = ColumnSelection.example();
        const view = ViewWithData.example();
        result = View.calcColumnTypes(view.rows, deep);
      });
      describe('== true', () => {
        it('returns an empty array when the rows contains no cells', () => {
          const view = new ViewWithData(ColumnSelection.empty(), [[]]);
          expect(view.columnTypes).toEqual([]);
        });

        it('returns the types collected from the various rows', () => {
          expect(result).toEqual([
            'string',
            'number',
            'number',
            'boolean',
            'json',
            'jsonArray',
            'jsonValue',
          ]);
        });

        it('returns "jsonValue" if a column contains more then one type', () => {
          expect(result[6]).toBe('jsonValue');
        });

        it('returns JsonValue if a column contains only null values', () => {
          const view = new ViewWithData(columnSelection, [
            [null, null, null, null, null, null, null],
            [null, null, null, null, null, null, null],
            [null, null, null, null, null, null, null],
          ]);
          expect(View.calcColumnTypes(view.rows, deep)).toEqual([
            'jsonValue',
            'jsonValue',
            'jsonValue',
            'jsonValue',
            'jsonValue',
            'jsonValue',
            'jsonValue',
          ]);
        });
      });

      describe('== false', () => {
        it('breaks if at least one type is found for each column', () => {
          const view = ViewWithData.example();
          expect(View.calcColumnTypes(view.rows, !deep)).toEqual([
            'string',
            'number',
            'number',
            'boolean',
            'json',
            'jsonArray',
            'number',
          ]);
        });
      });
    });
  });

  describe('validateSelection', () => {
    it('does not throw when selection matches existing columns', () => {
      const view = ViewWithData.example();
      const selection = ColumnSelection.example();
      expect(() => view.validateSelection(selection)).not.toThrow();
    });

    describe('throws with correct mesage', () => {
      it('when selection contains one missing column', () => {
        const view = ViewWithData.example();
        // Create a selection with a non-existent column
        const selection = new ColumnSelection([
          {
            key: 'nonexistent',
            route: 'nonexistent/a/b',
            alias: 'nonexistent',
            type: 'string',
            titleShort: 'ne',
            titleLong: 'Non existent',
          },
        ]);
        let message: string[] = [];
        try {
          view.validateSelection(selection);
        } catch (e: any) {
          message = e.message.split('\n');
        }
        expect(message).toEqual([
          'Missing column(s) "nonexistent":',
          '',
          '  Missing:',
          '    - nonexistent/a/b',
          '',
          '  Available:',
          '    -basicTypes/stringsRef/value',
          '    -basicTypes/numbersRef/intsRef/value',
          '    -basicTypes/numbersRef/floatsRef/value',
          '    -basicTypes/booleansRef/value',
          '    -complexTypes/jsonObjectsRef/value',
          '    -complexTypes/jsonArraysRef/value',
          '    -complexTypes/jsonValuesRef/value',
        ]);
      });

      it('when selection contains two missing column', () => {
        const view = ViewWithData.example();
        // Create a selection with a non-existent column
        const selection = new ColumnSelection([
          {
            key: 'nonexistentA',
            route: 'nonexistent/a',
            alias: 'nonexistentA',
            type: 'string',
            titleShort: 'ne',
            titleLong: 'Non existent',
          },

          {
            key: 'nonexistentB',
            route: 'nonexistent/b',
            alias: 'nonexistentB',
            type: 'string',
            titleShort: 'ne',
            titleLong: 'Non existent',
          },
        ]);
        let message: string[] = [];
        try {
          view.validateSelection(selection);
        } catch (e: any) {
          message = e.message.split('\n');
        }
        expect(message).toEqual([
          'Missing column(s) "nonexistentA" and "nonexistentB":',
          '',
          '  Missing:',
          '    - nonexistent/a',
          '    - nonexistent/b',
          '',
          '  Available:',
          '    -basicTypes/stringsRef/value',
          '    -basicTypes/numbersRef/intsRef/value',
          '    -basicTypes/numbersRef/floatsRef/value',
          '    -basicTypes/booleansRef/value',
          '    -complexTypes/jsonObjectsRef/value',
          '    -complexTypes/jsonArraysRef/value',
          '    -complexTypes/jsonValuesRef/value',
        ]);
      });

      it('when selection contains three and more missing column', () => {
        const view = ViewWithData.example();
        // Create a selection with a non-existent column
        const selection = new ColumnSelection([
          {
            key: 'nonexistentA',
            route: 'nonexistent/a',
            alias: 'nonexistentA',
            type: 'string',
            titleShort: 'ne',
            titleLong: 'Non existent',
          },

          {
            key: 'nonexistentB',
            route: 'nonexistent/b',
            alias: 'nonexistentB',
            type: 'string',
            titleShort: 'ne',
            titleLong: 'Non existent',
          },
          {
            key: 'nonexistentC',
            route: 'nonexistent/c',
            alias: 'nonexistentC',
            type: 'string',
            titleShort: 'ne',
            titleLong: 'Non existent',
          },
        ]);
        let message: string[] = [];
        try {
          view.validateSelection(selection);
        } catch (e: any) {
          message = e.message.split('\n');
        }
        expect(message).toEqual([
          'Missing column(s) "nonexistentA", "nonexistentB", "nonexistentC":',
          '',
          '  Missing:',
          '    - nonexistent/a',
          '    - nonexistent/b',
          '    - nonexistent/c',
          '',
          '  Available:',
          '    -basicTypes/stringsRef/value',
          '    -basicTypes/numbersRef/intsRef/value',
          '    -basicTypes/numbersRef/floatsRef/value',
          '    -basicTypes/booleansRef/value',
          '    -complexTypes/jsonObjectsRef/value',
          '    -complexTypes/jsonArraysRef/value',
          '    -complexTypes/jsonValuesRef/value',
        ]);
      });
    });
  });
});
