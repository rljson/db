// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';

import { BehaviorSubject, firstValueFrom } from 'rxjs';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { RowFilter } from '../../src/edit/filter/row-filter';
import { StringFilter } from '../../src/edit/filter/string-filter';
import { ColumnSelection } from '../../src/edit/selection/column-selection';
import { RowSort } from '../../src/edit/sort/row-sort';
import { View } from '../../src/edit/view/view';
import { ViewProcessed } from '../../src/edit/view/view-processed';
import { ViewWithData } from '../../src/edit/view/view-with-data';

describe.skip('ViewProcessed', () => {
  let viewProcessed: ViewProcessed;

  // Input data
  let masterView: BehaviorSubject<View>;
  let columnSelection: BehaviorSubject<ColumnSelection>;
  let filter: BehaviorSubject<RowFilter>;
  let sort: BehaviorSubject<RowSort>;
  let errors: any[] = [];

  beforeEach(() => {
    errors = [];
    viewProcessed = ViewProcessed.example((error): void => {
      errors.push(error);
    });
    masterView = viewProcessed.masterView$ as BehaviorSubject<View>;
    columnSelection =
      viewProcessed.columnSelection$ as BehaviorSubject<ColumnSelection>;
    filter = viewProcessed.filter$ as BehaviorSubject<RowFilter>;
    sort = viewProcessed.sort$ as BehaviorSubject<RowSort>;
  });

  describe('dispose()', () => {
    it('calls onDispose', () => {
      let disposedView: ViewProcessed | null = null;

      const onDispose = (viewProcessed: ViewProcessed) => {
        disposedView = viewProcessed;
      };

      const viewProcessed1 = new ViewProcessed(
        viewProcessed.masterView$,
        viewProcessed.columnSelection$,
        viewProcessed.filter$,
        viewProcessed.sort$,
        onDispose,
      );

      viewProcessed1.dispose();
      expect(disposedView).toBe(viewProcessed1);
    });
  });

  describe('view', () => {
    it('is updated when master view, column selection, filter or sort change', async () => {
      // Initially no filter and sort is applied, and all columns are selected
      let view = await firstValueFrom(viewProcessed.view);
      expect(view.rows).toEqual([
        [
          'Zero',
          0,
          0.01,
          false,
          {
            _hash: '7nnKrH6KoTEC3e9xxliBiP',
            a: {
              _hash: '9Iao-QHCqda6NiJKtRDGx4',
              b: 0,
            },
          },
          [0, 1, [2, 3]],
          0,
        ],
        [
          'OneA',
          1,
          1.01,
          false,
          {
            _hash: 'aGyCrR_fCrzMa6oP_6N50z',
            a: {
              _hash: '647TzLUCMJO1b0kKRlAeiN',
              b: 1,
            },
          },
          [1, 2, [3, 4]],
          'OneA',
        ],
        [
          'Two',
          2,
          2.02,
          false,
          {
            _hash: 'fryoUHLYrdlumjdozOJR0G',
            a: {
              _hash: 'CrGm05TNMBlfBkK2euEYDD',
              b: 2,
            },
          },
          [2, 3, [4, 5]],
          {
            _hash: 'foBZ9JVYn82YEjLMEdALAN',
            a: 2,
          },
        ],
        [
          'OneB',
          11,
          11.01,
          false,
          {
            _hash: 'ap4-YEnJA9ZqfJEu1Ma2Am',
            a: {
              _hash: 'y-ZJyvmx49eS3YoRFL9nMG',
              b: 11,
            },
          },
          [1, 2, [3, 4]],
          'OneB',
        ],

        [
          'True',
          12,
          12.1,
          true,
          {
            _hash: 'ap4-YEnJA9ZqfJEu1Ma2Am',
            a: {
              _hash: 'y-ZJyvmx49eS3YoRFL9nMG',
              b: 11,
            },
          },
          [1, 2, [3, 4]],
          'True',
        ],
      ]);

      // Select columns
      columnSelection.next(
        new ColumnSelection([
          {
            key: 'stringCol',
            alias: 'stringCol',
            route: 'basicTypes/stringsRef/value',
            type: 'string',
            titleLong: 'String values',
            titleShort: 'Strings',
          },
          {
            key: 'intCol',
            alias: 'intCol',
            route: 'basicTypes/numbersRef/intsRef/value',
            type: 'number',
            titleLong: 'Int values',
            titleShort: 'Ints',
          },
        ]),
      );

      view = await firstValueFrom(viewProcessed.view);
      expect(view.rows).toEqual([
        ['Zero', 0],
        ['OneA', 1],
        ['Two', 2],
        ['OneB', 11],
        ['True', 12],
      ]);

      // Filter rows
      const stringFilter: StringFilter = hip({
        type: 'string',
        operator: 'startsWith',
        search: 'O',
        _hash: '',
        column: 'basicTypes/stringsRef/value',
      });

      const rowFilter: RowFilter = hip({
        columnFilters: [stringFilter],
        operator: 'and',
        _hash: '',
      });

      filter.next(rowFilter);

      view = await firstValueFrom(viewProcessed.view);
      expect(view.rows).toEqual([
        ['OneA', 1],
        ['OneB', 11],
      ]);

      // Sort rows
      sort.next(new RowSort({ 'basicTypes/stringsRef/value': 'asc' }));

      view = await firstValueFrom(viewProcessed.view);
      expect(view.rows).toEqual([
        ['OneA', 1],
        ['OneB', 11],
      ]);

      // Change master view data
      masterView.next(
        new ViewWithData(masterView.value.columnSelection, [
          ['Zero', 0, 0.01, false, { a: { b: 0 } }, [0, 1, [2, 3]], 0],
          ['OneA', 1, 1.01, true, { a: { b: 1 } }, [1, 2, [3, 4]], 'OneA'],
          ['Two', 2, 2.02, false, { a: { b: 2 } }, [2, 3, [4, 5]], { a: 2 }],
          ['OneB', 11, 11.01, true, { a: { b: 11 } }, [1, 2, [3, 4]], 'OneB'],
          ['OneD', 13, 13.01, true, { a: { b: 11 } }, [1, 2, [3, 4]], 'OneD'],
          ['OneC', 12, 12.01, true, { a: { b: 11 } }, [1, 2, [3, 4]], 'OneC'],
        ]),
      );

      // View will still output sorted data
      view = await firstValueFrom(viewProcessed.view);

      expect(view.rows).toEqual([
        ['OneA', 1],
        ['OneB', 11],
        ['OneC', 12],
        ['OneD', 13],
      ]);
    });

    it(
      'is not updated as long not all selected columns ' +
        'are available in the master view',
      async () => {
        // Get the current view
        const view = await firstValueFrom(viewProcessed.view);
        expect(view.columnSelection.routes).toEqual([
          'basicTypes/stringsRef/value',
          'basicTypes/numbersRef/intsRef/value',
          'basicTypes/numbersRef/floatsRef/value',
          'basicTypes/booleansRef/value',
          'complexTypes/jsonObjectsRef/value',
          'complexTypes/jsonArraysRef/value',
          'complexTypes/jsonValuesRef/value',
        ]);

        // Now add another column to the views column selection
        // that is not yet available in the master view

        columnSelection.next(
          new ColumnSelection([
            ...columnSelection.value.columns,
            {
              key: 'ac',
              alias: 'ac',
              route: 'additionalCol',
              type: 'string',
              titleLong: 'Additional Column',
              titleShort: 'Add',
            },
          ]),
        );

        // Listen to the change in order to trigger the pipeline
        firstValueFrom(viewProcessed.view).then(() => {});

        // Wait a bit to make sure the view is not updated
        await new Promise((resolve) => setTimeout(resolve, 1));

        // No error happens because with _hasMissingColumns we make sure
        // the chain is not processed as long as not all selected columns are
        // available in the master view
        expect(errors.length).toBe(0);

        const newMasterView = new ViewWithData(columnSelection.value, [
          [
            'Additional',
            0,
            0.01,
            false,
            { a: { b: 0 } },
            [0, 1, [2, 3]],
            0,
            'additional',
          ],
        ] as any[]);

        masterView.next(newMasterView);

        const newView = await firstValueFrom(viewProcessed.view);
        expect(newView.columnSelection.routes).toEqual([
          'basicTypes/stringsRef/value',
          'basicTypes/numbersRef/intsRef/value',
          'basicTypes/numbersRef/floatsRef/value',
          'basicTypes/booleansRef/value',
          'complexTypes/jsonObjectsRef/value',
          'complexTypes/jsonArraysRef/value',
          'complexTypes/jsonValuesRef/value',
          'additionalCol',
        ]);

        expect(newView.rows).toEqual([
          [
            'Additional',
            0,
            0.01,
            false,
            {
              _hash: '7nnKrH6KoTEC3e9xxliBiP',
              a: {
                _hash: '9Iao-QHCqda6NiJKtRDGx4',
                b: 0,
              },
            },
            [0, 1, [2, 3]],
            0,
            'additional',
          ],
        ]);
      },
    );

    it('reports an error when a missing column is not available after 300ms', async () => {
      vi.useFakeTimers();

      // Select a column that is not available in the master view
      columnSelection.next(
        new ColumnSelection([
          ...columnSelection.value.columns,
          {
            key: 'ac',
            alias: 'ac',
            route: 'additionalCol',
            type: 'string',
            titleLong: 'Additional Column',
            titleShort: 'Add',
          },
        ]),
      );

      firstValueFrom(viewProcessed.view).then(() => {});

      // After 250ms no error is reported
      vi.advanceTimersByTime(250);
      expect(errors).toEqual([]);

      // After 300ms an error is reported
      vi.advanceTimersByTime(51);
      expect(errors).toEqual([
        'Warning: Could not apply column selection to master view: ' +
          'The following columns are missing: additionalCol',
      ]);
    });
  });

  describe('complete coverage', () => {
    it('with different error handler configurations', () => {
      const viewProcessed = ViewProcessed.example();
      expect(viewProcessed).toBeInstanceOf(ViewProcessed);
      viewProcessed.errorHandler('error');
    });
  });
});
