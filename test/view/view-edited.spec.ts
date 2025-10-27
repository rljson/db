// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';

import { describe, expect, it } from 'vitest';

import {
  Edit,
  exampleEditSetAllEndingWithOTo1234,
  exampleEditSetAllStartingWithOToTrue,
  exampleEditSetAllTrueToDone,
} from '../../src/edit/edit/edit';
import { EditAction } from '../../src/edit/edit/edit-action';
import { exampleRowFilter, RowFilter } from '../../src/edit/filter/row-filter';
import { StringFilter } from '../../src/edit/filter/string-filter';
import { View } from '../../src/edit/view/view';
import { ViewEdited } from '../../src/edit/view/view-edited';
import { ViewWithData } from '../../src/edit/view/view-with-data';

describe.skip('ViewEdited', () => {
  describe('rows', () => {
    it('returns the edited rows of the original view', () => {
      const original = ViewWithData.example();
      const originalData = original.rows.map(View.exampleSelect);

      const edited = ViewEdited.example;
      const editedData = edited.rows.map(View.exampleSelect);

      // For reference show the original view data
      expect(originalData).toEqual([
        ['Zero', 0, false, 'todo'],
        ['OneA', 1, false, 'todo'],
        ['Two', 2, false, 'todo'],
        ['OneB', 11, false, 'todo'],
        ['True', 12, true, 'todo'],
      ]);

      // Check the edited view
      expect(edited.edit.name).toEqual(
        'Set bool to true  and done to true of all items that  ' +
          '1.) end with "o" and  2.) have an int column greater > 1.',
      );

      expect(editedData).toEqual([
        ['Zero', 0, false, 'todo'], // Ends with o is not greater 1
        ['OneA', 1, false, 'todo'],
        ['Two', 2, true, 'done'], // Ends with o and is > 1
        ['OneB', 11, false, 'todo'],
        ['True', 12, true, 'todo'],
      ]);
    });

    describe('returns the original view', () => {
      it('when the array of actions is empty', () => {
        const view = ViewWithData.example();
        const edit: Edit = hip({
          name: 'Example Step',
          filter: exampleRowFilter(),
          actions: [],
          _hash: '',
        });

        const viewEdited = new ViewEdited(view, edit);
        expect(viewEdited.rows).toBe(view.rows);
      });

      it('when filter does not apply to any row', () => {
        const startsWithXyz: StringFilter = hip({
          _hash: '',
          type: 'string',
          column: 'basicTypes/stringsRef/value',
          operator: 'startsWith',
          search: 'XYZ',
        });

        const filter: RowFilter = hip({
          columnFilters: [startsWithXyz],
          operator: 'and',
          _hash: '',
        });

        const action: EditAction = hip({
          route: 'basicTypes/numbersRef/intsRef/value',
          setValue: 500,
          _hash: '',
        });

        const view = ViewWithData.example();
        const edit: Edit = hip({
          _hash: '',
          name: 'Example Step',
          filter: filter,
          actions: [action],
        });
        const viewEdited = new ViewEdited(view, edit);
        expect(viewEdited.rows).toBe(view.rows);
      });
    });

    describe('throws an error', () => {
      it('when an action refers to a column that does not exist', () => {
        const view = ViewWithData.example();
        const edit: Edit = hip({
          _hash: '',
          name: 'Example Step',
          filter: exampleRowFilter(),
          actions: [
            {
              route: 'nonExistingColumn',
              setValue: 500,
            },
          ],
        });
        expect(() => new ViewEdited(view, edit)).toThrowError(
          'ViewEdited: Error while applying an Edit to a view: ' +
            'One of the edit actions refers to the column "nonExistingColumn" ' +
            ' that does not exist in the view. ' +
            'Please make sure that alle columns referred in any action ' +
            'are available in the view.',
        );
      });
    });

    describe('apply example edits', () => {
      it('exampleEditSetAllTrueToDone', () => {
        const view = ViewWithData.example();
        const edit = exampleEditSetAllTrueToDone();
        const viewEdited = new ViewEdited(view, edit);
        const editedData = viewEdited.rows.map(View.exampleSelect);

        expect(editedData).toEqual([
          ['Zero', 0, false, 'todo'],
          ['OneA', 1, false, 'todo'],
          ['Two', 2, false, 'todo'],
          ['OneB', 11, false, 'todo'],
          ['True', 12, true, 'done'],
        ]);
      });

      it('exampleEditSetAllStartingWithOToTrue', () => {
        const view = ViewWithData.example();
        const edit = exampleEditSetAllStartingWithOToTrue();
        const viewEdited = new ViewEdited(view, edit);
        const editedData = viewEdited.rows.map(View.exampleSelect);
        expect(editedData).toEqual([
          ['Zero', 0, false, 'todo'],
          ['OneA', 1, true, 'todo'], // Starts with O
          ['Two', 2, false, 'todo'],
          ['OneB', 11, true, 'todo'], // Starts with O
          ['True', 12, true, 'todo'],
        ]);
      });

      it('export const exampleEditSetAllEndingWithOTo1234', () => {
        const view = ViewWithData.example();
        const edit = exampleEditSetAllEndingWithOTo1234();
        const viewEdited = new ViewEdited(view, edit);
        const editedData = viewEdited.rows.map(View.exampleSelect);

        expect(editedData).toEqual([
          ['Zero', 1234, false, 'todo'], // Ends with o
          ['OneA', 1, false, 'todo'],
          ['Two', 1234, false, 'todo'], // Ends with o
          ['OneB', 11, false, 'todo'],
          ['True', 12, true, 'todo'],
        ]);
      });
    });

    describe('rowHashes', () => {
      it('are updated for the edited rows', () => {
        const original = ViewWithData.example();
        const edit = exampleEditSetAllTrueToDone();
        const edited = new ViewEdited(original, edit);

        let i = 0;
        for (const row of edited.rows) {
          const originalRow = original.rows[i];
          const editedRow = row;

          const originalDone = originalRow[4]['done'] ?? false;
          const editedDone = editedRow[4]['done'] ?? false;

          const originalHash = original.rowHashes[i];
          const editedHash = edited.rowHashes[i];

          if (originalDone !== editedDone) {
            expect(editedHash).not.toBe(originalHash);
          } else {
            expect(editedHash).toBe(originalHash);
          }

          i++;
        }
      });
    });
  });

  it('columnTypes', () => {
    expect(ViewEdited.example.columnTypes).toEqual([
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
