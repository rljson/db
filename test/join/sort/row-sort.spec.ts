// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Route } from '@rljson/rljson';

import { describe, expect, it } from 'vitest';

import { Container } from '../../../src/db';
import { Join, JoinColumn, JoinRows } from '../../../src/join/join';
import { ColumnSelection } from '../../../src/join/selection/column-selection';
import { RowSort } from '../../../src/join/sort/row-sort';

describe('RowSort', () => {
  const columnSelection = new ColumnSelection([
    {
      key: 'name',
      type: 'string',
      alias: 'name',
      route: 'person/name',
      titleShort: 'Name',
      titleLong: 'Name',
    },
    {
      key: 'age',
      type: 'number',
      alias: 'age',
      route: 'person/age',
      titleShort: 'Age',
      titleLong: 'Age',
    },
  ]);

  describe('applyTo(rows, columnAliases)', () => {
    describe('throws', () => {
      it('when sort specifies non existent column routes', () => {
        // Arrange
        const rows = [[1, 2, 3]];

        const joinRows: JoinRows = rows
          .map((r, i) => ({
            ['row' + i]: r.map((v, colIndex) => ({
              value: v,
              route: columnSelection.columns[colIndex]
                ? columnSelection.columns[colIndex].route
                : null,
              insert: null,
            })) as any[],
          }))
          .reduce((acc, curr) => ({ ...acc, ...curr }), {});

        const sort = new RowSort({ 'unknown/route': 'asc' });

        const join = new Join(
          joinRows,
          new ColumnSelection([
            ...columnSelection.columns,
            {
              key: 'weight',
              type: 'number',
              alias: 'weight',
              route: 'person/weight',
              titleShort: 'Weight',
              titleLong: 'Weight',
            },
          ]),
        );

        expect(() => sort.applyTo(join)).toThrow(
          'RowFilterProcessor: Error while applying sort to join: ' +
            'There is a sort entry for route "unknown/route", but the join does ' +
            'not have a column with this route.' +
            '\n\n' +
            'Available routes:\n' +
            '- person/name\n' +
            '- person/age\n' +
            '- person/weight',
        );
      });
    });

    describe('returns the original rows', () => {
      it('when no rows are provided', () => {
        // Arrange
        const rows = {} as JoinRows;
        const sort = new RowSort({ 'person/name': 'asc' });
        const join = new Join(rows, columnSelection);

        // Act
        const result = sort.applyTo(join);

        // Assert
        expect(result).toEqual(join.rows);
      });

      it('when the sort contains no columns', () => {
        // Arrange
        const rows = [['Alice', 25]];

        const joinRows: JoinRows = rows
          .map((r, i) => ({
            ['row' + i]: r.map((v, colIdx) => ({
              value: { cell: [{ value: [v] }] } as Container,
              route: Route.fromFlat(columnSelection.columns[colIdx].route),
              inserts: null,
            })) as JoinColumn[],
          }))
          .reduce((acc, curr) => ({ ...acc, ...curr }), {});

        const sort = new RowSort({});
        const join = new Join(joinRows, columnSelection);

        // Act
        const result = sort.applyTo(join);

        // Assert
        expect(result).toEqual(join.rowIndices);
      });
    });

    describe('returns the sorted rows', () => {
      it('when the rows are sorted by one column', () => {
        // Arrange
        const rows = [
          ['Charlie', 20],
          ['Bob', 30],
          ['Alice', 25],
        ];

        const joinRows: JoinRows = rows
          .map((r, i) => ({
            ['row' + i]: r.map((v, colIdx) => ({
              value: { cell: [{ value: [v] }] } as Container,
              route: Route.fromFlat(columnSelection.columns[colIdx].route),
              inserts: null,
            })) as JoinColumn[],
          }))
          .reduce((acc, curr) => ({ ...acc, ...curr }), {});

        const sort = new RowSort({ 'person/name': 'asc' });
        const join = new Join(joinRows, columnSelection);

        // Act
        const result = sort.applyTo(join);

        // Assert
        expect(result).toEqual(['row2', 'row1', 'row0']);
      });

      it('when the rows are sorted by multiple columns', () => {
        // Arrange
        const rows = [
          ['Charlie', 20],
          ['Alice', 25],
          ['Bob', 30],
          ['Alice', 30],
        ];

        const joinRows: JoinRows = rows
          .map((r, i) => ({
            ['row' + i]: r.map((v, colIdx) => ({
              value: { cell: [{ value: [v] }] } as Container,
              route: Route.fromFlat(columnSelection.columns[colIdx].route),
              inserts: null,
            })) as JoinColumn[],
          }))
          .reduce((acc, curr) => ({ ...acc, ...curr }), {});

        const sort = new RowSort({
          'person/name': 'asc',
          'person/age': 'desc',
        });

        const join = new Join(joinRows, columnSelection);

        // Act
        const result = sort
          .applyTo(join)
          .map((index) =>
            join.row(index).flatMap((c) => c.value.cell[0].value),
          );

        // Assert
        expect(result).toEqual([
          ['Alice', 30],
          ['Alice', 25],
          ['Bob', 30],
          ['Charlie', 20],
        ]);
      });
    });

    describe('special cases', () => {
      it('when all rows contain the same data', () => {
        // Arrange
        const rows = [
          ['Alice', 25],
          ['Alice', 25],
          ['Alice', 25],
        ];

        const joinRows: JoinRows = rows
          .map((r, i) => ({
            ['row' + i]: r.map((v, colIdx) => ({
              value: { cell: [{ value: [v] }] } as Container,
              route: Route.fromFlat(columnSelection.columns[colIdx].route),
              inserts: null,
            })) as JoinColumn[],
          }))
          .reduce((acc, curr) => ({ ...acc, ...curr }), {});

        const sort = new RowSort({ 'person/name': 'asc' });

        const join = new Join(joinRows, columnSelection);

        // Act
        const result = sort
          .applyTo(join)
          .map((index) =>
            join.row(index).flatMap((c) => c.value.cell[0].value),
          );

        // Assert
        expect(result).toEqual([
          ['Alice', 25],
          ['Alice', 25],
          ['Alice', 25],
        ]);
      });

      describe('when all data are already sorted in ascending order', () => {
        // Arrange
        const rows = [
          ['Alice', 20],
          ['Bob', 25],
          ['Charlie', 30],
        ];

        const joinRows: JoinRows = rows
          .map((r, i) => ({
            ['row' + i]: r.map((v, colIdx) => ({
              value: { cell: [{ value: [v] }] } as Container,
              route: Route.fromFlat(columnSelection.columns[colIdx].route),
              inserts: null,
            })) as JoinColumn[],
          }))
          .reduce((acc, curr) => ({ ...acc, ...curr }), {});

        const join = new Join(joinRows, columnSelection);

        it('and the sort is ascending', () => {
          const sort = new RowSort({
            'person/name': 'asc',
            'person/age': 'asc',
          });

          const result = sort
            .applyTo(join)
            .map((index) =>
              join.row(index).flatMap((c) => c.value.cell[0].value),
            );

          expect(result).toEqual([
            ['Alice', 20],
            ['Bob', 25],
            ['Charlie', 30],
          ]);
        });

        it('and the sort is descending', () => {
          const sort = new RowSort({
            'person/name': 'desc',
            'person/age': 'desc',
          });
          const result = sort
            .applyTo(join)
            .map((index) =>
              join.row(index).flatMap((c) => c.value.cell[0].value),
            );

          // Assert
          expect(result).toEqual([
            ['Charlie', 30],
            ['Bob', 25],
            ['Alice', 20],
          ]);
        });
      });

      describe('when all data are already sorted in descending order', () => {
        const rows = [
          ['Charlie', 30],
          ['Bob', 25],
          ['Alice', 20],
        ];

        const joinRows: JoinRows = rows
          .map((r, i) => ({
            ['row' + i]: r.map((v, colIdx) => ({
              value: { cell: [{ value: [v] }] } as Container,
              route: Route.fromFlat(columnSelection.columns[colIdx].route),
              inserts: null,
            })) as JoinColumn[],
          }))
          .reduce((acc, curr) => ({ ...acc, ...curr }), {});

        const join = new Join(joinRows, columnSelection);

        it('and the sort is ascending', () => {
          const sort = new RowSort({
            'person/name': 'asc',
            'person/age': 'asc',
          });
          const result = sort
            .applyTo(join)
            .map((index) =>
              join.row(index).flatMap((c) => c.value.cell[0].value),
            );

          expect(result).toEqual([
            ['Alice', 20],
            ['Bob', 25],
            ['Charlie', 30],
          ]);
        });

        it('and the sort is descending', () => {
          const sort = new RowSort({
            'person/name': 'desc',
            'person/age': 'desc',
          });
          const result = sort
            .applyTo(join)
            .map((index) =>
              join.row(index).flatMap((c) => c.value.cell[0].value),
            );

          // Assert
          expect(result).toEqual([
            ['Charlie', 30],
            ['Bob', 25],
            ['Alice', 20],
          ]);
        });
      });

      it('when the sort columns are in different orders', () => {
        const rows = [
          ['Alice', 20],
          ['Charlie', 15],
          ['Charlie Old', 85],
          ['Bob', 25],
        ];

        const joinRows: JoinRows = rows
          .map((r, i) => ({
            ['row' + i]: r.map((v, colIdx) => ({
              value: { cell: [{ value: [v] }] } as Container,
              route: Route.fromFlat(columnSelection.columns[colIdx].route),
              inserts: null,
            })) as JoinColumn[],
          }))
          .reduce((acc, curr) => ({ ...acc, ...curr }), {});

        const join = new Join(joinRows, columnSelection);

        // Sort by age first and then by name
        const sort2 = new RowSort({
          'person/age': 'desc',
          'person/name': 'asc',
        });
        const result2 = sort2
          .applyTo(join)
          .map((index) =>
            join.row(index).flatMap((c) => c.value.cell[0].value),
          );

        expect(result2).toEqual([
          ['Charlie Old', 85],
          ['Bob', 25],
          ['Alice', 20],
          ['Charlie', 15],
        ]);

        // Sort by name first and then by age
        const sort = new RowSort({
          'person/name': 'asc',
          'person/age': 'desc',
        });
        const result = sort
          .applyTo(join)
          .map((index) =>
            join.row(index).flatMap((c) => c.value.cell[0].value),
          );

        expect(result).toEqual([
          ['Alice', 20],
          ['Bob', 25],
          ['Charlie', 15],
          ['Charlie Old', 85],
        ]);
      });
    });
  });
});
