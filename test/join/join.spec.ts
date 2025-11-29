// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { IoMem } from '@rljson/io';
import { Route } from '@rljson/rljson';

import { beforeEach, describe, expect, it } from 'vitest';

import { Db } from '../../src/db';
import { staticExample } from '../../src/example-static/example-static';
import { RowFilter } from '../../src/join/filter/row-filter';
import { ColumnSelection } from '../../src/join/selection/column-selection';
import { SetValue } from '../../src/join/set-value/set-value';
import { RowSort } from '../../src/join/sort/row-sort';

describe('Join', () => {
  let db: Db;

  const cakeKey = 'carCake';
  const cakeRef = staticExample().carCake._data[2]._hash as string;

  const columnSelection: ColumnSelection =
    ColumnSelection.exampleCarsColumnSelection();

  beforeEach(async () => {
    //Init io
    const io = new IoMem();
    await io.init();
    await io.isReady();

    //Init Core
    db = new Db(io);

    //Create Tables for TableCfgs in carsExample
    for (const tableCfg of staticExample().tableCfgs._data) {
      await db.core.createTableWithInsertHistory(tableCfg);
    }

    //Import Data
    await db.core.import(staticExample());
  });

  describe('Constructor', () => {
    it('should be defined', async () => {
      const join = await db.join(columnSelection, cakeKey, cakeRef);
      expect(join).toBeDefined();
    });
  });
  describe('componentRoutes', () => {
    it('should return all component routes of Join', async () => {
      const join = await db.join(columnSelection, cakeKey, cakeRef);

      const componentRoutes = join.componentRoutes;
      expect(componentRoutes.length).toBe(3);
    });
  });
  describe('layerRoutes', () => {
    it('should return all layer routes of Join', async () => {
      const join = await db.join(columnSelection, cakeKey, cakeRef);

      const layerRoutes = join.layerRoutes;
      expect(layerRoutes.length).toBe(2);
    });
  });
  describe('cakeRoute', () => {
    it('should return the cake route of Join', async () => {
      const join = await db.join(columnSelection, cakeKey, cakeRef);

      const cakeRoute = join.cakeRoute;
      expect(cakeRoute.flat).toBe(`/${cakeKey}`);
    });
  });
  describe('rowCount', () => {
    it('should return the row count of Join', async () => {
      const join = await db.join(columnSelection, cakeKey, cakeRef);
      const rowCount = join.rowCount;

      const sliceIds = new Set(
        staticExample().carSliceId._data.flatMap((s) => s.add),
      );

      expect(rowCount).toBe(sliceIds.size);
    });
  });
  describe('columnCount', () => {
    it('should return the column count of Join', async () => {
      const join = await db.join(columnSelection, cakeKey, cakeRef);
      const columnCount = join.columnCount;

      expect(columnCount).toBe(columnSelection.columns.length);
    });
  });
  describe('data', () => {
    it('should return the data of Join', async () => {
      const join = await db.join(columnSelection, cakeKey, cakeRef);
      const data = join.data;

      const sliceIds = new Set(
        staticExample().carSliceId._data.flatMap((s) => s.add),
      );

      expect(Object.keys(data).length).toBe(sliceIds.size);
    });
  });
  describe('columnTypes', () => {
    it('should return the column types of Join', async () => {
      const join = await db.join(columnSelection, cakeKey, cakeRef);
      const colTypes = join.columnTypes.sort();

      expect(Object.values(colTypes)).toEqual(
        columnSelection.columns.map((c) => c.type).sort(),
      );
    });
  });

  describe('columnSelection', () => {
    it('should return the column selection of Join', async () => {
      const join = await db.join(columnSelection, cakeKey, cakeRef);
      const initialColSelection = join.columnSelection;

      expect(initialColSelection.columns.map((c) => c.key).sort()).toEqual(
        columnSelection.columns.map((c) => c.key).sort(),
      );

      // Edit the column selection and verify that it has changed
      join.select(new ColumnSelection(columnSelection.columns.slice(0, 2)));
      const editedColSelection = join.columnSelection;

      expect(editedColSelection).not.toBe(initialColSelection);
      expect(editedColSelection.columns).toEqual(
        columnSelection.columns.slice(0, 2),
      );
    });
  });

  describe('rows', () => {
    it('should return the rows of Join', async () => {
      const join = await db.join(columnSelection, cakeKey, cakeRef);
      const rows = join.rows;

      const sliceIds = new Set(
        staticExample().carSliceId._data.flatMap((s) => s.add),
      );

      expect(rows.length).toBe(sliceIds.size);
    });
  });

  describe('setValue', () => {
    it('should set values in the Join', async () => {
      const brandColumnSelection = new ColumnSelection([
        columnSelection.columns[0],
      ]);

      // Initial join for CarGeneral -> Brand
      const join = await db.join(brandColumnSelection, cakeKey, cakeRef);
      const setValue: SetValue = {
        route: '/carCake/carGeneralLayer/carGeneral/brand',
        value: 'Opel',
      };
      const editedJoin = join.setValue(setValue);

      const values = editedJoin.rows.flatMap((r) => r).flat();
      const uniqueValues = Array.from(new Set(values));

      expect(uniqueValues).toEqual(['Opel']);
    });
  });
  describe('setValues', () => {
    it('should set multiple values in the Join', async () => {
      const brandColumnSelection = new ColumnSelection([
        columnSelection.columns[0],
      ]);

      // Initial join for CarGeneral -> Brand
      const join = await db.join(brandColumnSelection, cakeKey, cakeRef);
      const setValues: SetValue[] = [
        {
          route: '/carCake/carGeneralLayer/carGeneral/brand',
          value: 'Opel',
        },
        {
          route: '/carCake/carGeneralLayer/carGeneral/brand',
          value: 'BMW',
        },
      ];
      const editedJoin = join.setValues(setValues);

      const values = editedJoin.rows.flatMap((r) => r).flat();
      const uniqueValues = Array.from(new Set(values)).sort();

      expect(uniqueValues).toEqual(['BMW']);
    });
  });

  describe('filter', () => {
    it('should filter the Join rows', async () => {
      const brandColumnSelection = new ColumnSelection([
        columnSelection.columns[0],
      ]);

      // Initial join for CarGeneral -> Brand
      const join = await db.join(brandColumnSelection, cakeKey, cakeRef);
      const filter: RowFilter = {
        columnFilters: [
          {
            column: 'carCake/carGeneralLayer/carGeneral/brand',
            type: 'string',
            operator: 'equals',
            _hash: '',
            search: 'Audi',
          },
        ],
        operator: 'and',
        _hash: '',
      };

      const filtered = join.filter(filter);
      const filteredResult = Array.from(
        new Set(filtered.rows.flatMap((r) => r)),
      );
      expect(filteredResult.length).toBe(2);
      expect(filteredResult).toEqual([['Audi'], ['Audi']]);
    });
  });

  describe('select', () => {
    it('should select a new columnSelection of join', async () => {
      const brandColumnSelection = new ColumnSelection([
        columnSelection.columns[0],
      ]);

      const join = await db.join(columnSelection, cakeKey, cakeRef);

      const selected = join.select(brandColumnSelection);
      const selectedResult = Array.from(
        new Set(selected.rows.flatMap((r) => r)),
      );

      expect(selectedResult.length).toBe(12);
      expect(selectedResult.flat()).toEqual([
        'Volkswagen',
        'Volkswagen',
        'Audi',
        'Audi',
        'BMW',
        'BMW',
        'Tesla',
        'Tesla',
        'Ford',
        'Chevrolet',
        'Nissan',
        'Hyundai',
      ]);
    });
  });

  describe('sort', () => {
    it('should sort a join', async () => {
      const join = await db.join(columnSelection, cakeKey, cakeRef);

      const rowSort = new RowSort({
        'carCake/carGeneralLayer/carGeneral/brand': 'desc',
      });

      const sorted = join.sort(rowSort);
      const sortedResult = sorted.rows.map((r) => r[0]);

      const expected = [
        'Volkswagen',
        'Volkswagen',
        'Tesla',
        'Tesla',
        'Nissan',
        'Hyundai',
        'Ford',
        'Chevrolet',
        'BMW',
        'BMW',
        'Audi',
        'Audi',
      ];
      expect(sortedResult.flat()).toEqual(expected);
    });
  });

  describe('insert', () => {
    it('should insert a new row into rljson data model', async () => {
      const brandColumnSelection = new ColumnSelection([
        columnSelection.columns[0],
      ]);

      // Initial join for CarGeneral -> Brand
      const join = await db.join(brandColumnSelection, cakeKey, cakeRef);
      const setValue: SetValue = {
        route: '/carCake/carGeneralLayer/carGeneral/brand',
        value: 'Opel',
      };

      const inserts = await join.setValue(setValue).insert();
      const insert = inserts[0];
      const inserteds = await db.insert(insert.route, insert.tree, {
        skipHistory: true,
      });
      const inserted = inserteds[0];

      const writtenCakeRef = inserted['carCakeRef'] as string;
      const { rljson: writtenData } = await db.get(
        Route.fromFlat(
          `/${cakeKey}@${writtenCakeRef}/carGeneralLayer/carGeneral/brand`,
        ),
        {},
      );

      expect(writtenData['carGeneral']._data.length).toBe(1);
      const writtenDataSet = new Set(
        writtenData['carGeneral']._data.map((d: any) => d['brand']),
      );
      expect(writtenDataSet.has('Opel')).toBe(true);
      expect(writtenDataSet.size).toBe(1);
    });
  });
});
