// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { IoMem } from '@rljson/io';
import { Route } from '@rljson/rljson';

import { beforeEach, describe, expect, it } from 'vitest';

import { carsExample } from '../src/cars-example';
import { Db } from '../src/db';
import { SetValue } from '../src/edit/join/set-value/set-value';
import { ColumnSelection } from '../src/edit/selection/column-selection';

describe('Join', () => {
  let db: Db;

  const cakeKey = 'carCake';
  const cakeRef = carsExample().carCake._data[0]._hash as string;

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
    for (const tableCfg of carsExample().tableCfgs._data) {
      await db.core.createTableWithHistory(tableCfg);
    }

    //Import Data
    await db.core.import(carsExample());
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
      expect(layerRoutes.length).toBe(3);
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
        carsExample().carSliceId._data.flatMap((s) => s.add),
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
        carsExample().carSliceId._data.flatMap((s) => s.add),
      );

      expect(Object.keys(data).length).toBe(sliceIds.size);
    });
  });
  describe('columnTypes', () => {
    it('should return the column types of Join', async () => {
      const join = await db.join(columnSelection, cakeKey, cakeRef);
      const colTypes = join.columnTypes;

      expect(Object.values(colTypes)).toEqual(
        columnSelection.columns.map((c) => c.type),
      );
    });
  });

  describe('columnSelection', () => {
    it('should return the column selection of Join', async () => {
      const join = await db.join(columnSelection, cakeKey, cakeRef);
      const initialColSelection = join.columnSelection;

      expect(initialColSelection).toBe(columnSelection);

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
        carsExample().carSliceId._data.flatMap((s) => s.add),
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

      const values = editedJoin.rows.flatMap((r) => r);
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

      const values = editedJoin.rows.flatMap((r) => r);
      const uniqueValues = Array.from(new Set(values)).sort();

      expect(uniqueValues).toEqual(['BMW']);
    });
  });

  describe('insert', () => {
    it('should insert a new row into the Join', async () => {
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
      const insert = await editedJoin.insert();

      const writtenCakeRef = insert['carCakeRef'] as string;
      const writtenData = await db.get(
        Route.fromFlat(
          `/${cakeKey}@${writtenCakeRef}/carGeneralLayer/carGeneral/brand`,
        ),
        {},
      );

      expect(writtenData['carGeneral']._data.length).toBe(4);
      const writtenDataSet = new Set(
        writtenData['carGeneral']._data.map((d: any) => d['brand']),
      );
      expect(writtenDataSet.has('Opel')).toBe(true);
      expect(writtenDataSet.size).toBe(1);
    });
  });
});
