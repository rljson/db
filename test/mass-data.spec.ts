// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Io, IoMem } from '@rljson/io';
import { Json } from '@rljson/json';
import { Ref, Rljson, Route, TableCfg } from '@rljson/rljson';

import { beforeAll, describe, expect, it } from 'vitest';

import { Db } from '../src/db';
import { convertMassData } from '../src/example-static/mass-data/convert-mass-data';
import {
  ColumnInfo,
  ColumnSelection,
} from '../src/join/selection/column-selection';

describe('mass data', () => {
  const dataLength = 102;

  let converted: {
    written: boolean;
    result: Rljson;
  };
  let io: Io;
  let db: Db;

  beforeAll(async () => {
    converted = convertMassData();

    io = new IoMem();
    await io.init();
    await io.isReady();

    db = new Db(io);

    const tableCfgs = converted.result.tableCfgs._data as Array<TableCfg>;
    for (const tableCfg of tableCfgs) {
      await db.core.createTable(tableCfg);
    }

    await db.core.import(converted.result);
  });

  it('should convert w/o error', async () => {
    expect(converted.written).toBe(true);
    expect(converted.result).toBeDefined();
  });

  it('should import result into db w/o errors', async () => {
    const cars = await db.get(Route.fromFlat('/carGeneral'), {});
    expect(cars.cell.length).toBe(dataLength);
  });

  it('should join on converted data', async () => {
    const columnSelection = new ColumnSelection([
      {
        key: 'brand',
        route: 'carCake/carGeneralLayer/carGeneral/brand',
        alias: 'brand',
        titleLong: 'Car Brand',
        titleShort: 'Brand',
        type: 'string',
        _hash: '',
      } as ColumnInfo,
      {
        key: 'type',
        route: 'carCake/carGeneralLayer/carGeneral/type',
        alias: 'type',
        titleLong: 'Car Type',
        titleShort: 'Type',
        type: 'string',
        _hash: '',
      } as ColumnInfo,
      {
        key: 'isElectric',
        route: 'carCake/carGeneralLayer/carGeneral/isElectric',
        alias: 'isElectric',
        titleLong: 'Is Electric Car',
        titleShort: 'Electric',
        type: 'boolean',
        _hash: '',
      } as ColumnInfo,
      {
        key: 'carHeight',
        route: 'carCake/carDimensionsLayer/carDimensions/carHeight/height',
        alias: 'height',
        titleLong: 'Car Height',
        titleShort: 'Height',
        type: 'number',
        _hash: '',
      } as ColumnInfo,
      {
        key: 'carWidth',
        route: 'carCake/carDimensionsLayer/carDimensions/carWidth/width',
        alias: 'width',
        titleLong: 'Car Width',
        titleShort: 'Width',
        type: 'number',
        _hash: '',
      } as ColumnInfo,
      {
        key: 'carLength',
        route: 'carCake/carDimensionsLayer/carDimensions/carLength/length',
        alias: 'length',
        titleLong: 'Car Length',
        titleShort: 'Length',
        type: 'number',
        _hash: '',
      } as ColumnInfo,
      {
        key: 'engine',
        route: 'carCake/carTechnicalLayer/carTechnical/engine',
        alias: 'engine',
        titleLong: 'Car Engine',
        titleShort: 'Engine',
        type: 'string',
        _hash: '',
      } as ColumnInfo,
      {
        key: 'transmission',
        route: 'carCake/carTechnicalLayer/carTechnical/transmission',
        alias: 'transmission',
        titleLong: 'Car Transmission',
        titleShort: 'Transmission',
        type: 'string',
        _hash: '',
      } as ColumnInfo,
      {
        key: 'gears',
        route: 'carCake/carTechnicalLayer/carTechnical/gears',
        alias: 'gears',
        titleLong: 'Car Gears',
        titleShort: 'Gears',
        type: 'number',
        _hash: '',
      } as ColumnInfo,
      {
        key: 'sides',
        route: 'carCake/carColorLayer/carColor/sides',
        alias: 'sides',
        titleLong: 'Car Sides Color',
        titleShort: 'Sides Color',
        type: 'string',
        _hash: '',
      } as ColumnInfo,
      {
        key: 'roof',
        route: 'carCake/carColorLayer/carColor/roof',
        alias: 'roof',
        titleLong: 'Car Roof Color',
        titleShort: 'Roof Color',
        type: 'string',
        _hash: '',
      } as ColumnInfo,
      {
        key: 'highlights',
        route: 'carCake/carColorLayer/carColor/highlights',
        alias: 'highlights',
        titleLong: 'Car Highlights Color',
        titleShort: 'Highlights Color',
        type: 'string',
        _hash: '',
      } as ColumnInfo,
    ]);

    const cakeKey = 'carCake';
    const cakeGet = await db.get(Route.fromFlat(`/${cakeKey}`), {});
    const cakeRef = (cakeGet.cell[0].row! as Json)._hash as Ref;

    const startTime = performance.now();
    const join = await db.join(columnSelection, cakeKey, cakeRef);
    const endTime = performance.now();
    console.log(`Join took ${endTime - startTime}ms`);

    expect(join.rows.length).toBe(dataLength);
  });
});
