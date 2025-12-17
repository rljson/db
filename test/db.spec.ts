// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { rmhsh } from '@rljson/hash';
import { IoMem } from '@rljson/io';
import { Json, JsonValue } from '@rljson/json';
import {
  Cake,
  InsertHistoryRow,
  InsertHistoryTable,
  Layer,
  LayersTable,
  Route,
  SliceIdsTable,
} from '@rljson/rljson';

import { beforeEach, describe, expect, it, vi } from 'vitest';

import { Container, Db } from '../src/db';
import {
  CarGeneral,
  staticExample,
} from '../src/example-static/example-static';
import {
  ColumnInfo,
  ColumnSelection,
} from '../src/join/selection/column-selection';
import { inject } from '../src/tools/inject';
import { isolate } from '../src/tools/isolate';

describe('Db', () => {
  let db: Db;

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

  describe('core', () => {
    it('should be defined', () => {
      expect(db.core).toBeDefined();
    });
  });

  describe('clone', () => {
    it('should clone a db to a given Io', () => {
      db.setCache(
        new Map([
          [
            'test',
            {
              rljson: {
                test: { _data: [{ _type: 'test', _hash: '' }] },
              } as Json,
            } as Container,
          ],
        ]),
      );
      const io2 = new IoMem();
      const db2 = db.clone(io2);

      expect(db2).toBeDefined();
      expect(db2).not.toBe(db);
      expect(db2.cache.size).toBe(1);
      expect(db2.cache.get('test')).toEqual({
        rljson: {
          test: { _data: [{ _type: 'test', _hash: '' }] },
        } as Json,
      });
    });
  });

  describe('getController', () => {
    it('throws on invalid tableKey', async () => {
      //Empty string
      await expect(db.getController('non-existing')).rejects.toThrow(
        'Db.getController: Table non-existing does not exist.',
      );
    });
  });

  describe('getInsertHistory', () => {
    it('throws on invalid tableKey', async () => {
      await expect(db.getInsertHistory('non-existing')).rejects.toThrow(
        'Db.getInsertHistory: Table non-existing does not exist',
      );
    });

    it('returns InsertHistory for valid tableKey', async () => {
      const insertHistory = await db.getInsertHistory('carGeneral');
      expect(insertHistory).toBeDefined();
    });

    it('returns InsertHistory with options', async () => {
      const {
        ['carGeneralInsertHistory']: { _data: inserts0 },
      } = await db.getInsertHistory('carGeneral', {
        sorted: true,
        ascending: true,
      });

      expect(inserts0.length).toBe(0);

      const route = Route.fromFlat(`carGeneral`);

      const { tree, cell } = await db.get(route, {});

      const paths = cell.flatMap((c) => c.path);
      expect(paths.length).toBe(12);

      const path = paths[0];

      const component1 = {
        brand: 'Porsche',
        type: 'Macan Electric',
        doors: 4,
        energyConsumption: 2.1,
        units: {
          energy: 'kWh/100km',
          _hash: '',
        },
        serviceIntervals: [20000, 40000, 60000],
        isElectric: false,
        meta: {
          pressText: 'A sporty electric SUV.',
          _hash: '',
        },
        _type: 'components',
        _hash: '',
      } as CarGeneral;

      const insert1 = isolate(tree, path);
      inject(insert1, path, component1);

      await db.insert(route, insert1);

      const {
        ['carGeneralInsertHistory']: { _data: inserts1 },
      } = await db.getInsertHistory('carGeneral', {
        sorted: true,
        ascending: true,
      });

      expect(inserts1.length).toBe(1);

      const component2 = {
        brand: 'Porsche',
        type: 'Macan Electric',
        doors: 4,
        energyConsumption: 2.1,
        units: {
          energy: 'kWh/100km',
          _hash: '',
        },
        serviceIntervals: [20000, 40000, 60000],
        isElectric: false,
        meta: {
          pressText: 'A sporty electric SUV.',
          _hash: '',
        },
        _type: 'components',
        _hash: '',
      } as CarGeneral;

      const insert2 = isolate(tree, path);
      inject(insert2, path, component2);

      await db.insert(route, insert2);

      const {
        ['carGeneralInsertHistory']: { _data: inserts2Asc },
      } = await db.getInsertHistory('carGeneral', {
        sorted: true,
        ascending: true,
      });

      const timeIdsAsc = (inserts2Asc as InsertHistoryRow<'CarGeneral'>[]).map(
        (i) => i.timeId.split(':')[0],
      );
      expect(timeIdsAsc[0] < timeIdsAsc[1]).toBe(true);

      const {
        ['carGeneralInsertHistory']: { _data: inserts2Desc },
      } = await db.getInsertHistory('carGeneral', {
        sorted: true,
        ascending: false,
      });

      const timeIdsDesc = (
        inserts2Desc as InsertHistoryRow<'CarGeneral'>[]
      ).map((i) => i.timeId.split(':')[0]);
      expect(timeIdsDesc[0] > timeIdsDesc[1]).toBe(true);
    });
  });

  describe('getInsertHistoryRowsByRef(table,ref)', () => {
    it('returns InsertHistory rows for given ref', async () => {
      const route = Route.fromFlat(`carGeneral`);

      const { tree, cell } = await db.get(route, {});

      const paths = cell.flatMap((c) => c.path);
      expect(paths.length).toBe(12);

      const path = paths[0];

      const component = {
        brand: 'Porsche',
        type: 'Macan Electric',
        doors: 4,
        energyConsumption: 2.1,
        units: {
          energy: 'kWh/100km',
          _hash: '',
        },
        serviceIntervals: [20000, 40000, 60000],
        isElectric: false,
        meta: {
          pressText: 'A sporty electric SUV.',
          _hash: '',
        },
        _type: 'components',
        _hash: '',
      } as CarGeneral;

      const insert = isolate(tree, path);
      inject(insert, path, component);

      const results = (await db.insert(
        route,
        insert,
      )) as InsertHistoryRow<'CarGeneral'>[];

      const result = results[0];

      const insertHistoryRowsByRef = await db.getInsertHistoryRowsByRef(
        'carGeneral',
        result.carGeneralRef as string,
      );

      expect(insertHistoryRowsByRef!.length).toBe(1);
    });
  });

  describe('getInsertHistoryRowByTimeId(table,timeId)', () => {
    it('returns InsertHistory row for given timeId', async () => {
      const route = Route.fromFlat(`carGeneral`);

      const { tree, cell } = await db.get(route, {});

      const paths = cell.flatMap((c) => c.path);
      expect(paths.length).toBe(12);

      const path = paths[0];

      const component = {
        brand: 'Porsche',
        type: 'Macan Electric',
        doors: 4,
        energyConsumption: 2.1,
        units: {
          energy: 'kWh/100km',
          _hash: '',
        },
        serviceIntervals: [20000, 40000, 60000],
        isElectric: false,
        meta: {
          pressText: 'A sporty electric SUV.',
          _hash: '',
        },
        _type: 'components',
        _hash: '',
      } as CarGeneral;

      const insert = isolate(tree, path);
      inject(insert, path, component);

      const results = (await db.insert(
        route,
        insert,
      )) as InsertHistoryRow<'CarGeneral'>[];

      const result = results[0];

      const insertHistoryRowByTimeId = await db.getInsertHistoryRowByTimeId(
        'carGeneral',
        result.timeId,
      );

      expect(insertHistoryRowByTimeId).toBeDefined();

      expect(insertHistoryRowByTimeId.timeId).toBe(result.timeId);
    });
  });

  describe('getTimeIdsForRef(table, ref)', () => {
    it('returns TimeIds for given ref', async () => {
      const route = Route.fromFlat(`carGeneral`);

      const { tree, cell } = await db.get(route, {});

      const paths = cell.flatMap((c) => c.path);
      expect(paths.length).toBe(12);

      const path = paths[0];

      const component = {
        brand: 'Porsche',
        type: 'Macan Electric',
        doors: 4,
        energyConsumption: 2.1,
        units: {
          energy: 'kWh/100km',
          _hash: '',
        },
        serviceIntervals: [20000, 40000, 60000],
        isElectric: false,
        meta: {
          pressText: 'A sporty electric SUV.',
          _hash: '',
        },
        _type: 'components',
        _hash: '',
      } as CarGeneral;

      const insert = isolate(tree, path);
      inject(insert, path, component);

      const results = (await db.insert(
        route,
        insert,
      )) as InsertHistoryRow<'CarGeneral'>[];

      const result = results[0];

      const timeIds = await db.getTimeIdsForRef(
        'carGeneral',
        result.carGeneralRef as string,
      );
      expect(timeIds.length).toBe(1);

      const nonExistingTimeIds = await db.getTimeIdsForRef(
        'carGeneral',
        'NONEXISTINGREF',
      );
      expect(nonExistingTimeIds.length).toBe(0);
    });
  });

  describe('isolatePropertyKeyFromRoute(route)', () => {
    it('returns property key for given route', async () => {
      const nonProcessedPropertyRoute = Route.fromFlat('/carGeneral/brand');
      expect(nonProcessedPropertyRoute.hasPropertyKey).toBe(false);

      const isolatedWithProperty = await db.isolatePropertyKeyFromRoute(
        nonProcessedPropertyRoute,
      );
      expect(isolatedWithProperty.hasPropertyKey).toBe(true);

      const nonProcessedComponentRoute = Route.fromFlat('/carGeneral');
      expect(nonProcessedComponentRoute.hasPropertyKey).toBe(false);

      const isolatedWithoutProperty = await db.isolatePropertyKeyFromRoute(
        nonProcessedComponentRoute,
      );
      expect(isolatedWithoutProperty.hasPropertyKey).toBe(false);
    });
  });

  describe('isolatePropertyFromComponents(rljson, propertyKey)', () => {
    it('returns rljson with isolated property for given propertyKey', async () => {
      const propertyKey = 'brand';
      const { rljson } = await db.get(Route.fromFlat('/carGeneral'), {});

      const isolated = await db.isolatePropertyFromComponents(
        rljson,
        propertyKey,
      );
      expect(isolated.carGeneral).toBeDefined();
      expect(
        Array.from(new Set(isolated.carGeneral._data.map((c) => c.brand))),
      ).toEqual([
        'Volkswagen',
        'Hyundai',
        'Nissan',
        'Tesla',
        'Audi',
        'Ford',
        'BMW',
        'Chevrolet',
      ]);

      const nonExistingPropertyKey = 'nonExistingProperty';
      const isolatedNonExisting = await db.isolatePropertyFromComponents(
        rljson,
        nonExistingPropertyKey,
      );

      expect(isolatedNonExisting.carGeneral).toBeDefined();
      expect(isolatedNonExisting.carGeneral._data).toEqual(
        rljson.carGeneral._data,
      );
    });
  });

  describe('getRefOfTimeId(table, timeId)', () => {
    it('returns ref for given timeId', async () => {
      const route = Route.fromFlat(`carGeneral`);

      const { tree, cell } = await db.get(route, {});

      const paths = cell.flatMap((c) => c.path);
      expect(paths.length).toBe(12);

      const path = paths[0];

      const component = {
        brand: 'Porsche',
        type: 'Macan Electric',
        doors: 4,
        energyConsumption: 2.1,
        units: {
          energy: 'kWh/100km',
          _hash: '',
        },
        serviceIntervals: [20000, 40000, 60000],
        isElectric: false,
        meta: {
          pressText: 'A sporty electric SUV.',
          _hash: '',
        },
        _type: 'components',
        _hash: '',
      } as CarGeneral;

      const insert = isolate(tree, path);
      inject(insert, path, component);

      const results = (await db.insert(
        route,
        insert,
      )) as InsertHistoryRow<'CarGeneral'>[];

      const result = results[0];

      const ref = await db.getRefOfTimeId('carGeneral', result.timeId);

      expect(ref).toBe(result.carGeneralRef);
    });
  });

  describe('get', () => {
    it('throws on invalid route', async () => {
      await expect(db.get(new Route([]), {})).rejects.toThrow(
        'Route  is not valid.',
      );
    });
    it('get sliceId of cake w/ chained sliceIds definition', async () => {
      const cakeKey = 'carCake';
      const sliceIds = ['VIN1', 'VIN2'];
      const route = `/${cakeKey}(${sliceIds.join(',')})`;

      const {
        rljson: {
          [cakeKey]: { _data: results },
        },
      } = await db.get(Route.fromFlat(route), {});

      expect(results.length).toBe(3);

      expect(results.map((r) => r._hash).sort()).toEqual(
        staticExample()
          .carCake._data.map((c) => c._hash)
          .sort(),
      );
    });
    it('get sliceId of cake w/ single sliceIds definition', async () => {
      const cakeKey = 'carCake';
      const sliceIds = ['VIN11'];
      const route = `/${cakeKey}(${sliceIds.join(',')})`;

      const {
        rljson: {
          [cakeKey]: { _data: results },
        },
      } = await db.get(Route.fromFlat(route), {});

      expect(results.length).toBe(1);

      expect(results.map((r) => r._hash)).toEqual([
        staticExample().carCake._data[2]._hash,
      ]);
    });
    it('get sliceId of layer/component', async () => {
      const layerKey = 'carGeneralLayer';
      const componentKey = 'carGeneral';
      const sliceIds = ['VIN11'];
      const route = `/${layerKey}(${sliceIds.join(',')})/${componentKey}`;

      const { rljson: result } = await db.get(Route.fromFlat(route), {});

      const layers = result[layerKey]._data;
      const components = result[componentKey]._data;

      expect(layers.length).toBe(1);
      expect(components.length).toBe(1);
    });
    it('get sliceId of cake/layer/component', async () => {
      const cakeKey = 'carCake';
      const sliceIds = ['VIN11'];
      const route = `/${cakeKey}(${sliceIds.join(
        ',',
      )})/carGeneralLayer/carGeneral`;

      const { rljson: result } = await db.get(Route.fromFlat(route), {});

      const cakes = result[cakeKey]._data;
      const layers = result['carGeneralLayer']._data;
      const components = result['carGeneral']._data;

      expect(cakes.length).toBe(1);
      expect(layers.length).toBe(1);
      expect(components.length).toBe(1);
    });
    it('get sliceId w/ ref of cake/layer/component', async () => {
      const cakeKey = 'carCake';
      const cakeRef = staticExample().carCake._data[2]._hash ?? '';
      const sliceIds = ['VIN11'];
      const route = `/${cakeKey}(${sliceIds.join(
        ',',
      )})@${cakeRef}/carGeneralLayer/carGeneral`;

      const { rljson: result } = await db.get(Route.fromFlat(route), {});

      const cakes = result[cakeKey]._data;
      const layers = result['carGeneralLayer']._data;
      const components = result['carGeneral']._data;

      expect(cakes.length).toBe(1);
      expect(layers.length).toBe(1);
      expect(components.length).toBe(1);
    });
    it('get empty for sliceId w/ unmatching ref of cake/layer/component', async () => {
      const cakeKey = 'carCake';
      const cakeRef = staticExample().carCake._data[0]._hash ?? '';
      const sliceIds = ['VIN12'];
      const route = `/${cakeKey}(${sliceIds.join(
        ',',
      )})@${cakeRef}/carGeneralLayer/carGeneral`;

      const { rljson: result } = await db.get(Route.fromFlat(route), {});

      const cakes = result[cakeKey];
      const layers = result['carGeneralLayer'];
      const components = result['carGeneral'];

      expect(!cakes || cakes._data.length == 0).toBe(true);
      expect(!layers).toBe(true);
      expect(!components).toBe(true);
    });
    it('get component by ref', async () => {
      const route = '/carGeneral';
      const ref = staticExample().carGeneral._data[0]._hash ?? '';

      const { rljson: result } = await db.get(Route.fromFlat(route), ref);
      expect(result).toBeDefined();
      expect(result.carGeneral).toBeDefined();
      expect(result.carGeneral._data.length).toBe(1);
      expect(result.carGeneral._data[0]._hash).toBe(ref);
    });
    it('get component by where from Cache', async () => {
      const route = '/carCake/carGeneralLayer/carGeneral';
      const where = {
        carGeneralLayer: {
          carGeneral: { brand: 'Volkswagen' } as Partial<CarGeneral>,
        },
      };

      const { rljson: firstGet } = await db.get(Route.fromFlat(route), where);

      const cache = db.cache;
      expect(cache.size).toBe(7);
      expect(firstGet).toBe(Array.from(cache.values()).map((v) => v.rljson)[6]);

      const { rljson: secondGet } = await db.get(Route.fromFlat(route), where);
      expect(secondGet).toEqual(firstGet);
      expect(cache.size).toBe(7);

      //Reset cache
      db.setCache(new Map());
      expect(db.cache.size).toBe(0);
    });

    it('get component property by ref', async () => {
      const componentKey = 'carGeneral';
      const propertyKey = 'brand';
      const route = `${componentKey}/${propertyKey}`;

      const ref = staticExample().carGeneral._data[0]._hash ?? '';

      const { rljson: result } = await db.get(Route.fromFlat(route), ref);

      expect(result).toBeDefined();
      expect(result[componentKey]).toBeDefined();
      expect(result[componentKey]._data.length).toBe(1);
      expect(result[componentKey]._data[0]).toEqual({
        _hash: ref,
        [propertyKey]: staticExample().carGeneral._data[0][propertyKey],
      });
      expect(result[componentKey]._data[0][propertyKey]).toBe(
        staticExample().carGeneral._data[0][propertyKey],
      );
    });
    it('get component by where', async () => {
      const route = '/carGeneral';
      const where = rmhsh(staticExample().carGeneral._data[0]) as {
        [column: string]: JsonValue;
      };

      const { rljson: result } = await db.get(Route.fromFlat(route), where);
      expect(result).toBeDefined();
      expect(result.carGeneral).toBeDefined();
      expect(result.carGeneral._data.length).toBe(1);
      expect(result.carGeneral._data[0]._hash).toBe(
        staticExample().carGeneral._data[0]._hash,
      );
    });
    it('get components by where', async () => {
      const route = '/carGeneral';
      const where = { brand: 'Volkswagen' } as Partial<CarGeneral>;

      const { rljson: result } = await db.get(Route.fromFlat(route), where);
      expect(result).toBeDefined();
      expect(result.carGeneral).toBeDefined();
      expect(result.carGeneral._data.length).toBe(2); //2 Volkswagens in example data
      expect(result.carGeneral._data[0].brand).toBe('Volkswagen');
      expect(result.carGeneral._data[1].brand).toBe('Volkswagen');
    });
    it('get layer by ref', async () => {
      const route = '/carGeneralLayer';
      const ref =
        (staticExample().carGeneralLayer._data[0]._hash as string) ?? '';

      const { rljson: result } = await db.get(Route.fromFlat(route), ref);
      expect(result).toBeDefined();
      expect(result.carGeneralLayer).toBeDefined();
      expect(result.carGeneralLayer._data.length).toBe(1);
      expect(result.carGeneralLayer._data[0]._hash).toBe(ref);
    });
    it('get cake by route ref', async () => {
      const route = '/carCake';
      const ref = (staticExample().carCake._data[0]._hash as string) ?? '';

      const { rljson: result } = await db.get(
        Route.fromFlat(`${route}@${ref}`),
        {},
      );
      expect(result).toBeDefined();
      expect(result.carCake).toBeDefined();
      expect(result.carCake._data.length).toBe(1);
      expect(result.carCake._data[0]._hash).toBe(ref);
    });
    it('get cake/layer by route ref', async () => {
      const cakeRoute = '/carCake';
      const cakeRef = (staticExample().carCake._data[0]._hash as string) ?? '';

      const { rljson: result } = await db.get(
        Route.fromFlat(`${cakeRoute}@${cakeRef}/carGeneralLayer`),
        {},
      );
      expect(result).toBeDefined();
      expect(result.carCake).toBeDefined();
      expect(result.carCake._data.length).toBe(1);
      expect(result.carCake._data[0]._hash).toBe(cakeRef);
    });
    it('get nested layer/component by where', async () => {
      const route = '/carGeneralLayer/carGeneral';
      const where = {
        carGeneral: rmhsh(staticExample().carGeneral._data[0]) as {
          [column: string]: JsonValue;
        },
      };

      const { rljson: result } = await db.get(Route.fromFlat(route), where);
      expect(result).toBeDefined();
      expect(result.carGeneralLayer).toBeDefined();
      expect(result.carGeneralLayer._data.length).toBe(3); //3 layers because they are chained
      expect(result.carGeneralLayer._data.map((l) => l._hash).sort()).toEqual(
        staticExample()
          .carGeneralLayer._data.map((l) => l._hash)
          .sort(),
      );
      expect(result.carGeneral).toBeDefined();
      expect(result.carGeneral._data.length).toBe(1);
      expect(result.carGeneral._data[0]._hash).toBe(
        staticExample().carGeneral._data[0]._hash,
      );
    });
    it('get nested layer/component by where w/ revision', async () => {
      //Add Layer with switching VIN -> CarGeneral relation
      const route = Route.fromFlat('/carGeneralLayer');
      const { tree, cell } = await db.get(route, {});
      const layer: Layer = {
        base: staticExample().carGeneralLayer._data[2]._hash as string,
        add: {
          VIN1: staticExample().carGeneral._data[1]._hash || '',
          VIN2: staticExample().carGeneral._data[0]._hash || '',
        },
        componentsTable: 'carGeneral',
        sliceIdsTable: 'carSliceId',
        sliceIdsTableRow: staticExample().carSliceId._data[2]._hash as string,
      } as Layer;

      expect(cell.length).toBe(3);
      expect(cell[0].path.length).toBe(1);

      const path = cell[0].path[0];

      inject(tree, path, layer);
      const insert = isolate(tree, path);

      const addedLayerInsertHistorys = (await db.insert(
        route,
        insert,
      )) as InsertHistoryRow<'CarGeneralLayer'>[];

      expect(addedLayerInsertHistorys.length).toBe(1);
      const addedLayerInsertHistory = addedLayerInsertHistorys[0];

      const where = {
        carGeneral: rmhsh(staticExample().carGeneral._data[0]) as {
          [column: string]: JsonValue;
        },
      };

      //GET Result via first layer revision
      const layerRevHash1 =
        staticExample().carGeneralLayer._data[0]._hash ?? '';
      const route1 = `/carGeneralLayer@${layerRevHash1}/carGeneral`;

      //GET Result via second layer revision
      const layerRevHash2 = addedLayerInsertHistory.carGeneralLayerRef ?? '';
      const route2 = `/carGeneralLayer@${layerRevHash2}/carGeneral`;

      const { rljson: result1 } = await db.get(Route.fromFlat(route1), where);

      const { rljson: result2 } = await db.get(Route.fromFlat(route2), where);

      expect(result1).toBeDefined();
      expect(result2).toBeDefined();

      expect(result1.carGeneral).toBeDefined();
      expect(result1.carGeneral._data.length).toBe(1);
      expect(result1.carGeneral._data[0]._hash).toBe(
        staticExample().carGeneral._data[0]._hash,
      );

      expect(result2.carGeneral).toBeDefined();
      expect(result2.carGeneral._data.length).toBe(1);
      expect(result2.carGeneral._data[0]._hash).toBe(
        staticExample().carGeneral._data[0]._hash,
      );

      expect(result1.carGeneralLayer).toBeDefined();
      expect(result1.carGeneralLayer._data.length).toBe(1);
      expect(result1.carGeneralLayer._data[0]._hash).toBe(layerRevHash1);

      expect(result2.carGeneralLayer).toBeDefined();
      expect(result2.carGeneralLayer._data.length).toBe(1);
      expect(result2.carGeneralLayer._data[0]._hash).toBe(layerRevHash2);
    });
    it('get nested component/component by where', async () => {
      const route = '/carTechnical/carDimensions';
      const where = {
        carDimensions: {
          ...(rmhsh(staticExample().carDimensions._data[0]) as {
            [column: string]: JsonValue;
          }),
          ...{ _through: 'dimensions' },
        },
      };

      const { rljson: result } = await db.get(Route.fromFlat(route), where);

      expect(result).toBeDefined();
      expect(result.carTechnical).toBeDefined();
      expect(result.carTechnical._data.length).toBe(1);
      expect(result.carTechnical._data[0]._hash).toBe(
        staticExample().carTechnical._data[0]._hash,
      );
      expect(result.carDimensions).toBeDefined();
      expect(result.carDimensions._data.length).toBe(1);
      expect(result.carDimensions._data[0]._hash).toBe(
        staticExample().carDimensions._data[0]._hash,
      );
    });

    it('get any nested cake/layer/component/component/property by route', async () => {
      const route =
        '/carCake/carTechnicalLayer/carTechnical/carDimensions/length';

      const sliceId = 'VIN1';
      const cakeRef = staticExample().carCake._data[0]._hash ?? '';
      const where = cakeRef as string;

      const result = await db.get(Route.fromFlat(route), where, undefined, [
        sliceId,
      ]);

      expect(result).toBeDefined();
    });

    it('get any nested cake/layer/component', async () => {
      const route = '/carCake/carGeneralLayer/carGeneral';
      const where = {};

      const { rljson: result } = await db.get(Route.fromFlat(route), where);
      expect(result).toBeDefined();
      expect(result.carCake).toBeDefined();
      expect(result.carCake._data.length).toBe(
        staticExample().carCake._data.length,
      );

      expect(result.carGeneralLayer).toBeDefined();
      expect(result.carGeneralLayer._data.length).toBe(
        staticExample().carGeneralLayer._data.length,
      );

      expect(result.carGeneral).toBeDefined();
      expect(result.carGeneral._data.length).toBe(
        staticExample().carGeneral._data.length,
      );
    });
    it('get nested cake/layer/component by where', async () => {
      const route = '/carCake/carGeneralLayer/carGeneral';
      const where = {
        carGeneralLayer: {
          carGeneral: rmhsh(staticExample().carGeneral._data[0]) as {
            [column: string]: JsonValue;
          },
        },
      };

      const { rljson: result } = await db.get(Route.fromFlat(route), where);
      expect(result).toBeDefined();
      expect(result.carCake).toBeDefined();
      expect(result.carCake._data.length).toBe(3); //3 cakes because layers are chained
      expect(result.carCake._data[0]._hash).toBe(
        staticExample().carCake._data[0]._hash,
      );
      expect(result.carGeneralLayer).toBeDefined();
      expect(result.carGeneralLayer._data.length).toBe(3);
      expect(result.carGeneralLayer._data.map((l) => l._hash).sort()).toEqual(
        staticExample()
          .carGeneralLayer._data.map((l) => l._hash)
          .sort(),
      );
      expect(result.carGeneral).toBeDefined();
      expect(result.carGeneral._data.length).toBe(1);
      expect(result.carGeneral._data[0]._hash).toBe(
        staticExample().carGeneral._data[0]._hash,
      );
    });
    it('get nested cake/layer/component by hash w/ route revision hash', async () => {
      const cakeRevisionHash = staticExample().carCake._data[0]._hash ?? '';
      const layerRevisionHash =
        staticExample().carGeneralLayer._data[0]._hash ?? '';

      const route = `/carCake@${cakeRevisionHash}/carGeneralLayer@${layerRevisionHash}/carGeneral`;

      const where = {
        carGeneralLayer: {
          carGeneral: {
            _hash: staticExample().carGeneral._data[0]._hash ?? '',
          },
        },
      };
      const { rljson: result } = await db.get(Route.fromFlat(route), where);

      expect(result).toBeDefined();
      expect(result.carCake).toBeDefined();
      expect(result.carCake._data.length).toBe(1);
      expect(result.carCake._data[0]._hash).toBe(cakeRevisionHash);

      expect(result.carGeneralLayer).toBeDefined();
      expect(result.carGeneralLayer._data.length).toBe(1);
      expect(result.carGeneralLayer._data[0]._hash).toBe(layerRevisionHash);

      expect(result.carGeneral).toBeDefined();
      expect(result.carGeneral._data.length).toBe(1);
      expect(result.carGeneral._data[0]._hash).toBe(
        staticExample().carGeneral._data[0]._hash,
      );
    });
    it('get nested cake/layer/component by hash w/ route revision TimeId', async () => {
      const route = Route.fromFlat('/carCake/carGeneralLayer');
      const { tree, cell } = await db.get(route, {});
      const layer: Layer = {
        add: {
          VIN1: staticExample().carGeneral._data[1]._hash || '',
          VIN2: staticExample().carGeneral._data[0]._hash || '',
        },
        componentsTable: 'carGeneral',
        sliceIdsTable: 'carSliceId',
        sliceIdsTableRow: staticExample().carSliceId._data[2]._hash as string,
      } as Layer;

      const paths = cell.flatMap((c) => c.path);
      expect(paths.length).toBe(3);

      const path = paths[0];

      const insert = isolate(tree, path);
      inject(insert, path, layer);

      const cakeInsertHistoryRows = (await db.insert(
        route,
        insert,
      )) as InsertHistoryRow<'CarCake'>[];

      const cakeInsertHistoryRow = cakeInsertHistoryRows[0];
      const cakeRevisionTimeId = cakeInsertHistoryRow.timeId;

      //Get layer revision TimeId
      const {
        ['carGeneralLayerInsertHistory']: { _data: layerInsertHistoryRows },
      } = await db.getInsertHistory('carGeneralLayer');
      const layerRevisionTimeId = layerInsertHistoryRows[0].timeId;

      //Build route with TimeIds
      const refRoute = `/carCake@${cakeRevisionTimeId}/carGeneralLayer@${layerRevisionTimeId}/carGeneral`;

      //Get all Volkswagens in example data
      const where = {
        carCake: {
          carGeneralLayer: {
            carGeneral: { brand: 'Volkswagen' } as Partial<CarGeneral>,
          },
        },
      };

      const { rljson: result } = await db.get(Route.fromFlat(refRoute), where);

      expect(result).toBeDefined();
      expect(result.carCake).toBeDefined();
      expect(result.carCake._data.length).toBe(1);
      expect(result.carCake._data[0]._hash).toBe(
        cakeInsertHistoryRow.carCakeRef,
      );

      expect(result.carGeneralLayer).toBeDefined();
      expect(result.carGeneralLayer._data.length).toBe(1);

      expect(result.carGeneral).toBeDefined();
      expect(result.carGeneral._data.length).toBe(2);
      expect(result.carGeneral._data[0].brand).toBe('Volkswagen');
      expect(result.carGeneral._data[1].brand).toBe('Volkswagen');
    });

    it('get related cakes by component/cake sliceId reference', async () => {
      const cakeKey = 'seriesCake';
      const cakeRef = staticExample().seriesCake._data[2]._hash ?? '';

      const route = `/${cakeKey}@${cakeRef}/seriesCarsLayer/seriesCars/carCake/carGeneralLayer/carGeneral/brand`;

      const { rljson: result } = await db.get(Route.fromFlat(route), {});

      const seriesCakes = result[cakeKey]._data;
      expect(seriesCakes.length).toBe(1);

      const seriesCarsLayers = result['seriesCarsLayer']._data;
      expect(seriesCarsLayers.length).toBe(1);

      const seriesCars = result['seriesCars']._data;
      expect(seriesCars.length).toBe(staticExample().seriesCars._data.length);

      const carCakes = result['carCake']._data;
      expect(carCakes.length).toBe(staticExample().carCake._data.length);

      const carGeneralLayers = result['carGeneralLayer']._data;
      expect(carGeneralLayers.length).toBe(
        staticExample().carGeneralLayer._data.length,
      );

      const carGeneralComponents = result['carGeneral']._data;
      expect(carGeneralComponents.length).toBe(
        staticExample().carGeneral._data.length,
      );
    });

    it('get single related component/cake by sliceId reference by explicit sliceId', async () => {
      const cakeKey = 'seriesCake';
      const cakeRef = staticExample().seriesCake._data[2]._hash ?? '';
      const sliceIds = ['Serie7'];

      const route = `/${cakeKey}(${sliceIds.join(
        ',',
      )})@${cakeRef}/seriesCarsLayer/seriesCars/carCake/carGeneralLayer/carGeneral/brand`;

      const { rljson: result } = await db.get(Route.fromFlat(route), {});

      const seriesCakes = result[cakeKey]._data;
      expect(seriesCakes.length).toBe(1);

      const seriesCarsLayers = result['seriesCarsLayer']._data;
      expect(seriesCarsLayers.length).toBe(1);
      expect(seriesCarsLayers.map((l) => l._hash)).toEqual([
        staticExample().seriesCarsLayer._data[2]._hash,
      ]);

      const seriesCars = result['seriesCars']._data;
      expect(seriesCars.length).toBe(1);
      expect(seriesCars.map((c) => c._hash)).toEqual([
        staticExample().seriesCars._data[7]._hash,
      ]);

      const carCakes = result['carCake']._data;
      expect(carCakes.length).toBe(1);
      expect(carCakes.map((c) => c._hash)).toEqual([
        staticExample().carCake._data[2]._hash,
      ]);

      const carGeneralLayers = result['carGeneralLayer']._data;
      expect(carGeneralLayers.length).toBe(1);
      expect(carGeneralLayers.map((l) => l._hash)).toEqual([
        staticExample().carGeneralLayer._data[2]._hash,
      ]);

      const carGeneralComponents = result['carGeneral']._data;
      expect(carGeneralComponents.length).toBe(1);
      expect(carGeneralComponents.map((c) => c._hash)).toEqual([
        staticExample().carGeneral._data[7]._hash,
      ]);
    });

    it('get related cakes by component/cake sliceId reference', async () => {
      const cakeKey = 'catalogCake';
      const cakeRef = staticExample().catalogCake._data[0]._hash ?? '';

      const route = `/${cakeKey}@${cakeRef}/catalogSeriesLayer/catalogSeries/seriesCake/seriesCarsLayer/seriesCars/carCake/carGeneralLayer/carGeneral/brand`;

      const { rljson: result } = await db.get(Route.fromFlat(route), {});

      //Single starting point cake
      const catalogCakes = result[cakeKey]._data;
      expect(catalogCakes.length).toBe(1);

      //Single catalogSeries layer related to starting cake
      const catalogSeriesLayers = result['catalogSeriesLayer']._data;
      expect(catalogSeriesLayers.length).toBe(1);
      expect(catalogSeriesLayers.map((l) => l._hash)).toEqual([
        staticExample().catalogSeriesLayer._data[0]._hash,
      ]);

      //All catalogSeries, due to layer is linking any of them
      const catalogSeries = result['catalogSeries']._data;
      expect(catalogSeries.length).toBe(
        staticExample().catalogSeries._data.length,
      );

      //All seriesCakes, due to catalogSeries are linking any of them
      const seriesCakes = result['seriesCake']._data;
      expect(seriesCakes.length).toBe(staticExample().seriesCake._data.length);
      expect(seriesCakes.map((c) => c._hash).sort()).toEqual(
        staticExample()
          .seriesCake._data.map((c) => c._hash)
          .sort(),
      );

      //All seriesCarsLayers, due to seriesCakes are linking any of them
      const seriesCarsLayers = result['seriesCarsLayer']._data;
      expect(seriesCarsLayers.length).toBe(
        staticExample().seriesCarsLayer._data.length,
      );
      expect(seriesCarsLayers.map((l) => l._hash).sort()).toEqual(
        staticExample()
          .seriesCarsLayer._data.map((l) => l._hash)
          .sort(),
      );

      //All seriesCars, due to seriesCarsLayers are linking any of them
      const seriesCars = result['seriesCars']._data;
      expect(seriesCars.length).toBe(staticExample().seriesCars._data.length);
      expect(seriesCars.map((c) => c._hash).sort()).toEqual(
        staticExample()
          .seriesCars._data.map((c) => c._hash)
          .sort(),
      );

      //All carCakes, due to seriesCars are linking any of them
      const carCakes = result['carCake']._data;
      expect(carCakes.length).toBe(staticExample().carCake._data.length);
      expect(carCakes.map((c) => c._hash).sort()).toEqual(
        staticExample()
          .carCake._data.map((c) => c._hash)
          .sort(),
      );

      //All carGeneralLayers, due to carCakes are linking any of them
      const carGeneralLayers = result['carGeneralLayer']._data;
      expect(carGeneralLayers.length).toBe(
        staticExample().carGeneralLayer._data.length,
      );
      expect(carGeneralLayers.map((l) => l._hash).sort()).toEqual(
        staticExample()
          .carGeneralLayer._data.map((l) => l._hash)
          .sort(),
      );

      //All carGeneralComponents, due to carGeneralLayers are linking any of them
      const carGeneralComponents = result['carGeneral']._data;
      expect(carGeneralComponents.length).toBe(
        staticExample().carGeneral._data.length,
      );
      expect(carGeneralComponents.map((c) => c._hash).sort()).toEqual(
        staticExample()
          .carGeneral._data.map((c) => c._hash)
          .sort(),
      );
    });
    it('get related cakes by component/cake sliceId reference, filter on deeper sliceId', async () => {
      const cakeKey = 'catalogCake';
      const cakeRef = staticExample().catalogCake._data[0]._hash ?? '';
      const filterSliceIds = ['VIN4', 'VIN8'];

      const route = `/${cakeKey}@${cakeRef}/catalogSeriesLayer/catalogSeries/seriesCake/seriesCarsLayer/seriesCars/carCake(${filterSliceIds.join(
        ',',
      )})/carGeneralLayer/carGeneral/brand`;

      const { rljson: result } = await db.get(Route.fromFlat(route), {});

      //Single starting point cake
      const catalogCakes = result[cakeKey]._data;
      expect(catalogCakes.length).toBe(1);

      //Single catalogSeries layer related to starting cake
      const catalogSeriesLayers = result['catalogSeriesLayer']._data;
      expect(catalogSeriesLayers.length).toBe(1);
      expect(catalogSeriesLayers.map((l) => l._hash)).toEqual([
        staticExample().catalogSeriesLayer._data[0]._hash,
      ]);

      //All catalogSeries, due to layer is linking any of them
      const catalogSeries = result['catalogSeries']._data;
      expect(catalogSeries.length).toBe(
        staticExample().catalogSeries._data.length,
      );

      //All seriesCakes, due to catalogSeries are linking any of them
      const seriesCakes = result['seriesCake']._data;
      expect(seriesCakes.length).toBe(staticExample().seriesCake._data.length);
      expect(seriesCakes.map((c) => c._hash).sort()).toEqual(
        staticExample()
          .seriesCake._data.map((c) => c._hash)
          .sort(),
      );

      //All seriesCarsLayers, due to seriesCakes are linking any of them
      const seriesCarsLayers = result['seriesCarsLayer']._data;
      expect(seriesCarsLayers.length).toBe(
        staticExample().seriesCarsLayer._data.length,
      );
      expect(seriesCarsLayers.map((l) => l._hash).sort()).toEqual(
        staticExample()
          .seriesCarsLayer._data.map((l) => l._hash)
          .sort(),
      );

      //All seriesCars, due to seriesCarsLayers are linking any of them
      const seriesCars = result['seriesCars']._data;
      expect(seriesCars.length).toBe(staticExample().seriesCars._data.length);
      expect(seriesCars.map((c) => c._hash).sort()).toEqual(
        staticExample()
          .seriesCars._data.map((c) => c._hash)
          .sort(),
      );

      //All carCakes, due to seriesCars are linking any of them
      const carCakes = result['carCake']._data;
      expect(carCakes.length).toBe(staticExample().carCake._data.length);
      expect(carCakes.map((c) => c._hash).sort()).toEqual(
        staticExample()
          .carCake._data.map((c) => c._hash)
          .sort(),
      );

      //All carGeneralLayers, due to carCakes are linking any of them
      const carGeneralLayers = result['carGeneralLayer']._data;
      expect(carGeneralLayers.length).toBe(
        staticExample().carGeneralLayer._data.length,
      );
      expect(carGeneralLayers.map((l) => l._hash).sort()).toEqual(
        staticExample()
          .carGeneralLayer._data.map((l) => l._hash)
          .sort(),
      );

      //All carGeneralComponents, due to carGeneralLayers are linking any of them
      const carGeneralComponents = result['carGeneral']._data;
      expect(carGeneralComponents.length).toBe(2);
      expect(carGeneralComponents.map((c) => c._hash)).toEqual([
        staticExample().carGeneral._data[3]._hash,
        staticExample().carGeneral._data[7]._hash,
      ]);
    });
  });
  describe('insert', () => {
    it('insert on component route', async () => {
      const route = Route.fromFlat(`carGeneral`);

      const { tree, cell } = await db.get(route, {});

      const paths = cell.flatMap((c) => c.path);
      expect(paths.length).toBe(12);

      const path = paths[0];

      const component = {
        brand: 'Porsche',
        type: 'Macan Electric',
        doors: 4,
        energyConsumption: 2.1,
        units: {
          energy: 'kWh/100km',
          _hash: '',
        },
        serviceIntervals: [20000, 40000, 60000],
        isElectric: false,
        meta: {
          pressText: 'A sporty electric SUV.',
          _hash: '',
        },
        _type: 'components',
        _hash: '',
      } as CarGeneral;

      const insert = isolate(tree, path);
      inject(insert, path, component);

      const results = (await db.insert(
        route,
        insert,
      )) as InsertHistoryRow<'CarGeneral'>[];

      const result = results[0];

      expect(result).toBeDefined();
      expect(result.timeId).toBeDefined();
      expect(result.carGeneralRef).toBeDefined();
      expect(result.route).toBe(route.flat);
    });

    it('insert on component route, w/ previous by Hash', async () => {
      //Add predecessor component to core db
      const previousTimeId = 'H45H:20240606T120000Z';
      await db.core.import({
        carGeneralInsertHistory: {
          _type: 'insertHistory',
          _data: [
            {
              carGeneralRef: staticExample().carGeneral._data[0]._hash ?? '',
              timeId: previousTimeId,
              route: '/carGeneral',
            } as InsertHistoryRow<'CarGeneral'>,
          ],
        } as InsertHistoryTable<'CarGeneral'>,
      });

      const cloneRoute = Route.fromFlat(`carGeneral`);

      const { tree, cell } = await db.get(cloneRoute, {});

      const paths = cell.flatMap((c) => c.path);
      expect(paths.length).toBe(12);

      const path = paths[0];

      const component = {
        brand: 'Porsche',
        type: 'Macan Electric',
        doors: 4,
        energyConsumption: 2.1,
        units: {
          energy: 'kWh/100km',
          _hash: '',
        },
        serviceIntervals: [20000, 40000, 60000],
        isElectric: false,
        meta: {
          pressText: 'A sporty electric SUV.',
          _hash: '',
        },
        _type: 'components',
        _hash: '',
      } as CarGeneral;

      const insert = isolate(tree, path);
      inject(insert, path, component);

      const previousHash = staticExample().carGeneral._data[0]._hash ?? '';
      const route = Route.fromFlat(['/carGeneral', previousHash].join('@'));

      const results = (await db.insert(
        route,
        insert,
      )) as InsertHistoryRow<'CarGeneral'>[];

      const result = results[0];

      expect(result).toBeDefined();
      expect(result.timeId).toBeDefined();
      expect(result.carGeneralRef).toBeDefined();
      expect(result.route).toBe(route.flat);
      expect(result.previous).toEqual([previousTimeId]);
    });

    it('insert on component route, w/ previous by TimeID', async () => {
      //Add predecessor component to core db
      const previousTimeId = 'H45H:20240606T120000Z';
      await db.core.import({
        carGeneralInsertHistory: {
          _type: 'insertHistory',
          _data: [
            {
              carGeneralRef: staticExample().carGeneral._data[0]._hash ?? '',
              timeId: previousTimeId,
              route: '/carGeneral',
            } as InsertHistoryRow<'CarGeneral'>,
          ],
        } as InsertHistoryTable<'CarGeneral'>,
      });

      const cloneRoute = Route.fromFlat(`carGeneral`);

      const { tree, cell } = await db.get(cloneRoute, {});

      const paths = cell.flatMap((c) => c.path);
      expect(paths.length).toBe(12);

      const path = paths[0];

      const component = {
        brand: 'Porsche',
        type: 'Macan Electric',
        doors: 4,
        energyConsumption: 2.1,
        units: {
          energy: 'kWh/100km',
          _hash: '',
        },
        serviceIntervals: [20000, 40000, 60000],
        isElectric: false,
        meta: {
          pressText: 'A sporty electric SUV.',
          _hash: '',
        },
        _type: 'components',
        _hash: '',
      } as CarGeneral;

      const insert = isolate(tree, path);
      inject(insert, path, component);

      const route = Route.fromFlat(['/carGeneral', previousTimeId].join('@'));

      const results = (await db.insert(
        route,
        insert,
      )) as InsertHistoryRow<'CarGeneral'>[];

      const result = results[0];

      expect(result).toBeDefined();
      expect(result.timeId).toBeDefined();
      expect(result.carGeneralRef).toBeDefined();
      expect(result.route).toBe(route.flat);
      expect(result.previous).toEqual([previousTimeId]);
    });

    it('insert on layer route', async () => {
      const sliceId = 'VIN1';
      const cakeRef = (staticExample().carCake._data[0]._hash ?? '') as string;
      const route = Route.fromFlat(
        `/carCake(${sliceId})@${cakeRef}/carGeneralLayer`,
      );

      const { tree, cell } = await db.get(route, {});

      const paths = cell.flatMap((c) => c.path);
      expect(paths.length).toBe(1);

      const path = paths[0];

      const layer = {
        add: {
          VIN3: staticExample().carGeneral._data[2]._hash || '',
          VIN4: staticExample().carGeneral._data[3]._hash || '',
        },
        componentsTable: 'carGeneral',
        sliceIdsTable: 'carSliceId',
        sliceIdsTableRow: staticExample().carSliceId._data[0]._hash as string,
      } as Layer;

      const insert = isolate(tree, path);
      inject(insert, path, layer);

      const results = (await db.insert(
        route,
        insert,
      )) as InsertHistoryRow<'CarCake'>[];

      expect(results.length).toBe(1);

      const result = results[0];
      expect(result).toBeDefined();
      expect(result.timeId).toBeDefined();
      expect(result.carCakeRef).toBeDefined();
      expect(result.route).toBe(route.flat);
    });

    it('insert on cake route', async () => {
      const route = Route.fromFlat(`/carCake`);

      const { tree, cell } = await db.get(route, {});

      const paths = cell.flatMap((c) => c.path);
      expect(paths.length).toBe(3);

      const path = paths[0];

      const cake = {
        layers: {
          carGeneralLayer: staticExample().carGeneralLayer._data[0]
            ._hash as string,
        },
        sliceIdsTable: 'carSliceId',
        sliceIdsRow: staticExample().carSliceId._data[0]._hash as string,
      } as Cake;

      const insert = isolate(tree, path);
      inject(insert, path, cake);

      const results = (await db.insert(
        route,
        insert,
      )) as InsertHistoryRow<'CarCake'>[];

      expect(results.length).toBe(1);

      const result = results[0];
      expect(result).toBeDefined();
      expect(result.timeId).toBeDefined();
      expect(result.carCakeRef).toBeDefined();
      expect(result.route).toBe(route.flat);
    });

    it('insert on nested: component/component', async () => {
      const route = Route.fromFlat(`/carTechnical/carDimensions`);

      const { tree, cell } = await db.get(route, {});

      const paths = cell.flatMap((c) => c.path);
      expect(paths.length).toBe(12);

      const path = paths[0];

      const component = {
        height: 1234,
        width: 4567,
        length: 8901,
      };

      inject(tree, path, component);

      //Nested --> Isolate only to first component
      const insert = isolate(tree, path.slice(0, -3));

      const results = (await db.insert(
        route,
        insert,
      )) as InsertHistoryRow<'CarTechnical'>[];

      expect(results.length).toBe(1);

      const result = results[0];
      expect(result).toBeDefined();
      expect(result.timeId).toBeDefined();
      expect(result.carTechnicalRef).toBeDefined();
      expect(result.route).toBe(route.flat);

      const writtenRow = await db.core.readRow(
        'carTechnical',
        result.carTechnicalRef as string,
      );
      expect(writtenRow).toBeDefined();
      expect(writtenRow?.carTechnical?._data.length).toBe(1);
      const writtenComponent = writtenRow?.carTechnical?._data[0] as Json;
      expect(writtenComponent.dimensions).toBeDefined();

      const writtenDimensionRow = await db.core.readRow(
        'carDimensions',
        (writtenComponent.dimensions as any)[0] as string,
      );
      expect(writtenDimensionRow).toBeDefined();
      expect(writtenDimensionRow?.carDimensions?._data.length).toBe(1);
      const writtenDimension = writtenDimensionRow?.carDimensions
        ?._data[0] as CarGeneral;
      expect(writtenDimension.height).toBe(1234);
      expect(writtenDimension.width).toBe(4567);
      expect(writtenDimension.length).toBe(8901);
    });

    it('insert on layer/component by layer', async () => {
      const sliceId = 'VIN1';
      const cakeRef = (staticExample().carCake._data[0]._hash ?? '') as string;
      const route = Route.fromFlat(
        `/carCake(${sliceId})@${cakeRef}/carGeneralLayer/carGeneral`,
      );

      const { tree, cell } = await db.get(route, {});

      const paths = cell.flatMap((c) => c.path);
      expect(paths.length).toBe(1);

      const path = paths[0];

      const layer = {
        add: {
          VIN1: {
            carGeneral: {
              _data: [
                {
                  brand: 'Toyota',
                  type: 'Yaris',
                  doors: 2,
                  energyConsumption: 8.2,
                  units: {
                    energy: 'l/100km',
                  },
                  serviceIntervals: [12000, 24000, 36000],
                  isElectric: false,
                  meta: {
                    pressText: 'A reliable subcompact car.',
                  },
                },
              ],
              _type: 'components',
            },
          },
        },
        sliceIdsTable: 'carSliceId',
        sliceIdsTableRow: staticExample().carSliceId._data[0]._hash as string,
        componentsTable: 'carGeneral',
      };

      const insert = isolate(tree, path);
      inject(insert, path.slice(0, -5), layer);

      const results = (await db.insert(
        route,
        insert,
      )) as InsertHistoryRow<'CarCake'>[];

      expect(results.length).toBe(1);

      const result = results[0];
      expect(result).toBeDefined();
      expect(result.timeId).toBeDefined();
      expect(result.carCakeRef).toBeDefined();

      const writtenCake = await db.core.readRow(
        'carCake',
        result.carCakeRef as string,
      );
      expect(writtenCake).toBeDefined();

      const writtenLayerHash = (writtenCake?.carCake?._data[0].layers as any)
        .carGeneralLayer as string;

      const writtenLayer = await db.core.readRow(
        'carGeneralLayer',
        writtenLayerHash,
      );
      expect(writtenLayer).toBeDefined();

      const writtenComponentHash = (
        writtenLayer?.carGeneralLayer?._data[0].add as any
      ).VIN1 as string;

      const writtenComponent = await db.core.readRow(
        'carGeneral',
        writtenComponentHash,
      );
      expect(writtenComponent).toBeDefined();
      expect(writtenComponent?.carGeneral?._data.length).toBe(1);
      const writtenComponentData = writtenComponent?.carGeneral
        ?._data[0] as CarGeneral;
      expect(writtenComponentData.brand).toBe('Toyota');
      expect(writtenComponentData.type).toBe('Yaris');
      expect(writtenComponentData.doors).toBe(2);
    });

    it('insert on cake/layer/component by cake', async () => {
      const sliceId = 'VIN1';
      const cakeRef = (staticExample().carCake._data[0]._hash ?? '') as string;
      const route = Route.fromFlat(
        `/carCake(${sliceId})@${cakeRef}/carGeneralLayer/carGeneral`,
      );

      const { tree, cell } = await db.get(route, {});

      const paths = cell.flatMap((c) => c.path);
      expect(paths.length).toBe(1);

      const path = paths[0];

      const cake = {
        _data: [
          {
            sliceIdsTable: 'carSliceId',
            sliceIdsRow: '2RefO1-vC_FbgYaC1FWzFo',
            layers: {
              carGeneralLayer: {
                _data: [
                  {
                    add: {
                      VIN1: {
                        carGeneral: {
                          _data: [
                            {
                              brand: 'Toyota',
                              type: 'Yaris',
                              doors: 2,
                              energyConsumption: 8.2,
                              units: {
                                energy: 'l/100km',
                              },
                              serviceIntervals: [12000, 24000, 36000],
                              isElectric: false,
                              meta: {
                                pressText: 'A reliable subcompact car.',
                              },
                            },
                          ],
                          _type: 'components',
                        },
                      },
                    },
                    sliceIdsTable: 'carSliceId',
                    sliceIdsTableRow: staticExample().carSliceId._data[0]
                      ._hash as string,
                    componentsTable: 'carGeneral',
                  },
                ],
                _type: 'layers',
              },
              carTechnicalLayer: staticExample().carTechnicalLayer._data[0]
                ._hash as string,
              carColorLayer: staticExample().carColorLayer._data[0]
                ._hash as string,
            },
          },
        ],
        _type: 'cakes',
      };

      const insert = isolate(tree, path);
      inject(insert, ['carCake'], cake);

      const results = (await db.insert(
        route,
        insert,
      )) as InsertHistoryRow<'CarCake'>[];

      expect(results.length).toBe(1);

      const result = results[0];
      expect(result).toBeDefined();
      expect(result.timeId).toBeDefined();
      expect(result.carCakeRef).toBeDefined();

      const writtenCake = await db.core.readRow(
        'carCake',
        result.carCakeRef as string,
      );
      expect(writtenCake).toBeDefined();

      const writtenLayerHash = (writtenCake?.carCake?._data[0].layers as any)
        .carGeneralLayer as string;

      const writtenLayer = await db.core.readRow(
        'carGeneralLayer',
        writtenLayerHash,
      );
      expect(writtenLayer).toBeDefined();

      const writtenComponentHash = (
        writtenLayer?.carGeneralLayer?._data[0].add as any
      ).VIN1 as string;

      const writtenComponent = await db.core.readRow(
        'carGeneral',
        writtenComponentHash,
      );
      expect(writtenComponent).toBeDefined();
      expect(writtenComponent?.carGeneral?._data.length).toBe(1);
      const writtenComponentData = writtenComponent?.carGeneral
        ?._data[0] as CarGeneral;
      expect(writtenComponentData.brand).toBe('Toyota');
      expect(writtenComponentData.type).toBe('Yaris');
      expect(writtenComponentData.doors).toBe(2);
    });

    it('insert multiple components on nested /cake/layer/component/property', async () => {
      const route = Route.fromFlat(
        '/carCake/carGeneralLayer/carGeneral/brand',
      ).toRouteWithProperty();
      const cakeRef = staticExample().carCake._data[0]._hash ?? '';
      const { tree, cell } = await db.get(route, cakeRef as string);

      const insert = { ...tree };
      for (const { path } of cell) {
        for (const p of path) {
          inject({ ...insert }, p, 'AMG');
        }
      }
      const inserted: InsertHistoryRow<any>[] = await db.insert(
        route,
        rmhsh(insert),
      );

      expect(inserted).toBeDefined();

      const { cell: values } = await db.get(
        route,
        inserted[0].carCakeRef as string,
      );

      const results = values.flatMap((v) => v.value);

      expect(Array.from(new Set(results))).toEqual(['AMG']);
      expect(results.length).toBe(cell.flatMap((c) => c.value).length);
    });

    it('insert single component on nested /cake/layer/component/property', async () => {
      const sliceId = 'VIN1';
      const cakeRef = (staticExample().carCake._data[0]._hash ?? '') as string;
      const route = Route.fromFlat(
        `/carCake(${sliceId})@${cakeRef}/carGeneralLayer/carGeneral`,
      );

      const { tree, cell } = await db.get(route, {});

      const paths = cell.flatMap((c) => c.path);
      expect(paths.length).toBe(1);

      const path = paths[0];

      const component = {
        brand: 'Volkswagen',
        type: 'Polo',
        doors: 5,
        energyConsumption: 7.4,
        units: {
          energy: 'l/100km',
          _hash: '',
        },
        serviceIntervals: [15000, 30000, 45000],
        isElectric: false,
        meta: {
          pressText: 'A popular compact car.',
          _hash: '',
        },
        _type: 'components',
        _hash: '',
      } as CarGeneral;

      const insert = isolate(tree, path);
      inject(insert, path, component);

      const results = (await db.insert(
        route,
        insert,
      )) as InsertHistoryRow<'CarCake'>[];

      expect(results.length).toBe(1);

      const result = results[0];
      expect(result).toBeDefined();
      expect(result.timeId).toBeDefined();
      expect(result.carCakeRef).toBeDefined();
      expect(result.route).toBe(route.flat);
    });
  });
  describe('notify', () => {
    it('returns 0 callbacks for unregistered route', () => {
      const route = Route.fromFlat('/carGeneral');
      expect(db.notify.getCallBacksForRoute(route).length).toBe(0);
    });

    it('register/unregister a callback', async () => {
      const callback = vi.fn();
      const route = Route.fromFlat('/carGeneral');

      db.registerObserver(route, callback);
      expect(db.notify.callbacks.size).toBe(1);

      db.unregisterObserver(route, callback);
      expect(db.notify.getCallBacksForRoute(route).length).toBe(0);
    });

    it('unregisterAllObservers', async () => {
      const callback = vi.fn();
      const route = Route.fromFlat('/carGeneral');

      db.registerObserver(route, callback);
      expect(db.notify.callbacks.size).toBe(1);

      db.unregisterAllObservers(route);
      expect(db.notify.getCallBacksForRoute(route).length).toBe(0);
    });

    it('notify on component route', async () => {
      const callback = vi.fn();
      const route = Route.fromFlat('/carGeneral');

      db.registerObserver(route, callback);

      const { tree, cell } = await db.get(route, {});

      const paths = cell.flatMap((c) => c.path);
      expect(paths.length).toBe(12);

      const path = paths[0];

      const component = {
        brand: 'Porsche',
        type: 'Macan Electric',
        doors: 4,
        energyConsumption: 2.1,
        units: {
          energy: 'kWh/100km',
          _hash: '',
        },
        serviceIntervals: [20000, 40000, 60000],
        isElectric: false,
        meta: {
          pressText: 'A sporty electric SUV.',
          _hash: '',
        },
        _type: 'components',
        _hash: '',
      } as CarGeneral;

      const insert = isolate(tree, path);
      inject(insert, path, component);

      const results = (await db.insert(
        route,
        insert,
      )) as InsertHistoryRow<'CarGeneral'>[];
      const result = results[0];

      expect(callback).toHaveBeenCalledTimes(1);
      expect(callback).toHaveBeenCalledWith(result);
    });
    it('notify several times on component route', async () => {
      const callback = vi.fn();
      const route = Route.fromFlat('/carGeneral');

      db.registerObserver(route, callback);

      const { tree, cell } = await db.get(route, {});

      const paths = cell.flatMap((c) => c.path);
      expect(paths.length).toBe(12);

      const path = paths[0];

      const component = {
        brand: 'Porsche',
        type: 'Macan Electric',
        doors: 4,
        energyConsumption: 2.1,
        units: {
          energy: 'kWh/100km',
          _hash: '',
        },
        serviceIntervals: [20000, 40000, 60000],
        isElectric: false,
        meta: {
          pressText: 'A sporty electric SUV.',
          _hash: '',
        },
        _type: 'components',
        _hash: '',
      } as CarGeneral;

      const insert = isolate(tree, path);
      inject(insert, path, component);

      const results = (await db.insert(
        route,
        insert,
      )) as InsertHistoryRow<'CarGeneral'>[];
      const result = results[0];

      const component2 = {
        brand: 'Porsche',
        type: 'Macan Electric',
        doors: 4,
        energyConsumption: 2.1,
        units: {
          energy: 'kWh/100km',
          _hash: '',
        },
        serviceIntervals: [20000, 40000, 60000],
        isElectric: false,
        meta: {
          pressText: 'A sporty electric SUV.',
          _hash: '',
        },
        _type: 'components',
        _hash: '',
      } as CarGeneral;

      const insert2 = isolate(tree, path);
      inject(insert, path, component2);

      const results2 = (await db.insert(
        route,
        insert2,
      )) as InsertHistoryRow<'CarGeneral'>[];
      const result2 = results2[0];

      expect(callback).toHaveBeenCalledTimes(2);
      expect(callback).toHaveBeenNthCalledWith(1, result);
      expect(callback).toHaveBeenNthCalledWith(2, result2);
    });

    it('notify on nested component/component route', async () => {
      const route = Route.fromFlat('/carTechnical/carDimensions');

      const carTechnicalCallback = vi.fn();
      db.registerObserver(
        Route.fromFlat('/carTechnical/carDimensions'),
        carTechnicalCallback,
      );

      const carDimensionsCallback = vi.fn();
      db.registerObserver(
        Route.fromFlat('/carDimensions'),
        carDimensionsCallback,
      );

      const { tree, cell } = await db.get(route, {});

      const paths = cell.flatMap((c) => c.path);
      expect(paths.length).toBe(12);

      const path = paths[0];

      const component = {
        height: 1234,
        width: 4567,
        length: 8901,
      };

      inject(tree, path, component);

      //Nested --> Isolate only to first component
      const insert = isolate(tree, path.slice(0, -3));

      await db.insert(route, insert);

      //Get written insertHistory rows
      const {
        ['carTechnicalInsertHistory']: { _data: carTechnicalInsertHistory },
      } = await db.getInsertHistory('carTechnical');
      const carTechnicalInsertHistoryRow = rmhsh(carTechnicalInsertHistory[0]);

      const {
        ['carDimensionsInsertHistory']: { _data: carDimensionsInsertHistory },
      } = await db.getInsertHistory('carDimensions');
      const carDimensionsInsertHistoryRow = rmhsh(
        carDimensionsInsertHistory[0],
      );

      expect(carTechnicalCallback).toHaveBeenCalledTimes(1);
      expect(carTechnicalCallback).toHaveBeenNthCalledWith(
        1,
        carTechnicalInsertHistoryRow,
      );

      expect(carDimensionsCallback).toHaveBeenCalledTimes(1);
      expect(carDimensionsCallback).toHaveBeenNthCalledWith(
        1,
        carDimensionsInsertHistoryRow,
      );
    });
    it('notify on nested cake/layer/component route', async () => {
      const callback = vi.fn();
      const sliceId = 'VIN1';
      const cakeRef = (staticExample().carCake._data[0]._hash ?? '') as string;
      const route = Route.fromFlat(
        `/carCake(${sliceId})@${cakeRef}/carGeneralLayer/carGeneral`,
      );

      db.registerObserver(route, callback);

      const { tree, cell } = await db.get(route, {});

      const paths = cell.flatMap((c) => c.path);
      expect(paths.length).toBe(1);

      const path = paths[0];

      const cake = {
        _data: [
          {
            sliceIdsTable: 'carSliceId',
            sliceIdsRow: staticExample().carSliceId._data[0]._hash as string,
            layers: {
              carGeneralLayer: {
                _data: [
                  {
                    add: {
                      VIN1: {
                        carGeneral: {
                          _data: [
                            {
                              brand: 'Toyota',
                              type: 'Yaris',
                              doors: 2,
                              energyConsumption: 8.2,
                              units: {
                                energy: 'l/100km',
                              },
                              serviceIntervals: [12000, 24000, 36000],
                              isElectric: false,
                              meta: {
                                pressText: 'A reliable subcompact car.',
                              },
                            },
                          ],
                          _type: 'components',
                        },
                      },
                    },
                    sliceIdsTable: 'carSliceId',
                    sliceIdsTableRow: staticExample().carSliceId._data[0]
                      ._hash as string,
                    componentsTable: 'carGeneral',
                  },
                ],
                _type: 'layers',
              },
              carTechnicalLayer: staticExample().carTechnicalLayer._data[0]
                ._hash as string,
              carColorLayer: staticExample().carColorLayer._data[0]
                ._hash as string,
            },
          },
        ],
        _type: 'cakes',
      };

      const insert = isolate(tree, path);
      inject(insert, ['carCake'], cake);

      await db.insert(route, insert);

      //Get written insertHistory rows
      const {
        ['carCakeInsertHistory']: { _data: carCakeInsertHistory },
      } = await db.getInsertHistory('carCake');
      const carCakeInsertHistoryRow = rmhsh(carCakeInsertHistory[0]);

      expect(callback).toHaveBeenCalledTimes(1);
      expect(callback).toHaveBeenNthCalledWith(1, carCakeInsertHistoryRow);
    });
    it('skips notification on component route', async () => {
      const callback = vi.fn();
      const route = Route.fromFlat('/carGeneral');

      db.registerObserver(route, callback);
      const { tree, cell } = await db.get(route, {});

      const paths = cell.flatMap((c) => c.path);
      expect(paths.length).toBe(12);

      const path = paths[0];

      const component = {
        brand: 'Porsche',
        type: 'Macan Electric',
        doors: 4,
        energyConsumption: 2.1,
        units: {
          energy: 'kWh/100km',
          _hash: '',
        },
        serviceIntervals: [20000, 40000, 60000],
        isElectric: false,
        meta: {
          pressText: 'A sporty electric SUV.',
          _hash: '',
        },
        _type: 'components',
        _hash: '',
      } as CarGeneral;

      const insert = isolate(tree, path);
      inject(insert, path, component);

      await db.insert(route, insert, { skipNotification: true });

      expect(callback).toHaveBeenCalledTimes(0);
    });
  });
  describe('join', () => {
    const cakeKey = 'carCake';
    const cakeRef = staticExample().carCake._data[2]._hash as string;

    it('should be defined', () => {
      expect(db.join).toBeDefined();
    });

    it('should join data of single route correctly', async () => {
      const columnInfos = [
        {
          route: 'carCake/carGeneralLayer/carGeneral/brand',
          key: 'brand',
          alias: 'brand',
          titleShort: 'Brand',
          titleLong: 'Car Brand',
          type: 'string',
        },
      ] as ColumnInfo[];

      const columnSelection = new ColumnSelection(columnInfos);
      const result = await db.join(columnSelection, cakeKey, cakeRef);

      // Build expected data for validation
      const exampleData = staticExample().carGeneral._data.map((c) => [
        c.brand,
      ]);

      expect(result.rows.flatMap((r) => r)).toEqual(exampleData);
    });

    it('should join data with missing/nulled values correctly', async () => {
      const columnInfos = [
        {
          route: 'carCake/carTechnicalLayer/carTechnical/repairedByWorkshop',
          key: 'repairedByWorkshop',
          alias: 'repairedByWorkshop',
          titleShort: 'Repaired By Workshop',
          titleLong: 'Last Repaired By Workshop',
          type: 'string',
        },
      ] as ColumnInfo[];

      const columnSelection = new ColumnSelection(columnInfos);
      const result = await db.join(columnSelection, cakeKey, cakeRef);

      // Build expected data for validation

      // Get slice IDs from carSliceIds layer
      const carSliceIds = (
        staticExample().carSliceId as SliceIdsTable
      )._data.flatMap((d) => d.add);

      // Get technical layer values to map slice IDs to component refs
      const carTechnicalLayerValues = (
        staticExample().carTechnicalLayer as LayersTable
      )._data.flatMap((l) =>
        Object.entries(l.add).map(([sliceId, compRef]) => ({
          sliceId,
          compRef,
        })),
      );

      // Map slice IDs to repairedByWorkshop values
      const exampleData = [];
      for (const carSliceId of carSliceIds) {
        const layerEntry = carTechnicalLayerValues.find(
          (l) => l.sliceId === carSliceId,
        );
        if (layerEntry) {
          const compRef = layerEntry.compRef;
          const comp = staticExample().carTechnical._data.find(
            (c) => c._hash === compRef,
          );
          if (comp && comp.repairedByWorkshop !== undefined) {
            // Value exists
            exampleData.push([[comp.repairedByWorkshop]]);
            continue;
          }
          exampleData.push([[null]]); // Value missing in component
        }
      }

      // Validate joined data
      expect(result.rows).toEqual(exampleData);
    });

    it('should join data of multiple routes correctly', async () => {
      const columnInfos = [
        {
          route: 'carCake/carGeneralLayer/carGeneral/isElectric',
          key: 'isElectric',
          alias: 'isElectric',
          titleShort: 'Is Electric',
          titleLong: 'This Car is Electric',
          type: 'boolean',
        },
        {
          route: 'carCake/carTechnicalLayer/carTechnical/transmission',
          key: 'transmission',
          alias: 'transmission',
          titleShort: 'Transmission',
          titleLong: 'Type of Transmission',
          type: 'string',
        },
      ] as ColumnInfo[];

      const columnSelection = new ColumnSelection(columnInfos);
      const result = await db.join(columnSelection, cakeKey, cakeRef);

      // Validate joined data
      expect(result.rows).toEqual([
        [[false], ['Manual']],
        [[false], ['Automatic']],
        [[true], ['Single-Speed']],
        [[true], ['Single-Speed']],
        [[true], ['Single-Speed']],
        [[false], ['Manual']],
        [[true], ['Single-Speed']],
        [[true], ['Single-Speed']],
        [[true], ['Single-Speed']],
        [[true], ['Single-Speed']],
        [[true], ['Single-Speed']],
        [[true], ['Single-Speed']],
      ]);
    });

    it('throws error if cake ref is not found', async () => {
      const columnInfos = [
        {
          route: 'carCake/carGeneralLayer/carGeneral/brand',
          key: 'brand',
          alias: 'brand',
          titleShort: 'Brand',
          titleLong: 'Car Brand',
          type: 'string',
        },
      ] as ColumnInfo[];

      const columnSelection = new ColumnSelection(columnInfos);
      await expect(
        db.join(columnSelection, cakeKey, 'MISSING_CAKE_REF'),
      ).rejects.toThrow('Base cake MISSING_CAKE_REF does not exist.');
    });
  });
});
