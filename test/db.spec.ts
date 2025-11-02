// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip, rmhsh } from '@rljson/hash';
import { IoMem } from '@rljson/io';
import { Json, JsonValue } from '@rljson/json';
import {
  History,
  HistoryRow,
  Insert,
  LayerRef,
  LayersTable,
  Route,
  SliceIdsTable,
} from '@rljson/rljson';

import { beforeEach, describe, expect, it, vi } from 'vitest';

import { CarGeneral, carsExample } from '../src/cars-example';
import { Db } from '../src/db';
import { Edit, exampleEditCarsExample } from '../src/edit/edit/edit';
import {
  ColumnInfo,
  ColumnSelection,
} from '../src/edit/selection/column-selection';

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
    for (const tableCfg of carsExample().tableCfgs._data) {
      await db.core.createTableWithHistory(tableCfg);
    }

    //Import Data
    await db.core.import(carsExample());
  });

  describe('core', () => {
    it('should be defined', () => {
      expect(db.core).toBeDefined();
    });
  });
  describe('get', () => {
    it('throws on invalid route', async () => {
      await expect(db.get(new Route([]), {})).rejects.toThrow(
        'Route  is not valid.',
      );
    });
    it('get component by ref', async () => {
      const route = '/carGeneral';
      const ref = carsExample().carGeneral._data[0]._hash ?? '';

      const result = await db.get(Route.fromFlat(route), ref);
      expect(result).toBeDefined();
      expect(result[0].carGeneral).toBeDefined();
      expect(result[0].carGeneral._data.length).toBe(1);
      expect(result[0].carGeneral._data[0]._hash).toBe(ref);
    });
    it('get component property by ref', async () => {
      const componentKey = 'carGeneral';
      const propertyKey = 'brand';
      const route = `${componentKey}/${propertyKey}`;

      const ref = carsExample().carGeneral._data[0]._hash ?? '';

      const result = await db.get(Route.fromFlat(route), ref);

      expect(result).toBeDefined();
      expect(result[0][componentKey]).toBeDefined();
      expect(result[0][componentKey]._data.length).toBe(1);
      expect(result[0][componentKey]._data[0]).toEqual({
        _hash: ref,
        [propertyKey]: carsExample().carGeneral._data[0][propertyKey],
      });
      expect(result[0][componentKey]._data[0][propertyKey]).toBe(
        carsExample().carGeneral._data[0][propertyKey],
      );
    });
    it('get component by where', async () => {
      const route = '/carGeneral';
      const where = rmhsh(carsExample().carGeneral._data[0]) as {
        [column: string]: JsonValue;
      };

      const result = await db.get(Route.fromFlat(route), where);
      expect(result).toBeDefined();
      expect(result[0].carGeneral).toBeDefined();
      expect(result[0].carGeneral._data.length).toBe(1);
      expect(result[0].carGeneral._data[0]._hash).toBe(
        carsExample().carGeneral._data[0]._hash,
      );
    });
    it('get components by where', async () => {
      const route = '/carGeneral';
      const where = { brand: 'Volkswagen' } as Partial<CarGeneral>;

      const result = await db.get(Route.fromFlat(route), where);
      expect(result).toBeDefined();
      expect(result[0].carGeneral).toBeDefined();
      expect(result[0].carGeneral._data.length).toBe(2); //2 Volkswagens in example data
      expect(result[0].carGeneral._data[0].brand).toBe('Volkswagen');
      expect(result[0].carGeneral._data[1].brand).toBe('Volkswagen');
    });
    it('get layer by ref', async () => {
      const route = '/carGeneralLayer';
      const ref =
        (carsExample().carGeneralLayer._data[0]._hash as string) ?? '';

      const result = await db.get(Route.fromFlat(route), ref);
      expect(result).toBeDefined();
      expect(result[0].carGeneralLayer).toBeDefined();
      expect(result[0].carGeneralLayer._data.length).toBe(1);
      expect(result[0].carGeneralLayer._data[0]._hash).toBe(ref);
    });
    it('get cake by ref', async () => {
      const route = '/carCake';
      const ref = (carsExample().carCake._data[0]._hash as string) ?? '';

      const result = await db.get(Route.fromFlat(`${route}@${ref}`), {});
      expect(result).toBeDefined();
      expect(result[0].carCake).toBeDefined();
      expect(result[0].carCake._data.length).toBe(1);
      expect(result[0].carCake._data[0]._hash).toBe(ref);
    });
    it('get cake/layer by ref', async () => {
      const cakeRoute = '/carCake';
      const cakeRef = (carsExample().carCake._data[0]._hash as string) ?? '';

      const result = await db.get(
        Route.fromFlat(`${cakeRoute}@${cakeRef}/carGeneralLayer`),
        {},
      );
      expect(result).toBeDefined();
      expect(result[0].carCake).toBeDefined();
      expect(result[0].carCake._data.length).toBe(1);
      expect(result[0].carCake._data[0]._hash).toBe(cakeRef);
    });
    it('get nested layer/component by where', async () => {
      const route = '/carGeneralLayer/carGeneral';
      const where = {
        carGeneral: rmhsh(carsExample().carGeneral._data[0]) as {
          [column: string]: JsonValue;
        },
      };

      const result = await db.get(Route.fromFlat(route), where);
      expect(result).toBeDefined();
      expect(result[0].carGeneralLayer).toBeDefined();
      expect(result[0].carGeneralLayer._data.length).toBe(1);
      expect(result[0].carGeneralLayer._data[0]._hash).toBe(
        carsExample().carGeneralLayer._data[0]._hash,
      );
      expect(result[0].carGeneral).toBeDefined();
      expect(result[0].carGeneral._data.length).toBe(1);
      expect(result[0].carGeneral._data[0]._hash).toBe(
        carsExample().carGeneral._data[0]._hash,
      );
    });
    it('get nested layer/component by where w/ revision', async () => {
      //Add Layer with switching VIN -> CarGeneral relation
      const Insert: Insert<Json> = {
        route: '/carGeneralLayer',
        command: 'add',
        value: {
          VIN1: carsExample().carGeneral._data[1]._hash || '',
          VIN2: carsExample().carGeneral._data[0]._hash || '',
          _hash: '',
        } as Json,
        origin: 'H45H',
        acknowledged: false,
      };

      const addedLayerHistory = (await db.insert(
        Insert,
      )) as HistoryRow<'CarGeneralLayer'>;

      //Search for first carGeneral
      const where = {
        carGeneral: rmhsh(carsExample().carGeneral._data[0]) as {
          [column: string]: JsonValue;
        },
      };

      //GET Result via first layer revision
      const layerRevHash1 = carsExample().carGeneralLayer._data[0]._hash ?? '';
      const route1 = `/carGeneralLayer@${layerRevHash1}/carGeneral`;

      //GET Result via second layer revision
      const layerRevHash2 = addedLayerHistory.carGeneralLayerRef ?? '';
      const route2 = `/carGeneralLayer@${layerRevHash2}/carGeneral`;

      const result1 = await db.get(Route.fromFlat(route1), where);

      const result2 = await db.get(Route.fromFlat(route2), where);

      expect(result1).toBeDefined();
      expect(result2).toBeDefined();

      expect(result1[0].carGeneral).toBeDefined();
      expect(result1[0].carGeneral._data.length).toBe(1);
      expect(result1[0].carGeneral._data[0]._hash).toBe(
        carsExample().carGeneral._data[0]._hash,
      );

      expect(result2[0].carGeneral).toBeDefined();
      expect(result2[0].carGeneral._data.length).toBe(1);
      expect(result2[0].carGeneral._data[0]._hash).toBe(
        carsExample().carGeneral._data[0]._hash,
      );

      expect(result1[0].carGeneralLayer).toBeDefined();
      expect(result1[0].carGeneralLayer._data.length).toBe(1);
      expect(result1[0].carGeneralLayer._data[0]._hash).toBe(layerRevHash1);

      expect(result2[0].carGeneralLayer).toBeDefined();
      expect(result2[0].carGeneralLayer._data.length).toBe(1);
      expect(result2[0].carGeneralLayer._data[0]._hash).toBe(layerRevHash2);
    });
    it('get nested component/component by where', async () => {
      const route = '/carTechnical/carDimensions';
      const where = {
        carDimensions: rmhsh(carsExample().carDimensions._data[0]) as {
          [column: string]: JsonValue;
        },
      };

      const result = await db.get(Route.fromFlat(route), where);
      expect(result).toBeDefined();
      expect(result[0].carTechnical).toBeDefined();
      expect(result[0].carTechnical._data.length).toBe(1);
      expect(result[0].carTechnical._data[0]._hash).toBe(
        carsExample().carTechnical._data[0]._hash,
      );
      expect(result[0].carDimensions).toBeDefined();
      expect(result[0].carDimensions._data.length).toBe(1);
      expect(result[0].carDimensions._data[0]._hash).toBe(
        carsExample().carDimensions._data[0]._hash,
      );
    });
    it('get nested cake/layer/component by where', async () => {
      const route = '/carCake/carGeneralLayer/carGeneral';
      const where = {
        carGeneralLayer: {
          carGeneral: rmhsh(carsExample().carGeneral._data[0]) as {
            [column: string]: JsonValue;
          },
        },
      };

      const result = await db.get(Route.fromFlat(route), where);
      expect(result).toBeDefined();
      expect(result[0].carCake).toBeDefined();
      expect(result[0].carCake._data.length).toBe(1);
      expect(result[0].carCake._data[0]._hash).toBe(
        carsExample().carCake._data[0]._hash,
      );
      expect(result[0].carGeneralLayer).toBeDefined();
      expect(result[0].carGeneralLayer._data.length).toBe(1);
      expect(result[0].carGeneralLayer._data[0]._hash).toBe(
        carsExample().carGeneralLayer._data[0]._hash,
      );
      expect(result[0].carGeneral).toBeDefined();
      expect(result[0].carGeneral._data.length).toBe(1);
      expect(result[0].carGeneral._data[0]._hash).toBe(
        carsExample().carGeneral._data[0]._hash,
      );
    });
    it('get nested cake/layer/component by hash w/ revision hash', async () => {
      const cakeRevisionHash = carsExample().carCake._data[0]._hash ?? '';
      const layerRevisionHash =
        carsExample().carGeneralLayer._data[0]._hash ?? '';

      const route = `/carCake@${cakeRevisionHash}/carGeneralLayer@${layerRevisionHash}/carGeneral`;

      const where = {
        carGeneralLayer: {
          carGeneral: {
            _hash: carsExample().carGeneral._data[0]._hash ?? '',
          },
        },
      };
      const result = await db.get(Route.fromFlat(route), where);

      expect(result).toBeDefined();
      expect(result[0].carCake).toBeDefined();
      expect(result[0].carCake._data.length).toBe(1);
      expect(result[0].carCake._data[0]._hash).toBe(cakeRevisionHash);

      expect(result[0].carGeneralLayer).toBeDefined();
      expect(result[0].carGeneralLayer._data.length).toBe(1);
      expect(result[0].carGeneralLayer._data[0]._hash).toBe(layerRevisionHash);

      expect(result[0].carGeneral).toBeDefined();
      expect(result[0].carGeneral._data.length).toBe(1);
      expect(result[0].carGeneral._data[0]._hash).toBe(
        carsExample().carGeneral._data[0]._hash,
      );
    });
    it('get nested cake/layer/component by hash w/ revision TimeId', async () => {
      //Add new History Entry to Layer Revisions, recursively adding it to the cake
      const layerInsert: Insert<Json> = {
        route: '/carCake/carGeneralLayer',
        command: 'add',
        value: {
          carGeneralLayer: {
            VIN1: carsExample().carGeneral._data[1]._hash || '',
            VIN2: carsExample().carGeneral._data[0]._hash || '',
            _hash: '',
          },
        } as Json,
        origin: 'H45H',
        acknowledged: false,
      };
      const cakeHistoryRow = await db.insert(layerInsert);
      const cakeRevisionTimeId = cakeHistoryRow.timeId;

      //Get layer revision TimeId
      const {
        ['carGeneralLayerHistory']: { _data: layerHistoryRows },
      } = await db.getHistory('carGeneralLayer');
      const layerRevisionTimeId = layerHistoryRows[0].timeId;

      //Build route with TimeIds
      const route = `/carCake@${cakeRevisionTimeId}/carGeneralLayer@${layerRevisionTimeId}/carGeneral`;

      //Get all Volkswagens in example data
      const where = {
        carGeneralLayer: {
          carGeneral: { brand: 'Volkswagen' } as Partial<CarGeneral>,
        },
      };

      const result = await db.get(Route.fromFlat(route), where);

      expect(result).toBeDefined();
      expect(result[0].carCake).toBeDefined();
      expect(result[0].carCake._data.length).toBe(1);
      expect(result[0].carCake._data[0]._hash).toBe(cakeHistoryRow.carCakeRef);

      expect(result[0].carGeneralLayer).toBeDefined();
      expect(result[0].carGeneralLayer._data.length).toBe(1);

      expect(result[0].carGeneral).toBeDefined();
      expect(result[0].carGeneral._data.length).toBe(2);
      expect(result[0].carGeneral._data[0].brand).toBe('Volkswagen');
      expect(result[0].carGeneral._data[1].brand).toBe('Volkswagen');
    });
  });
  describe('insert', () => {
    it('throws on invalid Insert', async () => {
      //Mismatched route/value depth
      await expect(
        db.insert({
          route: '/carCake/carGeneralLayer/carGeneral',
          command: 'add',
          value: { x: 1, y: 2 },
          origin: 'H45H',
          acknowledged: false,
        }),
      ).rejects.toThrow();
    });

    it('run Insert on component route', async () => {
      const Insert: Insert<CarGeneral> = {
        route: '/carGeneral',
        command: 'add',
        value: {
          _hash: '',
          brand: 'Porsche',
          doors: 2,
          type: 'Macan Electric',
        } as CarGeneral,
        origin: 'H45H',
        acknowledged: false,
      };

      const result = await db.insert(Insert);
      expect(result).toBeDefined();
      expect(result.timeId).toBeDefined();
      expect(result.carGeneralRef).toBeDefined();
      expect(result.route).toBe('/carGeneral');
      expect(result.origin).toBe('H45H');
    });

    it('run Insert on component route, w/ previous by Hash', async () => {
      //Add predecessor component to core db
      const previousTimeId = 'H45H:20240606T120000Z';
      await db.core.import({
        carGeneralHistory: {
          _type: 'history',
          _data: [
            {
              carGeneralRef: carsExample().carGeneral._data[0]._hash ?? '',
              timeId: previousTimeId,
              route: '/carGeneral',
            } as HistoryRow<'CarGeneral'>,
          ],
        } as History<'CarGeneral'>,
      });

      //Create Insert with predecessor ref in route
      //by hash
      const previousHash = carsExample().carGeneral._data[0]._hash ?? '';
      const route = ['/carGeneral', previousHash].join('@');
      const Insert: Insert<CarGeneral> = {
        route,
        command: 'add',
        value: {
          _hash: '',
          brand: 'Porsche',
          doors: 2,
          type: 'Macan Electric',
        } as CarGeneral,
        origin: 'H45H',
        acknowledged: false,
      };

      const result = await db.insert(Insert);
      expect(result).toBeDefined();
      expect(result.timeId).toBeDefined();
      expect(result.carGeneralRef).toBeDefined();
      expect(result.route).toBe(route);
      expect(result.origin).toBe('H45H');
      expect(result.previous).toEqual([previousTimeId]);
    });

    it('run Insert on component route, w/ previous by TimeID', async () => {
      //Add predecessor component to core db
      const previousTimeId = 'H45H:20240606T120000Z';
      await db.core.import({
        carGeneralHistory: {
          _type: 'history',
          _data: [
            {
              carGeneralRef: carsExample().carGeneral._data[0]._hash ?? '',
              timeId: previousTimeId,
              route: '/carGeneral',
            } as HistoryRow<'CarGeneral'>,
          ],
        } as History<'CarGeneral'>,
      });

      //Create Insert with predecessor ref in route
      //by timeId
      const route = ['/carGeneral', previousTimeId].join('@');
      const Insert: Insert<CarGeneral> = {
        route,
        command: 'add',
        value: {
          _hash: '',
          brand: 'Porsche',
          doors: 2,
          type: 'Macan Electric',
        } as CarGeneral,
        origin: 'H45H',
        acknowledged: false,
      };

      const result = await db.insert(Insert);
      expect(result).toBeDefined();
      expect(result.timeId).toBeDefined();
      expect(result.carGeneralRef).toBeDefined();
      expect(result.route).toBe(route);
      expect(result.origin).toBe('H45H');
      expect(result.previous).toEqual([previousTimeId]);
    });

    it('run Insert on layer route', async () => {
      const Insert: Insert<Json> = {
        route: '/carGeneralLayer',
        command: 'add',
        value: {
          VIN1: carsExample().carGeneral._data[1]._hash || '',
          VIN2: carsExample().carGeneral._data[0]._hash || '',
          _hash: '',
        } as Json,
        origin: 'H45H',
        acknowledged: false,
      };

      const result = await db.insert(Insert);
      expect(result).toBeDefined();
      expect(result.timeId).toBeDefined();
      expect(result.carGeneralLayerRef).toBeDefined();
      expect(result.route).toBe('/carGeneralLayer');
      expect(result.origin).toBe('H45H');
    });

    it('run Insert on cake route', async () => {
      const carCake = carsExample().carCake._data[0];
      const Insert: Insert<Record<string, LayerRef>> = {
        route: '/carCake',
        command: 'add',
        value: {
          ...rmhsh(carCake.layers),
          ...{ carGeneralLayer: 'NEWHASH' },
        },
        origin: 'H45H',
        acknowledged: false,
      };

      const result = await db.insert(Insert);

      expect(result).toBeDefined();
      expect(result.timeId).toBeDefined();
      expect(result.carCakeRef).toBeDefined();
      expect(result.route).toBe('/carCake');
      expect(result.origin).toBe('H45H');
    });

    it('run Insert on nested: component/component', async () => {
      const Insert: Insert<Json> = {
        route: '/carTechnical/carDimensions',
        command: 'add',
        value: {
          engine: 'Electric',
          transmission: 'Automatic',
          gears: 1,
          dimensions: {
            height: 1600,
            width: 2000,
            length: 4700,
          },
        } as Json,
        origin: 'H45H',
        acknowledged: false,
      };

      const result = await db.insert(Insert);
      expect(result).toBeDefined();
      expect(result.timeId).toBeDefined();
      expect(result.carTechnicalRef).toBeDefined();
      expect(result.route).toBe('/carTechnical/carDimensions');
      expect(result.origin).toBe('H45H');

      const writtenRow = await db.core.readRow(
        'carTechnical',
        result.carTechnicalRef as string,
      );
      expect(writtenRow).toBeDefined();
      expect(writtenRow?.carTechnical?._data.length).toBe(1);
      const writtenComponent = writtenRow?.carTechnical?._data[0] as Json;
      expect(writtenComponent.engine).toBe('Electric');
      expect(writtenComponent.transmission).toBe('Automatic');
      expect(writtenComponent.gears).toBe(1);
      expect(writtenComponent.dimensions).toBeDefined();

      const writtenDimensionRow = await db.core.readRow(
        'carDimensions',
        writtenComponent.dimensions as string,
      );
      expect(writtenDimensionRow).toBeDefined();
      expect(writtenDimensionRow?.carDimensions?._data.length).toBe(1);
      const writtenDimension = writtenDimensionRow?.carDimensions
        ?._data[0] as CarGeneral;
      expect(writtenDimension.height).toBe(1600);
      expect(writtenDimension.width).toBe(2000);
      expect(writtenDimension.length).toBe(4700);
    });

    it('run Insert on nested: layer/component', async () => {
      const Insert: Insert<Json> = {
        route: '/carGeneralLayer/carGeneral',
        command: 'add',
        value: {
          /*TODO: Validate value structure*/
          VIN3: {
            brand: 'Porsche',
            doors: 4,
            type: 'Cayenne E-Hybrid',
          } as CarGeneral,
          VIN4: {
            brand: 'Mercedes Benz',
            doors: 4,
            type: 'EQC 400 4MATIC',
          } as CarGeneral,
        } as Record<string, CarGeneral>,
        origin: 'H45H',
        acknowledged: false,
      };

      const result = await db.insert(Insert);
      expect(result).toBeDefined();
      expect(result.timeId).toBeDefined();
      expect(result.carGeneralLayerRef).toBeDefined();
      expect(result.route).toBe('/carGeneralLayer/carGeneral');
      expect(result.origin).toBe('H45H');

      //Check if new layer is in db
      const { carGeneralLayer: layerTable } = await db.core.dumpTable(
        'carGeneralLayer',
      );
      const newLayer = layerTable._data.find(
        (l) => l._hash === result.carGeneralLayerRef,
      );
      expect(newLayer).toBeDefined();

      //Check if new components are in db
      const { carGeneral: componentTable } = await db.core.dumpTable(
        'carGeneral',
      );
      const newComponents = componentTable._data.filter(
        (c) =>
          c._hash === (newLayer as any).add.VIN3 ||
          c._hash === (newLayer as any).add.VIN4,
      );
      expect(newComponents.length).toBe(2);
    });

    it('run Insert on nested: cake/layer/component', async () => {
      const Insert: Insert<Json> = {
        route: '/carCake/carGeneralLayer/carGeneral',
        command: 'add',
        value: {
          carGeneralLayer: {
            VIN5: {
              brand: 'Porsche',
              doors: 2,
              type: '911 Carrera 4S',
            } as CarGeneral,
            VIN6: {
              brand: 'Mercedes Benz',
              doors: 4,
              type: 'EQE 350+',
            } as CarGeneral,
          } as Record<string, CarGeneral>,
        } as Record<string, Record<string, CarGeneral>>,
        origin: 'H45H',
        acknowledged: false,
      };

      const result = await db.insert(Insert);
      expect(result).toBeDefined();
      expect(result.timeId).toBeDefined();
      expect(result.carCakeRef).toBeDefined();
      expect(result.route).toBe('/carCake/carGeneralLayer/carGeneral');
      expect(result.origin).toBe('H45H');

      //Check if new cake is in db
      const { carCake: cakeTable } = await db.core.dumpTable('carCake');
      const newCake = cakeTable._data.find(
        (c) => c._hash === result.carCakeRef,
      );
      expect(newCake).toBeDefined();

      //Check if new layer is in db
      const { carGeneralLayer: layerTable } = await db.core.dumpTable(
        'carGeneralLayer',
      );
      const newLayer = layerTable._data.find(
        (l) => l._hash === (newCake as any).layers.carGeneralLayer,
      );
      expect(newLayer).toBeDefined();

      //Check if new components are in db
      const { carGeneral: componentTable } = await db.core.dumpTable(
        'carGeneral',
      );
      const newComponents = componentTable._data.filter(
        (c) =>
          c._hash ===
            (newLayer as any).add.VIN5 /*TODO: Fix typing for layers*/ ||
          c._hash === (newLayer as any).add.VIN6,
      );
      expect(newComponents.length).toBe(2); //Only checking first layer components
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

    it('notify on component route', async () => {
      const callback = vi.fn();
      const route = Route.fromFlat('/carGeneral');

      db.registerObserver(route, callback);

      const Insert: Insert<CarGeneral> = {
        route: '/carGeneral',
        command: 'add',
        value: {
          _hash: '',
          brand: 'Porsche',
          doors: 2,
          type: 'Macan Electric',
        } as CarGeneral,
        origin: 'H45H',
        acknowledged: false,
      };

      const result = await db.insert(Insert);

      expect(callback).toHaveBeenCalledTimes(1);
      expect(callback).toHaveBeenCalledWith(result);
    });
    it('notify several times on component route', async () => {
      const callback = vi.fn();
      const route = Route.fromFlat('/carGeneral');

      db.registerObserver(route, callback);

      const Insert: Insert<CarGeneral> = {
        route: '/carGeneral',
        command: 'add',
        value: {
          _hash: '',
          brand: 'Porsche',
          doors: 2,
          type: 'Macan Electric',
        } as CarGeneral,
        origin: 'H45H',
        acknowledged: false,
      };

      const Insert2: Insert<CarGeneral> = {
        route: '/carGeneral',
        command: 'add',
        value: {
          _hash: '',
          brand: 'Porsche',
          doors: 4,
          type: 'Cayenne E-Hybrid',
        } as CarGeneral,
        origin: 'H45H',
        acknowledged: false,
      };

      const result = await db.insert(Insert);
      const result2 = await db.insert(Insert2);

      expect(callback).toHaveBeenCalledTimes(2);
      expect(callback).toHaveBeenNthCalledWith(1, result);
      expect(callback).toHaveBeenNthCalledWith(2, result2);
    });

    it('notify on nested component/component route', async () => {
      const callback = vi.fn();
      const route = Route.fromFlat('/carTechnical/carDimensions');

      db.registerObserver(route, callback);

      const Insert: Insert<Json> = {
        route: '/carTechnical/carDimensions',
        command: 'add',
        value: {
          engine: 'Electric',
          transmission: 'Automatic',
          gears: 1,
          dimensions: {
            height: 1600,
            width: 2000,
            length: 4700,
          },
        } as Json,
        origin: 'H45H',
        acknowledged: false,
      };

      await db.insert(Insert);

      //Get written history rows
      const {
        ['carTechnicalHistory']: { _data: carTechnicalHistory },
      } = await db.getHistory('carTechnical');
      const carTechnicalHistoryRow = rmhsh(carTechnicalHistory[0]);

      const {
        ['carDimensionsHistory']: { _data: carDimensionsHistory },
      } = await db.getHistory('carDimensions');
      const carDimensionsHistoryRow = rmhsh(carDimensionsHistory[0]);

      expect(callback).toHaveBeenCalledTimes(2);
      expect(callback).toHaveBeenNthCalledWith(2, carTechnicalHistoryRow);
      expect(callback).toHaveBeenNthCalledWith(1, carDimensionsHistoryRow);
    });
    it('notify on nested cake/layer/component route', async () => {
      const callback = vi.fn();
      const route = Route.fromFlat('/carCake/carGeneralLayer/carGeneral');

      db.registerObserver(route, callback);

      const Insert: Insert<Json> = {
        route: '/carCake/carGeneralLayer/carGeneral',
        command: 'add',
        value: {
          carGeneralLayer: {
            VIN5: {
              brand: 'Porsche',
              doors: 2,
              type: '911 Carrera 4S',
            } as CarGeneral,
            VIN6: {
              brand: 'Mercedes Benz',
              doors: 4,
              type: 'EQE 350+',
            } as CarGeneral,
          } as Record<string, CarGeneral>,
        } as Record<string, Record<string, CarGeneral>>,
        origin: 'H45H',
        acknowledged: false,
      };

      await db.insert(Insert);

      //Get written history rows
      const {
        ['carCakeHistory']: { _data: carCakeHistory },
      } = await db.getHistory('carCake');
      const carCakeHistoryRow = rmhsh(carCakeHistory[0]);

      const {
        ['carGeneralLayerHistory']: { _data: carGeneralLayerHistory },
      } = await db.getHistory('carGeneralLayer');
      const carGeneralLayerHistoryRow = rmhsh(carGeneralLayerHistory[0]);

      const {
        ['carGeneralHistory']: { _data: carGeneralHistory },
      } = await db.getHistory('carGeneral');
      const carGeneralHistoryRow1 = rmhsh(carGeneralHistory[0]);
      const carGeneralHistoryRow2 = rmhsh(carGeneralHistory[1]);

      expect(callback).toHaveBeenCalledTimes(4);
      expect(callback).toHaveBeenNthCalledWith(4, carCakeHistoryRow);
      expect(callback).toHaveBeenNthCalledWith(3, carGeneralLayerHistoryRow);
      //Order of component History not guaranteed
      expect([2, 1]).toContain(
        (callback.mock.calls[1][0] as HistoryRow<'CarGeneral'>).timeId ===
          carGeneralHistoryRow1.timeId
          ? 2
          : 1,
      );
      expect([2, 1]).toContain(
        (callback.mock.calls[1][0] as HistoryRow<'CarGeneral'>).timeId ===
          carGeneralHistoryRow2.timeId
          ? 2
          : 1,
      );
    });
    it('skips notification on component route', async () => {
      const callback = vi.fn();
      const route = Route.fromFlat('/carGeneral');

      db.registerObserver(route, callback);

      const Insert: Insert<CarGeneral> = {
        route: '/carGeneral',
        command: 'add',
        value: {
          _hash: '',
          brand: 'Porsche',
          doors: 2,
          type: 'Macan Electric',
        } as CarGeneral,
        origin: 'H45H',
        acknowledged: false,
      };

      await db.insert(Insert, { skipNotification: true });

      expect(callback).toHaveBeenCalledTimes(0);
    });
  });
  describe('join', () => {
    const cakeKey = 'carCake';
    const cakeRef = carsExample().carCake._data[0]._hash as string;

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
      const exampleData = carsExample().carGeneral._data.map((c) => [c.brand]);

      expect(result.rows).toEqual(exampleData);
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
        carsExample().carSliceId as SliceIdsTable
      )._data.flatMap((d) => d.add);

      // Get technical layer values to map slice IDs to component refs
      const carTechnicalLayerValues = (
        carsExample().carTechnicalLayer as LayersTable
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
          const comp = carsExample().carTechnical._data.find(
            (c) => c._hash === compRef,
          );
          if (comp && comp.repairedByWorkshop !== undefined) {
            // Value exists
            exampleData.push([comp.repairedByWorkshop]);
            continue;
          }
          exampleData.push([null]); // Value missing in component
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

      // Build expected data for validation

      const exampleData = [];
      // Get slice IDs from carSliceIds layer
      const carSliceIds = (
        carsExample().carSliceId as SliceIdsTable
      )._data.flatMap((d) => d.add);

      // Get general layer values to map slice IDs to general component refs
      const carGeneralLayerValues = (
        carsExample().carGeneralLayer as LayersTable
      )._data.flatMap((l) =>
        Object.entries(l.add).map(([sliceId, compRef]) => ({
          sliceId,
          compRef,
        })),
      );

      // Get technical layer values to map slice IDs to technical component refs
      const carTechnicalLayerValues = (
        carsExample().carTechnicalLayer as LayersTable
      )._data.flatMap((l) =>
        Object.entries(l.add).map(([sliceId, compRef]) => ({
          sliceId,
          compRef,
        })),
      );

      // Map slice IDs to isElectric and transmission values
      for (const carSliceId of carSliceIds) {
        // General Component
        const generalLayerEntry = carGeneralLayerValues.find(
          (l) => l.sliceId === carSliceId,
        );
        let isElectricValue: boolean | null = null;
        if (generalLayerEntry) {
          const generalCompRef = generalLayerEntry.compRef;
          const generalComp = carsExample().carGeneral._data.find(
            (c) => c._hash === generalCompRef,
          );
          if (generalComp && generalComp.isElectric !== undefined) {
            isElectricValue = generalComp.isElectric;
          }
        }

        // Technical Component
        const technicalLayerEntry = carTechnicalLayerValues.find(
          (l) => l.sliceId === carSliceId,
        );
        let transmissionValue: string | null = null;
        if (technicalLayerEntry) {
          const technicalCompRef = technicalLayerEntry.compRef;
          const technicalComp = carsExample().carTechnical._data.find(
            (c) => c._hash === technicalCompRef,
          );
          if (technicalComp && technicalComp.transmission !== undefined) {
            transmissionValue = (technicalComp.transmission as string) ?? null;
          }
        }

        exampleData.push([isElectricValue, transmissionValue]);
      }

      // Validate joined data
      expect(result.rows).toEqual(exampleData);
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
      ).rejects.toThrow(
        'Db.join: Cake with ref "MISSING_CAKE_REF" not found in cake table "carCake".',
      );
    });
  });
  describe('getColumnSelectionFromEdit', () => {
    it('should be defined', () => {
      expect(db.getColumnSelectionFromEdit).toBeDefined();
    });

    it('returns correct column selection for example edit', async () => {
      const exampleEdit = exampleEditCarsExample();
      const columnSelection = await db.getColumnSelectionFromEdit(exampleEdit);

      expect(columnSelection.routes).toEqual([
        'carCake/carGeneralLayer/carGeneral/isElectric',
        'carCake/carTechnicalLayer/carTechnical/transmission',
      ]);
    });

    it('throws error if filter columns doesnt match tableCfg', async () => {
      const brokenEdit: Edit = hip<Edit>({
        name: 'Broken Edit',
        filter: {
          columnFilters: [
            {
              type: 'string',
              column: 'carCake/carGeneralLayer/carGeneral/missingColumn',
              operator: 'endsWith',
              search: 'o',
              _hash: '',
            },
          ],
          operator: 'and',
          _hash: '',
        },
        actions: [],
        _hash: '',
      });

      await expect(db.getColumnSelectionFromEdit(brokenEdit)).rejects.toThrow(
        'Db.getColumnSelection: Column "missingColumn" not found in table "carGeneral".',
      );
    });
  });
});
