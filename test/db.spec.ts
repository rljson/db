// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { rmhsh } from '@rljson/hash';
import { IoMem } from '@rljson/io';
import { Json, JsonValue } from '@rljson/json';
import { History, HistoryRow, Insert, LayerRef, Route } from '@rljson/rljson';

import { beforeEach, describe, expect, it, vi } from 'vitest';

import { CarGeneral, carsExample } from '../src/cars-example';
import { Db } from '../src/db';

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

      const result = await db.get(Route.fromFlat(route), ref);
      expect(result).toBeDefined();
      expect(result[0].carCake).toBeDefined();
      expect(result[0].carCake._data.length).toBe(1);
      expect(result[0].carCake._data[0]._hash).toBe(ref);
    });
    it('get nested layer/component by where', async () => {
      const route = '/carGeneralLayer/carGeneral';
      const where = rmhsh(carsExample().carGeneral._data[0]) as {
        [column: string]: JsonValue;
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
    it('get nested layer/component by hash w/ revision', async () => {
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

      const addedLayerHistory = (await db.run(
        Insert,
      )) as HistoryRow<'CarGeneralLayer'>;

      //Search for first carGeneral
      const where = carsExample().carGeneral._data[0]._hash ?? '';

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
      expect(result1[0].carGeneral._data[0]._hash).toBe(where);

      expect(result2[0].carGeneral).toBeDefined();
      expect(result2[0].carGeneral._data.length).toBe(1);
      expect(result2[0].carGeneral._data[0]._hash).toBe(where);

      expect(result1[0].carGeneralLayer).toBeDefined();
      expect(result1[0].carGeneralLayer._data.length).toBe(1);
      expect(result1[0].carGeneralLayer._data[0]._hash).toBe(layerRevHash1);

      expect(result2[0].carGeneralLayer).toBeDefined();
      expect(result2[0].carGeneralLayer._data.length).toBe(1);
      expect(result2[0].carGeneralLayer._data[0]._hash).toBe(layerRevHash2);
    });
    it('get nested component/component by where', async () => {
      const route = '/carTechnical/carDimensions';
      const where = rmhsh(carsExample().carDimensions._data[0]) as {
        [column: string]: JsonValue;
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
      const where = rmhsh(carsExample().carGeneral._data[0]) as {
        [column: string]: JsonValue;
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

      const where = carsExample().carGeneral._data[0]._hash ?? '';
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
      expect(result[0].carGeneral._data[0]._hash).toBe(where);
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
      const cakeHistoryRow = await db.run(layerInsert);
      const cakeRevisionTimeId = cakeHistoryRow.timeId;

      //Get layer revision TimeId
      const {
        ['carGeneralLayerHistory']: { _data: layerHistoryRows },
      } = await db.getHistory('carGeneralLayer');
      const layerRevisionTimeId = layerHistoryRows[0].timeId;

      //Build route with TimeIds
      const route = `/carCake@${cakeRevisionTimeId}/carGeneralLayer@${layerRevisionTimeId}/carGeneral`;

      //Get all Volkswagens in example data
      const where = { brand: 'Volkswagen' } as Partial<CarGeneral>;

      const result = await db.get(Route.fromFlat(route), where);

      expect(result).toBeDefined();
      expect(result[0].carCake).toBeDefined();
      expect(result[0].carCake._data.length).toBe(1);
      expect(result[0].carCake._data[0]._hash).toBe(cakeHistoryRow.carCakeRef);

      expect(result[0].carGeneralLayer).toBeDefined();
      expect(result[0].carGeneralLayer._data.length).toBe(1);

      expect(result[0].carGeneral).toBeDefined();
      expect(result[0].carGeneral._data.length).toBe(2); //2 Volkswagens in example data
      expect(result[0].carGeneral._data[0].brand).toBe('Volkswagen');
      expect(result[0].carGeneral._data[1].brand).toBe('Volkswagen');

      expect(result[1].carCake).toBeDefined();
      expect(result[1].carCake._data.length).toBe(1);
      expect(result[1].carCake._data[0]._hash).toBe(cakeHistoryRow.carCakeRef);

      expect(result[1].carGeneralLayer).toBeDefined();
      expect(result[1].carGeneralLayer._data.length).toBe(1);

      expect(result[1].carGeneral).toBeDefined();
      expect(result[1].carGeneral._data.length).toBe(2); //2 Volkswagens in example data
      expect(result[1].carGeneral._data[0].brand).toBe('Volkswagen');
      expect(result[1].carGeneral._data[1].brand).toBe('Volkswagen');
    });
  });

  describe('run', () => {
    it('throws on invalid Insert', async () => {
      //Mismatched route/value depth
      await expect(
        db.run({
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

      const result = await db.run(Insert);
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

      const result = await db.run(Insert);
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

      const result = await db.run(Insert);
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

      const result = await db.run(Insert);
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

      const result = await db.run(Insert);

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
          carDimensionsRef: {
            height: 1600,
            width: 2000,
            length: 4700,
          },
        } as Json,
        origin: 'H45H',
        acknowledged: false,
      };

      const result = await db.run(Insert);
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
      expect(writtenComponent.carDimensionsRef).toBeDefined();

      const writtenDimensionRow = await db.core.readRow(
        'carDimensions',
        writtenComponent.carDimensionsRef as string,
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

      const result = await db.run(Insert);
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

      const result = await db.run(Insert);
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

      const result = await db.run(Insert);

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

      const result = await db.run(Insert);
      const result2 = await db.run(Insert2);

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
          carDimensionsRef: {
            height: 1600,
            width: 2000,
            length: 4700,
          },
        } as Json,
        origin: 'H45H',
        acknowledged: false,
      };

      await db.run(Insert);

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

      await db.run(Insert);

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

      await db.run(Insert, { skipNotification: true });

      expect(callback).toHaveBeenCalledTimes(0);
    });
  });
});
