// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { rmhsh } from '@rljson/hash';
import { IoMem } from '@rljson/io';
import { Json, JsonValue } from '@rljson/json';
import {
  Edit,
  EditProtocol,
  EditProtocolRow,
  LayerRef,
  Route,
} from '@rljson/rljson';

import { beforeEach, describe, expect, it } from 'vitest';

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
      await db.core.createEditable(tableCfg);
    }

    //Import Data
    await db.core.import(carsExample());
  });

  describe('core', () => {
    it('should be defined', () => {
      expect(db.core).toBeDefined();
    });
    describe('get', () => {
      it('component by ref', async () => {
        const route = '/carGeneral';
        const ref = carsExample().carGeneral._data[0]._hash ?? '';

        const result = await db.get(Route.fromFlat(route), ref);
        expect(result).toBeDefined();
        expect(result[0].carGeneral).toBeDefined();
        expect(result[0].carGeneral._data.length).toBe(1);
        expect(result[0].carGeneral._data[0]._hash).toBe(ref);
      });
      it('layer by ref', async () => {
        const route = '/carGeneralLayer';
        const ref =
          (carsExample().carGeneralLayer._data[0]._hash as string) ?? '';

        const result = await db.get(Route.fromFlat(route), ref);
        expect(result).toBeDefined();
        expect(result[0].carGeneralLayer).toBeDefined();
        expect(result[0].carGeneralLayer._data.length).toBe(1);
        expect(result[0].carGeneralLayer._data[0]._hash).toBe(ref);
      });
      it('cake by ref', async () => {
        const route = '/carCake';
        const ref = (carsExample().carCake._data[0]._hash as string) ?? '';

        const result = await db.get(Route.fromFlat(route), ref);
        expect(result).toBeDefined();
        expect(result[0].carCake).toBeDefined();
        expect(result[0].carCake._data.length).toBe(1);
        expect(result[0].carCake._data[0]._hash).toBe(ref);
      });
      it('component by where', async () => {
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
      it('nested layer/component by where', async () => {
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
      it('nested component/component by where', async () => {
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
      it('nested cake/layer/component by where', async () => {
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
    });

    describe('run', () => {
      it('run edit on component route', async () => {
        const edit: Edit<CarGeneral> = {
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

        const result = await db.run(edit);
        expect(result).toBeDefined();
        expect(result.timeId).toBeDefined();
        expect(result.carGeneralRef).toBeDefined();
        expect(result.route).toBe('/carGeneral');
        expect(result.origin).toBe('H45H');
      });

      it('run edit on component route, w/ previous by Hash', async () => {
        //Add predecessor component to core db
        const previousTimeId = 'H45H:20240606T120000Z';
        await db.core.import({
          carGeneralEdits: {
            _type: 'edits',
            _data: [
              {
                carGeneralRef: carsExample().carGeneral._data[0]._hash ?? '',
                timeId: previousTimeId,
                route: '/carGeneral',
              } as EditProtocolRow<'CarGeneral'>,
            ],
          } as EditProtocol<'CarGeneral'>,
        });

        //Create edit with predecessor ref in route
        //by hash
        const previousHash = carsExample().carGeneral._data[0]._hash ?? '';
        const route = ['/carGeneral', previousHash].join('@');
        const edit: Edit<CarGeneral> = {
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

        const result = await db.run(edit);
        expect(result).toBeDefined();
        expect(result.timeId).toBeDefined();
        expect(result.carGeneralRef).toBeDefined();
        expect(result.route).toBe(route);
        expect(result.origin).toBe('H45H');
        expect(result.previous).toEqual([previousTimeId]);
      });

      it('run edit on component route, w/ previous by TimeID', async () => {
        //Add predecessor component to core db
        const previousTimeId = 'H45H:20240606T120000Z';
        await db.core.import({
          carGeneralEdits: {
            _type: 'edits',
            _data: [
              {
                carGeneralRef: carsExample().carGeneral._data[0]._hash ?? '',
                timeId: previousTimeId,
                route: '/carGeneral',
              } as EditProtocolRow<'CarGeneral'>,
            ],
          } as EditProtocol<'CarGeneral'>,
        });

        //Create edit with predecessor ref in route
        //by timeId
        const route = ['/carGeneral', previousTimeId].join('@');
        const edit: Edit<CarGeneral> = {
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

        const result = await db.run(edit);
        expect(result).toBeDefined();
        expect(result.timeId).toBeDefined();
        expect(result.carGeneralRef).toBeDefined();
        expect(result.route).toBe(route);
        expect(result.origin).toBe('H45H');
        expect(result.previous).toEqual([previousTimeId]);
      });

      it('run edit on layer route', async () => {
        const edit: Edit<Json> = {
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

        const result = await db.run(edit);
        expect(result).toBeDefined();
        expect(result.timeId).toBeDefined();
        expect(result.carGeneralLayerRef).toBeDefined();
        expect(result.route).toBe('/carGeneralLayer');
        expect(result.origin).toBe('H45H');
      });

      it('run edit on cake route', async () => {
        const carCake = carsExample().carCake._data[0];
        const edit: Edit<Record<string, LayerRef>> = {
          route: '/carCake',
          command: 'add',
          value: {
            ...rmhsh(carCake.layers),
            ...{ carGeneralLayer: 'NEWHASH' },
          },
          origin: 'H45H',
          acknowledged: false,
        };

        const result = await db.run(edit);

        expect(result).toBeDefined();
        expect(result.timeId).toBeDefined();
        expect(result.carCakeRef).toBeDefined();
        expect(result.route).toBe('/carCake');
        expect(result.origin).toBe('H45H');
      });

      it('run edit on nested: component/component', async () => {
        const edit: Edit<Json> = {
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

        const result = await db.run(edit);
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

      it('run edit on nested: layer/component', async () => {
        const edit: Edit<Json> = {
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

        const result = await db.run(edit);
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

      it('run edit on nested: cake/layer/component', async () => {
        const edit: Edit<Json> = {
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

        const result = await db.run(edit);
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
  });
});
