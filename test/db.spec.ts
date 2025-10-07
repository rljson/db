// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { IoMem } from '@rljson/io';
import { Json } from '@rljson/json';
import { Edit } from '@rljson/rljson';

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

    describe('resolve', () => {
      it('resolves basic component route and value', async () => {
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
          previous: [],
          acknowledged: false,
        };

        const result = await db.resolve(edit);
        expect(result).toBeDefined();
        expect(result.timeId).toBeDefined();
        expect(result.carGeneralRef).toBeDefined();
        expect(result.route).toBe('/carGeneral');
        expect(result.origin).toBe('H45H');
        expect(result.previous).toEqual([]);
      });

      it('resolves basic layer route and value', async () => {
        const edit: Edit<Json> = {
          route: '/carGeneralLayer',
          command: 'add',
          value: {
            VIN1: carsExample().carGeneral._data[1]._hash || '',
            VIN2: carsExample().carGeneral._data[0]._hash || '',
            _hash: '',
          } as Json,
          origin: 'H45H',
          previous: [],
          acknowledged: false,
        };

        const result = await db.resolve(edit);
        expect(result).toBeDefined();
        expect(result.timeId).toBeDefined();
        expect(result.carGeneralLayerRef).toBeDefined();
        expect(result.route).toBe('/carGeneralLayer');
        expect(result.origin).toBe('H45H');
        expect(result.previous).toEqual([]);
      });

      it('resolves nested: layer/component', async () => {
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
          previous: [] /*TODO: Add previous on all levels*/,
          acknowledged: false,
        };

        const result = await db.resolve(edit);
        expect(result).toBeDefined();
        expect(result.timeId).toBeDefined();
        expect(result.carGeneralLayerRef).toBeDefined();
        expect(result.route).toBe('/carGeneralLayer/carGeneral');
        expect(result.origin).toBe('H45H');
        expect(result.previous).toEqual([]);

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

      it('resolves nested: cake/layer/component', async () => {
        const edit: Edit<Json> = {
          route: '/carCake/carGeneralLayer/carGeneral',
          command: 'add@rezIXkbWisvjRvYoyRAg0q',
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
          previous: [] /*TODO: Add previous on all levels*/,
          acknowledged: false,
        };

        const result = await db.resolve(edit);
        expect(result).toBeDefined();
        expect(result.timeId).toBeDefined();
        expect(result.carCakeRef).toBeDefined();
        expect(result.route).toBe('/carCake/carGeneralLayer/carGeneral');
        expect(result.origin).toBe('H45H');
        expect(result.previous).toEqual([]);

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
