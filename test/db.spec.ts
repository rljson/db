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

      it('resolves nested structures: layer/component', async () => {
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
      });
    });
  });
});
