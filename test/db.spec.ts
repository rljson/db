// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';
import { IoMem } from '@rljson/io';
import { JsonValueH } from '@rljson/json';
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
  });

  describe('edit', () => {
    describe('component edit', () => {
      it('basic edit', () => {
        const carGeneralEdit: Edit<CarGeneral> = hip<any>({
          type: 'components',
          value: {
            brand: 'Audi',
            type: 'A4',
            doors: 5,
          } as CarGeneral & JsonValueH,
          route: 'carGeneral',
          origin: '',
          acknowledged: false,
        } as Edit<CarGeneral>);

        db.core.run(carGeneralEdit);

        expect(db.core).toBeDefined();
      });
    });
  });
});
