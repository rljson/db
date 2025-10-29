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
import { Edit } from '../src/edit/edit/edit';

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

  describe('Edit', () => {
    it('Should Run Edits', async () => {
      const cakeKey = 'carCake';
      const cakeRef = carsExample().carCake._data[0]._hash as string;

      const edit: Edit = {
        name: 'Set all cars that are not electric to electric',
        filter: {
          columnFilters: [
            {
              type: 'boolean',
              column: `${cakeKey}/carGeneralLayer/carGeneral/isElectric`,
              operator: 'equals',
              search: false,
              _hash: '',
            },
            {
              type: 'string',
              column: `${cakeKey}/carTechnicalLayer/carTechnical/transmission`,
              operator: 'equals',
              search: 'Automatic',
              _hash: '',
            },
          ],
          operator: 'and',
          _hash: '',
        },
        actions: [
          {
            route: `${cakeKey}/carGeneralLayer/carGeneral/isElectric`,
            setValue: true,
          },
          {
            route: `${cakeKey}/carTechnicalLayer/carTechnical/gears`,
            setValue: 5,
          },
        ],
        _hash: '',
      };

      const insertHistoryRows = await db.saveEdit(edit, cakeKey, cakeRef);

      const result = await db.get(
        Route.fromFlat(`${cakeKey}/carGeneralLayer/carGeneral/`),
        {},
      );

      expect(insertHistoryRows).toBeDefined();
    });
  });
});
