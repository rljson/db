// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { IoMem } from '@rljson/io';

import { beforeEach, describe, it } from 'vitest';

import { carsExample } from '../../src/cars-example';
import { Db } from '../../src/db';
import { ColumnSelection } from '../../src/join/selection/column-selection';

describe('MultiEditProcessor', () => {
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
      await db.core.createTableWithInsertHistory(tableCfg);
    }

    //Import Data
    await db.core.import(carsExample());
  });

  describe('Constructor', () => {
    it('should be defined', async () => {});
  });
});
