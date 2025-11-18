// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { rmhsh } from '@rljson/hash';
import { Io, IoMem } from '@rljson/io';
import { Cake, Route } from '@rljson/rljson';

import { beforeAll, describe, expect, it } from 'vitest';

import { Db } from '../src/db';
import { ExampleGenerator } from '../src/example-generator/example-generator';

describe('ExampleGenerator', () => {
  let db: Db;
  let io: Io;
  let ex: ExampleGenerator;
  beforeAll(async () => {
    //Init io
    io = new IoMem();
    await io.init();
    await io.isReady();

    //Init Db
    db = new Db(io);

    ex = new ExampleGenerator(db);
    await ex.init();
  });
  it.skip('should run without error', async () => {
    const cakeKey = 'carCake';
    const cakeRoute = Route.fromFlat(`/${cakeKey}`);

    // Get Cake
    const {
      [cakeKey]: { _data: cakes },
    } = await db.get(cakeRoute, {});

    expect(cakes.length).toBe(1);

    const cake = cakes[0] as Cake;
    expect(Object.keys(rmhsh(cake.layers))).toEqual([
      'carGeneralLayer',
      'carTechnicalLayer',
      'carColorLayer',
    ]);
  });
});
