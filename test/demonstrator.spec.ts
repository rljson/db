// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { rmhsh } from '@rljson/hash';
import { IoMem } from '@rljson/io';
import { TableCfg } from '@rljson/rljson';

import { readFileSync } from 'node:fs';
import { beforeAll, describe, it } from 'vitest';

import { Db } from '../src/db';
import { example } from '../src/example';

import { checkGoldens } from './setup/goldens';

describe('Demonstrator', () => {
  let io: IoMem;
  let db: Db;

  beforeAll(async () => {
    const catalogFs = readFileSync(
      'src/example-converted/catalog.rljson.json',
      'utf-8',
    );
    const catalogData = JSON.parse(catalogFs);

    const catalogTableCfgs = { ...catalogData.tableCfgs }._data as TableCfg[];

    delete catalogData.tableCfgs;

    //Init io
    io = new IoMem();
    await io.init();
    await io.isReady();

    //Init Core
    db = new Db(io);

    //Initialize Catalog
    for (const tableCfg of catalogTableCfgs) {
      await db.core.createTable(tableCfg);
    }

    //Import Data
    await db.core.import(rmhsh(catalogData));
  });
  it('should run without error', async () => {
    // Prepare logging
    const backup = console.log;
    const messages: string[] = [];
    console.log = (m: string) => messages.push(m);

    // Run example
    await example();

    // Compare log with golden
    checkGoldens('test/goldens/example.log', messages.join('\n'));

    // Restore logging
    console.log = backup;
  });
});
