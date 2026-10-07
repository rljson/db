// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { rmhsh } from '@rljson/hash';
import { IoMem } from '@rljson/io';
import { createTreesTableCfg, TableCfg } from '@rljson/rljson';

import { beforeEach, describe, expect, it } from 'vitest';

import { createController } from '../../src/controller/controller';
import { Db } from '../../src/db';
import { staticExample } from '../../src/example-static/example-static';

describe('Controller content type', () => {
  let db: Db;

  beforeEach(async () => {
    const io = new IoMem();
    await io.init();
    await io.isReady();
    db = new Db(io);
  });

  // Creates a table with the config of the example table "from",
  // but with a name that does not end with the type's suffix
  const createRenamed = async (from: string, key: string) => {
    const cfg = staticExample().tableCfgs._data.find(
      (c) => c.key === from,
    ) as TableCfg;
    await db.core.createTable(rmhsh({ ...cfg, key }) as TableCfg);
  };

  it('takes the type of a table from its config, not from its name', async () => {
    await createRenamed('carCake', 'catalogs');
    await createRenamed('carGeneralLayer', 'carPrices');
    await createRenamed('carSliceId', 'carIds');
    await db.core.createTable(
      rmhsh({ ...createTreesTableCfg('x'), key: 'scenes' }) as TableCfg,
    );

    for (const [type, key] of [
      ['cakes', 'catalogs'],
      ['layers', 'carPrices'],
      ['sliceIds', 'carIds'],
      ['trees', 'scenes'],
    ] as const) {
      const ctrl = await createController(type, db.core, key);
      expect(ctrl).toBeDefined();
    }
  });

  it('throws when the config has another type', async () => {
    await createRenamed('carGeneral', 'prices');
    for (const type of ['cakes', 'layers', 'sliceIds', 'trees'] as const) {
      await expect(createController(type, db.core, 'prices')).rejects.toThrow(
        `Table prices is not of type ${type}.`,
      );
    }
  });
});
