// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';
import { IoMem } from '@rljson/io';
import { Route, TableCfg } from '@rljson/rljson';

import { describe, expect, it } from 'vitest';

import { Db } from '../../src/db';

describe('ComponentController with a self reference', () => {
  it('resolves the reference columns of a table referring to itself', async () => {
    const io = new IoMem();
    await io.init();
    await io.isReady();
    const db = new Db(io);

    const partsCfg = hip<TableCfg>({
      key: 'parts',
      type: 'components',
      isHead: false,
      isRoot: false,
      isShared: true,
      columns: [
        { key: '_hash', type: 'string', titleLong: 'Hash', titleShort: 'Hash' },
        { key: 'name', type: 'string', titleLong: 'Name', titleShort: 'Name' },
        {
          key: 'subPartRefs',
          type: 'jsonArray',
          titleLong: 'Sub parts',
          titleShort: 'Sub parts',
          ref: { tableKey: 'parts', type: 'components' },
        },
      ],
      _hash: '',
    });
    await db.core.createTableWithInsertHistory(partsCfg);

    const screw = hip({ name: 'Screw', subPartRefs: [], _hash: '' });
    const engine = hip({
      name: 'Engine',
      subPartRefs: [screw._hash],
      _hash: '',
    });
    await db.core.import({
      parts: { _type: 'components', _data: [screw, engine] },
    } as any);

    const { cell } = await db.get(Route.fromFlat('parts/name'), {});
    expect(cell.map((c) => c.value).sort()).toEqual(['Engine', 'Screw']);
  });
});
