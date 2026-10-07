// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { IoMem } from '@rljson/io';
import { Route } from '@rljson/rljson';

import { beforeEach, describe, expect, it } from 'vitest';

import { Db } from '../../src/db';
import { staticExample } from '../../src/example-static/example-static';
import { RowFilter } from '../../src/join/filter/row-filter';
import { Join } from '../../src/join/join';
import { ColumnSelection } from '../../src/join/selection/column-selection';

describe('Join.setValue replaces earlier setValue on the same cell', () => {
  let db: Db;
  let join: Join;

  const cakeKey = 'carCake';
  const cakeRef = staticExample().carCake._data[2]._hash as string;
  const brand = '/carCake/carGeneralLayer/carGeneral/brand';
  const type = '/carCake/carGeneralLayer/carGeneral/type';

  const brandFilter = (search: string): RowFilter => ({
    columnFilters: [
      {
        column: 'carCake/carGeneralLayer/carGeneral/brand',
        type: 'string',
        operator: 'equals',
        _hash: '',
        search,
      },
    ],
    operator: 'and',
    _hash: '',
  });

  const column = (j: Join, index: number) => j.rows.map((r) => r[index]);

  beforeEach(async () => {
    const io = new IoMem();
    await io.init();
    await io.isReady();
    db = new Db(io);
    for (const tableCfg of staticExample().tableCfgs._data) {
      await db.core.createTableWithInsertHistory(tableCfg);
    }
    await db.core.import(staticExample());

    const cs = ColumnSelection.exampleCarsColumnSelection();
    join = await db.join(
      new ColumnSelection([cs.columns[0], cs.columns[1]]),
      cakeKey,
      cakeRef,
    );
  });

  it('shows only the last value for the same cell', () => {
    const edited = join
      .setValue({ route: brand, value: 'Opel' })
      .setValue({ route: brand, value: 'Kia' });

    for (const value of column(edited, 0)) {
      expect(value).toEqual(['Kia']);
    }
  });

  it('keeps edits of other cells', () => {
    const edited = join
      .setValue({ route: brand, value: 'Opel' })
      .setValue({ route: type, value: 'Truck' })
      .setValue({ route: brand, value: 'Kia' });

    for (const row of edited.rows) {
      expect(row).toEqual([['Kia'], ['Truck']]);
    }
  });

  it('lets the later setValue win on overlapping rows', () => {
    const all = join.setValue({ route: type, value: 'A' });
    const bmw = all.filter(brandFilter('BMW')).setValue({
      route: type,
      value: 'B',
    });

    expect(column(bmw, 1)).toEqual([['B'], ['B']]);
    // Rows outside the overlap keep the earlier edit
    for (const value of column(all, 1)) {
      expect(value).toEqual(['A']);
    }
  });

  it('keeps earlier history states unchanged', () => {
    const first = join.setValue({ route: brand, value: 'Opel' });
    const second = first.setValue({ route: brand, value: 'Kia' });

    expect(Array.from(new Set(column(first, 0).flat()))).toEqual(['Opel']);
    expect(Array.from(new Set(column(second, 0).flat()))).toEqual(['Kia']);
    expect(column(join, 0).flat()).not.toContain('Opel');
    // Going back and forth again gives the same values
    expect(Array.from(new Set(column(first, 0).flat()))).toEqual(['Opel']);
  });

  it('publishes only the final value', async () => {
    const edited = join
      .select(new ColumnSelection([join.columnSelection.columns[0]]))
      .setValue({ route: brand, value: 'Opel' })
      .setValue({ route: brand, value: 'Kia' });

    const [insert] = edited.insert();
    const [inserted] = await db.insert(insert.route, insert.tree, {
      skipHistory: true,
    });
    const { rljson } = await db.get(
      Route.fromFlat(
        `/${cakeKey}@${inserted['carCakeRef']}/carGeneralLayer/carGeneral/brand`,
      ),
      {},
    );
    const brands = new Set(
      rljson['carGeneral']._data.map((d: any) => d['brand']),
    );
    expect(Array.from(brands)).toEqual(['Kia']);
  });
});
