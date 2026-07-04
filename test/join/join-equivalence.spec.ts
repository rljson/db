// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { IoMem } from '@rljson/io';
import { Json } from '@rljson/json';
import { Cake, Ref, Route, TableCfg } from '@rljson/rljson';

import { beforeEach, describe, expect, it } from 'vitest';

import { Cell, Db } from '../../src/db';
import { convertMassData } from '../../src/example-static/mass-data/convert-mass-data';
import { staticExample } from '../../src/example-static/example-static';
import {
  ColumnInfo,
  ColumnSelection,
} from '../../src/join/selection/column-selection';

/**
 * Equivalence guard for the route-group optimized Db.join:
 * every column container produced by join() must be deep-equal to the
 * container a direct per-column get() (the previous implementation)
 * returns.
 */
describe('Join equivalence', () => {
  let db: Db;

  const cakeKey = 'carCake';
  const cakeRef = staticExample().carCake._data[2]._hash as string;

  beforeEach(async () => {
    const io = new IoMem();
    await io.init();
    await io.isReady();

    db = new Db(io);
    for (const tableCfg of staticExample().tableCfgs._data) {
      await db.core.createTableWithInsertHistory(tableCfg);
    }
    await db.core.import(staticExample());
  });

  const comparableCells = (cells: Cell[]) =>
    cells.map((c) => ({
      routeFlat: c.route?.flat ?? null,
      routePropertyKey: c.route?.propertyKey ?? null,
      value: c.value,
      row: c.row,
      path: c.path,
    }));

  it('join columns equal direct per-column gets (incl. nested routes)', async () => {
    const columnSelection = ColumnSelection.exampleCarsColumnSelection();

    const join = await db.join(columnSelection, cakeKey, cakeRef);

    // Resolve slice ids the same way join does
    const cakeGet = await db.get(Route.fromFlat(`${cakeKey}@${cakeRef}`), {});
    const cake = cakeGet.rljson[cakeKey]._data[0] as Cake;
    const sliceIds = await (db as any)._resolveSliceIds(
      cake.sliceIdsTable,
      cake.sliceIdsRow,
    );

    expect(sliceIds.length).toBeGreaterThan(0);
    expect(join.rowIndices).toEqual(sliceIds);

    for (const sliceId of sliceIds) {
      const row = join.row(sliceId);
      expect(row.length).toBe(columnSelection.columns.length);

      for (let i = 0; i < columnSelection.columns.length; i++) {
        const columnInfo = columnSelection.columns[i];
        const columnRoute = Route.fromFlat(
          columnInfo.route,
        ).toRouteWithProperty();

        // Reference: the previous per-column implementation
        const reference = await db.get(columnRoute, cakeRef, undefined, [
          sliceId,
        ]);

        const joinColumn = row[i];
        expect(joinColumn.route.flat).toBe(columnRoute.flat);
        expect(joinColumn.value.rljson).toEqual(reference.rljson);
        expect(joinColumn.value.tree).toEqual(reference.tree);
        expect(comparableCells(joinColumn.value.cell)).toEqual(
          comparableCells(reference.cell),
        );
      }
    }
  });

  it('non-groupable columns (nested json property) use the direct path', async () => {
    // 'units' is a nested json property of carGeneral, not a table —
    // the route group is not groupable and must use the per-column path
    const columnSelection = new ColumnSelection([
      {
        key: 'brand',
        route: 'carCake/carGeneralLayer/carGeneral/brand',
        alias: 'brand',
        titleLong: 'Brand',
        titleShort: 'Brand',
        type: 'string',
        _hash: '',
      } as ColumnInfo,
      {
        key: 'energy',
        route: 'carCake/carGeneralLayer/carGeneral/units/energy',
        alias: 'energy',
        titleLong: 'Energy Unit',
        titleShort: 'Energy',
        type: 'string',
        _hash: '',
      } as ColumnInfo,
    ]);

    const join = await db.join(columnSelection, cakeKey, cakeRef);

    const cakeGet = await db.get(Route.fromFlat(`${cakeKey}@${cakeRef}`), {});
    const cake = cakeGet.rljson[cakeKey]._data[0] as Cake;
    const sliceIds: string[] = await (db as any)._resolveSliceIds(
      cake.sliceIdsTable,
      cake.sliceIdsRow,
    );

    for (const sliceId of sliceIds) {
      const row = join.row(sliceId);
      for (let i = 0; i < columnSelection.columns.length; i++) {
        const columnRoute = Route.fromFlat(
          columnSelection.columns[i].route,
        ).toRouteWithProperty();
        const reference = await db.get(columnRoute, cakeRef, undefined, [
          sliceId,
        ]);

        expect(row[i].value.rljson).toEqual(reference.rljson);
        expect(row[i].value.tree).toEqual(reference.tree);
        expect(comparableCells(row[i].value.cell)).toEqual(
          comparableCells(reference.cell),
        );
      }
    }
  });

  describe('_sliceGroupContainers shape guards', () => {
    const groupRoute = () =>
      Route.fromFlat('carCake/carGeneralLayer/carGeneral');

    const validBase = () => ({
      rljson: {
        carCake: { _type: 'cakes', _data: [{ _hash: 'C' }] },
        carGeneralLayer: {
          _type: 'layers',
          _data: [{ _hash: 'L', add: { S1: 'R1' } }],
        },
        carGeneral: { _type: 'components', _data: [{ _hash: 'R1', a: 1 }] },
      },
      tree: {
        carCake: {
          _type: 'cakes',
          _data: [
            {
              _hash: 'C',
              layers: {
                carGeneralLayer: {
                  _type: 'layers',
                  _data: [
                    {
                      _hash: 'L',
                      add: {
                        S1: {
                          carGeneral: {
                            _type: 'components',
                            _data: [{ _hash: 'R1', a: 1 }],
                          },
                        },
                      },
                    },
                  ],
                },
              },
            },
          ],
        },
      },
      cell: [
        {
          route: Route.fromFlat('carCake/carGeneralLayer/carGeneral'),
          value: null,
          row: { _hash: 'R1', a: 1 },
          path: [
            [
              'carCake',
              '_data',
              0,
              'layers',
              'carGeneralLayer',
              '_data',
              0,
              'add',
              'S1',
              'carGeneral',
              '_data',
              0,
            ],
          ],
        },
      ],
      controllers: {},
    });

    it('slices a valid standard shape', () => {
      const derived = (db as any)._sliceGroupContainers(
        validBase(),
        groupRoute(),
        'carCake',
        ['S1'],
      );
      expect(derived).not.toBeNull();
      expect(derived.get('S1').cell.length).toBe(1);
    });

    it('rejects multiple cake rows', () => {
      const base = validBase();
      base.rljson.carCake._data.push({ _hash: 'C2' });
      expect(
        (db as any)._sliceGroupContainers(base, groupRoute(), 'carCake', [
          'S1',
        ]),
      ).toBeNull();
    });

    it('rejects multiple layer rows', () => {
      const base = validBase();
      base.rljson.carGeneralLayer._data.push({ _hash: 'L2', add: {} });
      expect(
        (db as any)._sliceGroupContainers(base, groupRoute(), 'carCake', [
          'S1',
        ]),
      ).toBeNull();
    });

    it('rejects cells with multiple paths', () => {
      const base = validBase();
      base.cell[0].path.push(base.cell[0].path[0]);
      expect(
        (db as any)._sliceGroupContainers(base, groupRoute(), 'carCake', [
          'S1',
        ]),
      ).toBeNull();
    });

    it('rejects unexpected cell path shapes', () => {
      const base = validBase();
      (base.cell[0].path[0] as any)[3] = 'somethingElse';
      expect(
        (db as any)._sliceGroupContainers(base, groupRoute(), 'carCake', [
          'S1',
        ]),
      ).toBeNull();
    });

    it('rejects resolved slices with multiple leaf rows', () => {
      const base = validBase();
      (
        base.tree.carCake._data[0].layers.carGeneralLayer._data[0].add.S1
          .carGeneral._data as any[]
      ).push({ _hash: 'R2' });
      expect(
        (db as any)._sliceGroupContainers(base, groupRoute(), 'carCake', [
          'S1',
        ]),
      ).toBeNull();
    });

    it('skips slices without cells', () => {
      const base = validBase();
      // Second slice resolved in the tree but without any cell
      (base.tree.carCake._data[0].layers.carGeneralLayer._data[0].add as any)[
        'S2'
      ] = { carGeneral: { _type: 'components', _data: [{ _hash: 'R1' }] } };

      const derived = (db as any)._sliceGroupContainers(
        validBase(),
        groupRoute(),
        'carCake',
        ['S2'],
      );
      expect(derived.get('S2')).toBeUndefined();

      const derived2 = (db as any)._sliceGroupContainers(
        base,
        groupRoute(),
        'carCake',
        ['S2'],
      );
      expect(derived2.get('S2')).toBeUndefined();
    });

    it('rejects self-referencing table chains', () => {
      expect(
        (db as any)._sliceGroupContainers(
          validBase(),
          Route.fromFlat('carCake/carGeneralLayer/carCake'),
          'carCake',
          ['S1'],
        ),
      ).toBeNull();
    });

    it('rejects resolved slices without the expected leaf table', () => {
      const base = validBase();
      delete (
        base.tree.carCake._data[0].layers.carGeneralLayer._data[0].add.S1 as any
      ).carGeneral;
      expect(
        (db as any)._sliceGroupContainers(base, groupRoute(), 'carCake', [
          'S1',
        ]),
      ).toBeNull();
    });

    it('falls back to per-slice queries for short group routes', async () => {
      // A 2-segment group route skips the single-fetch fast path
      const containers = await (db as any)._joinGroupContainers(
        Route.fromFlat('carCake/carGeneralLayer'),
        'carCake',
        cakeRef,
        ['CAR1'],
      );
      expect(containers.size).toBe(1);
      expect(containers.get('CAR1')).toBeDefined();
    });

    it('buckets multiple cells of the same slice together', () => {
      const base = validBase();
      base.cell.push({ ...base.cell[0] });
      const derived = (db as any)._sliceGroupContainers(
        base,
        groupRoute(),
        'carCake',
        ['S1'],
      );
      expect(derived.get('S1').cell.length).toBe(2);
    });
  });

  it('mass-data join columns equal direct per-column gets', async () => {
    const io = new IoMem();
    await io.init();
    await io.isReady();
    const massDb = new Db(io);

    const converted = convertMassData();
    for (const tableCfg of converted.result.tableCfgs._data as TableCfg[]) {
      await massDb.core.createTable(tableCfg);
    }
    await massDb.core.import(converted.result);

    const col = (key: string, route: string): ColumnInfo =>
      ({
        key,
        route,
        alias: key,
        titleLong: key,
        titleShort: key,
        type: 'string',
        _hash: '',
      }) as ColumnInfo;

    const columnSelection = new ColumnSelection([
      col('brand', 'carCake/carGeneralLayer/carGeneral/brand'),
      col('engine', 'carCake/carTechnicalLayer/carTechnical/engine'),
      col(
        'height',
        'carCake/carDimensionsLayer/carDimensions/carHeight/height',
      ),
      col('sides', 'carCake/carColorLayer/carColor/sides'),
    ]);

    const cakeGet = await massDb.get(Route.fromFlat('/carCake'), {});
    const massCakeRef = (cakeGet.cell[0].row! as Json)._hash as Ref;

    const join = await massDb.join(columnSelection, 'carCake', massCakeRef);

    const cake = (
      await massDb.get(Route.fromFlat(`carCake@${massCakeRef}`), {})
    ).rljson.carCake._data[0] as Cake;
    const sliceIds: string[] = await (massDb as any)._resolveSliceIds(
      cake.sliceIdsTable,
      cake.sliceIdsRow,
    );
    expect(sliceIds.length).toBeGreaterThan(50);

    // Spot-check a spread of slices against the reference implementation
    const samples = [
      sliceIds[0],
      sliceIds[1],
      sliceIds[Math.floor(sliceIds.length / 2)],
      sliceIds[sliceIds.length - 1],
    ];

    for (const sliceId of samples) {
      const row = join.row(sliceId);
      for (let i = 0; i < columnSelection.columns.length; i++) {
        const columnRoute = Route.fromFlat(
          columnSelection.columns[i].route,
        ).toRouteWithProperty();
        const reference = await massDb.get(columnRoute, massCakeRef, undefined, [
          sliceId,
        ]);

        expect(row[i].value.rljson).toEqual(reference.rljson);
        expect(row[i].value.tree).toEqual(reference.tree);
        expect(comparableCells(row[i].value.cell)).toEqual(
          comparableCells(reference.cell),
        );
      }
    }
  });
});
