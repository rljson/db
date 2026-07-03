// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { IoMem } from '@rljson/io';
import { Json } from '@rljson/json';
import {
  createTreesTableCfg,
  Ref,
  Route,
  TableCfg,
  treeFromObject,
} from '@rljson/rljson';

import { bench, describe } from 'vitest';

import { Db } from '../../src/db';
import { convertMassData } from '../../src/example-static/mass-data/convert-mass-data';
import { staticExample } from '../../src/example-static/example-static';
import { RowFilter } from '../../src/join/filter/row-filter';
import {
  ColumnInfo,
  ColumnSelection,
} from '../../src/join/selection/column-selection';

/**
 * Performance benchmark suite (Phase 0 of README.performance.md).
 *
 * Run with: pnpm exec vitest bench --run
 *
 * Scenarios mirror the estimated-effects table in the concept document.
 * Not part of `pnpm test` (bench mode only).
 *
 * NOTE: setup happens via top-level await — beforeAll hooks are not
 * executed in vitest benchmark mode.
 */

// .............................................................................
const col = (key: string, route: string, type = 'string'): ColumnInfo =>
  ({
    key,
    route,
    alias: key,
    titleLong: key,
    titleShort: key,
    type,
    _hash: '',
  }) as ColumnInfo;

const massColumnSelection = () =>
  new ColumnSelection([
    col('brand', 'carCake/carGeneralLayer/carGeneral/brand'),
    col('type', 'carCake/carGeneralLayer/carGeneral/type'),
    col(
      'isElectric',
      'carCake/carGeneralLayer/carGeneral/isElectric',
      'boolean',
    ),
    col(
      'height',
      'carCake/carDimensionsLayer/carDimensions/carHeight/height',
      'number',
    ),
    col(
      'width',
      'carCake/carDimensionsLayer/carDimensions/carWidth/width',
      'number',
    ),
    col(
      'length',
      'carCake/carDimensionsLayer/carDimensions/carLength/length',
      'number',
    ),
    col('engine', 'carCake/carTechnicalLayer/carTechnical/engine'),
    col('transmission', 'carCake/carTechnicalLayer/carTechnical/transmission'),
    col('gears', 'carCake/carTechnicalLayer/carTechnical/gears', 'number'),
    col('sides', 'carCake/carColorLayer/carColor/sides'),
    col('roof', 'carCake/carColorLayer/carColor/roof'),
    col('highlights', 'carCake/carColorLayer/carColor/highlights'),
  ]);

const brandFilter = (): RowFilter =>
  ({
    columnFilters: [
      {
        type: 'string',
        column: 'carCake/carGeneralLayer/carGeneral/brand',
        operator: 'startsWith',
        search: 'b',
        matchCase: false,
        _hash: '',
      },
    ],
    operator: 'and',
    _hash: '',
  }) as RowFilter;

// .............................................................................
const setupMassDb = async (): Promise<{ db: Db; cakeRef: Ref }> => {
  const io = new IoMem();
  await io.init();
  await io.isReady();
  const db = new Db(io);

  const converted = convertMassData();
  for (const tableCfg of converted.result.tableCfgs._data as TableCfg[]) {
    await db.core.createTable(tableCfg);
  }
  await db.core.import(converted.result);

  const cakeGet = await db.get(Route.fromFlat('/carCake'), {});
  const cakeRef = (cakeGet.cell[0].row! as Json)._hash as Ref;
  db.setCache(new Map());
  return { db, cakeRef };
};

const setupStaticDb = async (): Promise<Db> => {
  const io = new IoMem();
  await io.init();
  await io.isReady();
  const db = new Db(io);
  for (const tableCfg of staticExample().tableCfgs._data) {
    await db.core.createTableWithInsertHistory(tableCfg);
  }
  await db.core.import(staticExample());
  db.setCache(new Map());
  return db;
};

// .............................................................................
// Top-level setup (bench mode does not execute beforeAll)
const { db: joinDb, cakeRef } = await setupMassDb();
const { db: joinProcDb, cakeRef: joinProcCakeRef } = await setupMassDb();
const processedJoin = await joinProcDb.join(
  massColumnSelection(),
  'carCake',
  joinProcCakeRef,
);
const getDb = await setupStaticDb();
const insertDb = await setupStaticDb();

const treeDb = await setupStaticDb();
const treeKey = 'benchTree';
// 3-level tree: 10 x 15 leaves = ~176 nodes
const treeObj: any = { root: {} };
for (let i = 0; i < 10; i++) {
  const child: any = {};
  for (let j = 0; j < 15; j++) {
    child[`leaf${j}`] = { value: i * 100 + j };
  }
  treeObj.root[`child${i}`] = child;
}
const benchTrees = treeFromObject(treeObj);
const treeRootHash = benchTrees[benchTrees.length - 1]._hash as string;
await treeDb.core.createTableWithInsertHistory(createTreesTableCfg(treeKey));
await treeDb.core.import({
  [treeKey]: { _type: 'trees', _data: benchTrees },
});
treeDb.setCache(new Map());

let insertCounter = 0;

// .............................................................................
describe('join', () => {
  bench(
    'db.join mass-data (102 slices x 12 cols)',
    async () => {
      joinDb.setCache(new Map());
      await joinDb.join(massColumnSelection(), 'carCake', cakeRef);
    },
    { iterations: 3, warmupIterations: 1 },
  );
});

describe('join processing', () => {
  bench('join.rows materialization (102 rows x 12 cols)', () => {
    void processedJoin.rows;
  });

  bench('join.filter string startsWith (102 rows)', () => {
    processedJoin.clone().filter(brandFilter());
  });

  bench('join.select 3 of 12 columns (102 rows)', () => {
    processedJoin
      .clone()
      .select(
        new ColumnSelection([
          col('brand', 'carCake/carGeneralLayer/carGeneral/brand'),
          col('type', 'carCake/carGeneralLayer/carGeneral/type'),
          col('engine', 'carCake/carTechnicalLayer/carTechnical/engine'),
        ]),
      );
  });
});

// .............................................................................
describe('get', () => {
  bench('db.get nested route cake->layer->component', async () => {
    getDb.setCache(new Map());
    await getDb.get(Route.fromFlat('/carCake/carGeneralLayer/carGeneral'), {});
  });

  bench('db.get flat component table', async () => {
    getDb.setCache(new Map());
    await getDb.get(Route.fromFlat('/carGeneral'), {});
  });
});

// .............................................................................
describe('tree', () => {
  bench(
    'tree expansion via route ref (~176 nodes)',
    async () => {
      treeDb.setCache(new Map());
      await treeDb.get(Route.fromFlat(`${treeKey}@${treeRootHash}/root`), {});
    },
    { iterations: 3, warmupIterations: 1 },
  );
});

// .............................................................................
describe('insert', () => {
  bench('db.insert component (history grows per iteration)', async () => {
    insertCounter++;
    await insertDb.insert(Route.fromFlat('carGeneral'), {
      carGeneral: {
        _type: 'components',
        _data: [
          {
            brand: `BenchBrand${insertCounter}`,
            type: `BenchType${insertCounter}`,
            doors: 4,
            energyConsumption: 5,
            units: { energy: 'kWh/100km', _hash: '' },
            serviceIntervals: [10000],
            isElectric: true,
            meta: { _hash: '' },
            _hash: '',
          },
        ],
      },
    });
  });
});
