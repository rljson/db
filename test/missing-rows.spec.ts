// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';
import { IoMem } from '@rljson/io';
import { Json } from '@rljson/json';
import {
  createCakeTableCfg,
  createEditHistoryTableCfg,
  createEditTableCfg,
  createInsertHistoryTableCfg,
  createLayerTableCfg,
  createMultiEditTableCfg,
  createSliceIdsTableCfg,
  createTreesTableCfg,
  TableCfg,
} from '@rljson/rljson';

import { describe, expect, it } from 'vitest';

import { Core } from '../src/core.ts';
import { MissingRows } from '../src/missing-rows.ts';

// .............................................................................
// The rows under a ref that a store lacks: the ref's row and everything it
// references, followed through every table, read from that store alone.
// .............................................................................

/** A row with its hash. */
const row = <T extends Record<string, unknown>>(data: T): T & { _hash: string } =>
  hip({ ...data, _hash: '' } as unknown as Json) as unknown as T & {
    _hash: string;
  };

/** A store with these tables and rows. */
const store = async (
  tables: Array<{ cfg: TableCfg; rows: Json[] }>,
): Promise<{ io: IoMem; core: Core }> => {
  const io = new IoMem();
  await io.init();
  await io.isReady();
  const core = new Core(io);
  for (const { cfg } of tables) await core.createTable(cfg);
  for (const { cfg, rows } of tables) {
    if (rows.length > 0) {
      await core.import({ [cfg.key]: { _type: cfg.type, _data: rows } } as never, {
        validate: false,
      });
    }
  }
  return { io, core };
};

/** Components with a reference column into `wheels`. */
const carsCfg: TableCfg = {
  key: 'cars',
  type: 'components',
  columns: [
    { key: '_hash', type: 'string', titleLong: 'Hash', titleShort: 'Hash' },
    {
      key: 'wheels',
      type: 'jsonValue',
      titleLong: 'Wheels',
      titleShort: 'Wheels',
      ref: { tableKey: 'wheels', type: 'components' },
    },
  ],
} as TableCfg;

const wheelsCfg: TableCfg = {
  key: 'wheels',
  type: 'components',
  columns: [
    { key: '_hash', type: 'string', titleLong: 'Hash', titleShort: 'Hash' },
    { key: 'size', type: 'number', titleLong: 'Size', titleShort: 'Size' },
  ],
} as TableCfg;

// A file tree: root → dir → file, the files edit chain on top.
const file = row({ id: 'a.txt', isParent: false, meta: null, children: null });
const dir = row({ id: 'dir', isParent: true, meta: null, children: [file._hash] });
const root = row({ id: 'root', isParent: true, meta: null, children: [dir._hash] });
const edit = row({ name: 'e1', action: { name: 'scan', type: 'set', data: {} } });
const multiEdit = row({ previous: null, edit: edit._hash });
const history1 = row({
  timeId: '1700000000000:AbCd',
  multiEditRef: multiEdit._hash,
  dataRef: root._hash,
  previous: [],
});
const edit2 = row({ name: 'e2', action: { name: 'scan', type: 'set', data: {} } });
const multiEdit2 = row({ previous: multiEdit._hash, edit: edit2._hash });
const history2 = row({
  timeId: '1700000000001:EfGh',
  multiEditRef: multiEdit2._hash,
  dataRef: root._hash,
  previous: [history1._hash],
});

const fileTables = (
  omit: { trees?: string[]; edits?: string[]; multiEdits?: string[]; history?: string[] } = {},
) => {
  const keep = (rows: Array<{ _hash: string }>, out: string[] = []) =>
    rows.filter((r) => !out.includes(r._hash)) as unknown as Json[];
  return [
    { cfg: createTreesTableCfg('files'), rows: keep([file, dir, root], omit.trees) },
    { cfg: createEditTableCfg('files'), rows: keep([edit, edit2], omit.edits) },
    {
      cfg: createMultiEditTableCfg('files'),
      rows: keep([multiEdit, multiEdit2], omit.multiEdits),
    },
    {
      cfg: createEditHistoryTableCfg('files'),
      rows: keep([history1, history2], omit.history),
    },
  ];
};

describe('MissingRows', () => {
  describe('of an edit history head', () => {
    it('finds nothing missing when the store holds the whole chain and tree', async () => {
      const { io } = await store(fileTables());
      const missing = new MissingRows(io);

      expect(await missing.of(history2._hash)).toEqual([]);
      expect(await missing.holds(history2._hash)).toBe(true);
    });

    it('names a file of the tree the store lacks', async () => {
      const { io } = await store(fileTables({ trees: [file._hash] }));

      expect(await new MissingRows(io).of(history2._hash)).toEqual([
        { table: 'files', hash: file._hash },
      ]);
    });

    it('names an ancestor entry the store lacks', async () => {
      const { io } = await store(fileTables({ history: [history1._hash] }));

      expect(await new MissingRows(io).of(history2._hash)).toEqual([
        { table: 'filesEditHistory', hash: history1._hash },
      ]);
    });

    it('names a multi edit, its predecessor and an edit the store lacks', async () => {
      const { io } = await store(
        fileTables({ multiEdits: [multiEdit._hash], edits: [edit2._hash] }),
      );

      const missing = await new MissingRows(io).of(history2._hash);

      expect(missing).toContainEqual({ table: 'filesEdits', hash: edit2._hash });
      expect(missing).toContainEqual({
        table: 'filesMultiEdits',
        hash: multiEdit._hash,
      });
      expect(missing).toHaveLength(2);
    });

    it('stops at the limit', async () => {
      const { io } = await store(
        fileTables({ multiEdits: [multiEdit._hash], edits: [edit2._hash] }),
      );

      expect(await new MissingRows(io).of(history2._hash, 1)).toHaveLength(1);
      expect(await new MissingRows(io).holds(history2._hash)).toBe(false);
    });

    it('takes a ref with a prefix by the part after its last colon', async () => {
      const { io } = await store(fileTables());

      expect(await new MissingRows(io).holds(`docs:${history2._hash}`)).toBe(true);
    });

    it('names the ref itself when its row is in no table', async () => {
      const { io } = await store(fileTables({ history: [history2._hash] }));

      expect(await new MissingRows(io).of(`docs:${history2._hash}`)).toEqual([
        { hash: `docs:${history2._hash}` },
      ]);
      expect(await new MissingRows(io).of('unknown')).toEqual([
        { hash: 'unknown' },
      ]);
    });

    it('treats a table the store lacks as lacking its rows', async () => {
      const tables = fileTables().filter(
        (t) => t.cfg.key !== 'filesMultiEdits',
      );
      const { io } = await store(tables);

      expect(await new MissingRows(io).of(history1._hash)).toEqual([
        { table: 'filesMultiEdits', hash: multiEdit._hash },
      ]);
    });

    it('treats a table it cannot read as lacking its rows', async () => {
      const { io } = await store(fileTables());
      const missing = new MissingRows(io);
      // A store that fails reading one table.
      const original = io.readRowsByHashes.bind(io);
      io.readRowsByHashes = async (request) => {
        if (request.table === 'files') throw new Error('broken');
        return original(request);
      };

      expect(await missing.of(history1._hash)).toEqual([
        { table: 'files', hash: root._hash },
      ]);
    });
  });

  describe('a store without readRowsByHashes', () => {
    it('is read row by row', async () => {
      const { io } = await store(fileTables({ trees: [file._hash] }));
      (io as { readRowsByHashes?: unknown }).readRowsByHashes = undefined;

      expect(await new MissingRows(io).of(history2._hash)).toEqual([
        { table: 'files', hash: file._hash },
      ]);
    });
  });

  describe('remembering', () => {
    it('walks only the new rows of the next head', async () => {
      const { io } = await store(fileTables());
      const missing = new MissingRows(io);
      expect(await missing.holds(history1._hash)).toBe(true);

      const read: string[] = [];
      const original = io.readRowsByHashes.bind(io);
      io.readRowsByHashes = async (request) => {
        read.push(...request.hashes);
        return original(request);
      };

      expect(await missing.holds(history2._hash)).toBe(true);
      expect(read).not.toContain(root._hash);
      expect(read).not.toContain(history1._hash);
      expect(read).toContain(multiEdit2._hash);
    });

    it('answers a complete ref again without reading', async () => {
      const { io } = await store(fileTables());
      const missing = new MissingRows(io);
      await missing.holds(history2._hash);

      io.readRowsByHashes = async () => {
        throw new Error('not read again');
      };
      io.rawTableCfgs = async () => {
        throw new Error('not read again');
      };

      expect(await missing.holds(history2._hash)).toBe(true);
      expect(await missing.holds(`docs:${history2._hash}`)).toBe(true);
    });

    it('remembers nothing of a walk that found rows missing', async () => {
      const { io, core } = await store(fileTables({ trees: [file._hash] }));
      const missing = new MissingRows(io);
      expect(await missing.holds(history2._hash)).toBe(false);

      await core.import(
        { files: { _type: 'trees', _data: [file as unknown as Json] } } as never,
        { validate: false },
      );

      expect(await missing.holds(history2._hash)).toBe(true);
    });

    it('starts over when it would remember more than maxRemembered', async () => {
      const { io } = await store(fileTables());
      const missing = new MissingRows(io, { maxRemembered: 4 });
      expect(await missing.holds(history1._hash)).toBe(true);
      expect(await missing.holds(history2._hash)).toBe(true);

      const read: string[] = [];
      const original = io.readRowsByHashes.bind(io);
      io.readRowsByHashes = async (request) => {
        read.push(...request.hashes);
        return original(request);
      };
      // history2's walk is remembered, history1's was forgotten when
      // history2's did not fit.
      expect(await missing.holds(history2._hash)).toBe(true);
      expect(read).toEqual([]);
      expect(await missing.holds(history1._hash)).toBe(true);
      expect(read).toContain(history1._hash);
    });
  });

  describe('references', () => {
    it('follows the declared reference columns of components', async () => {
      const wheel1 = row({ size: 17 });
      const wheel2 = row({ size: 18 });
      const car = row({ wheels: [wheel1._hash, { ref: wheel2._hash }] });
      const { io } = await store([
        { cfg: carsCfg, rows: [car as unknown as Json] },
        { cfg: wheelsCfg, rows: [wheel1 as unknown as Json] },
      ]);

      expect(await new MissingRows(io).of(car._hash)).toEqual([
        { table: 'wheels', hash: wheel2._hash },
      ]);
    });

    it('ignores empty references', async () => {
      const car = row({ wheels: null });
      const { io } = await store([
        { cfg: carsCfg, rows: [car as unknown as Json] },
        { cfg: wheelsCfg, rows: [] },
      ]);

      expect(await new MissingRows(io).holds(car._hash)).toBe(true);
    });

    it('follows a cake to its layers, a layer to its base, slice ids and components', async () => {
      const wheel = row({ size: 17 });
      const sliceIdsBase = row({ base: null, add: ['s0'], remove: null });
      const sliceIds = row({ base: sliceIdsBase._hash, add: ['s1'], remove: null });
      const layerBase = row({
        base: null,
        sliceIdsTable: 'slices',
        sliceIdsTableRow: sliceIds._hash,
        componentsTable: 'wheels',
        add: {},
        remove: null,
      });
      const layer = row({
        base: layerBase._hash,
        sliceIdsTable: 'slices',
        sliceIdsTableRow: sliceIds._hash,
        componentsTable: 'wheels',
        add: { s1: wheel._hash },
        remove: null,
      });
      const cake = row({
        sliceIdsTable: 'slices',
        sliceIdsRow: sliceIds._hash,
        layers: { _note: 'skipped', wheelLayer: layer._hash },
        id: 'cake',
      });

      const complete = await store([
        { cfg: createCakeTableCfg('carCake'), rows: [cake as unknown as Json] },
        {
          cfg: createLayerTableCfg('wheelLayer'),
          rows: [layer, layerBase] as unknown as Json[],
        },
        {
          cfg: createSliceIdsTableCfg('slices'),
          rows: [sliceIds, sliceIdsBase] as unknown as Json[],
        },
        { cfg: wheelsCfg, rows: [wheel as unknown as Json] },
      ]);
      expect(await new MissingRows(complete.io).of(cake._hash)).toEqual([]);

      const lacking = await store([
        { cfg: createCakeTableCfg('carCake'), rows: [cake as unknown as Json] },
        {
          cfg: createLayerTableCfg('wheelLayer'),
          rows: [layer as unknown as Json],
        },
        {
          cfg: createSliceIdsTableCfg('slices'),
          rows: [sliceIds as unknown as Json],
        },
        { cfg: wheelsCfg, rows: [] },
      ]);
      const missing = await new MissingRows(lacking.io).of(cake._hash);
      expect(missing).toContainEqual({ table: 'wheelLayer', hash: layerBase._hash });
      expect(missing).toContainEqual({ table: 'slices', hash: sliceIdsBase._hash });
      expect(missing).toContainEqual({ table: 'wheels', hash: wheel._hash });
      expect(missing).toHaveLength(3);
    });

    it('takes layers and cakes with their optional fields empty', async () => {
      const layer = row({
        base: null,
        sliceIdsTable: null,
        sliceIdsTableRow: null,
        componentsTable: 'wheels',
        add: null,
        remove: null,
      });
      const cake = row({
        sliceIdsTable: 'slices',
        sliceIdsRow: '',
        layers: null,
        id: 'cake',
      });
      const { io } = await store([
        { cfg: createCakeTableCfg('carCake'), rows: [cake as unknown as Json] },
        {
          cfg: createLayerTableCfg('wheelLayer'),
          rows: [layer as unknown as Json],
        },
      ]);

      expect(await new MissingRows(io).holds(cake._hash)).toBe(true);
      expect(await new MissingRows(io).holds(layer._hash)).toBe(true);
    });

    it('takes the newest version of a table config, the one with most columns', async () => {
      const { io } = await store(fileTables());
      const cfgs = await io.rawTableCfgs();
      const history = cfgs.find((c) => c.key === 'filesEditHistory')!;
      const older = { ...history, columns: history.columns.slice(0, 2) };
      io.rawTableCfgs = async () => [older, ...cfgs, older];

      // With the older version, multiEditRef and dataRef would not be
      // followed and the lacking edit would go unnoticed.
      const lacking = await store(fileTables({ edits: [edit2._hash] }));
      const lackingCfgs = await lacking.io.rawTableCfgs();
      lacking.io.rawTableCfgs = async () => [older, ...lackingCfgs, older];

      expect(await new MissingRows(io).holds(history2._hash)).toBe(true);
      expect(await new MissingRows(lacking.io).holds(history2._hash)).toBe(false);
    });

    it('takes an answer without the table as no rows', async () => {
      const { io } = await store(fileTables());
      io.readRowsByHashes = async () => ({}) as never;

      expect(await new MissingRows(io).of('anything')).toEqual([
        { hash: 'anything' },
      ]);
    });

    it('follows an insert history row to the row it names', async () => {
      const wheel = row({ size: 17 });
      const entry = row({
        timeId: '1700000000000:AbCd',
        wheelsRef: wheel._hash,
        route: '/wheels',
      });
      const tables = (rows: Json[]) => [
        { cfg: wheelsCfg, rows },
        {
          cfg: createInsertHistoryTableCfg(wheelsCfg),
          rows: [entry as unknown as Json],
        },
      ];

      const complete = await store(tables([wheel as unknown as Json]));
      expect(await new MissingRows(complete.io).holds(entry._hash)).toBe(true);

      const lacking = await store(tables([]));
      expect(await new MissingRows(lacking.io).of(entry._hash)).toEqual([
        { table: 'wheels', hash: wheel._hash },
      ]);
    });

    it('looks for the ref only in tables it has a config for', async () => {
      const { io } = await store(fileTables());
      const cfgs = await io.rawTableCfgs();
      io.rawTableCfgs = async () =>
        cfgs.filter((c) => c.key !== 'filesEditHistory');

      expect(await new MissingRows(io).holds(history2._hash)).toBe(false);
    });

    it('follows nothing from a row of a table it has no config for', async () => {
      const { io } = await store(fileTables({ edits: [edit2._hash] }));
      const cfgs = await io.rawTableCfgs();
      io.rawTableCfgs = async () =>
        cfgs.filter((c) => c.key !== 'filesMultiEdits');

      // The multi edits are read, but nothing says what they reference.
      expect(await new MissingRows(io).holds(history2._hash)).toBe(true);
    });
  });
});
