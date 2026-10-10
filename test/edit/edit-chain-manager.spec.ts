// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { IoMem } from '@rljson/io';
import { createTreesTableCfg, EditHistory, MultiEdit } from '@rljson/rljson';

import { beforeEach, describe, expect, it } from 'vitest';

import { Db } from '../../src/db';
import { EditChainManager } from '../../src/edit/edit-chain-manager';

const key = 'fileTree';

// The chain's `dataRef` column refers to the `${key}` table, so that table
// has to exist — here a trees table, as fs-agent's host creates it.
const newDb = async (): Promise<Db> => {
  const io = new IoMem();
  await io.init();
  await io.isReady();
  const db = new Db(io);
  await db.core.createTable(createTreesTableCfg(key));
  return db;
};

const action = (data: string) => ({
  name: 'putTree',
  type: 'putTree',
  data: { tree: data },
});

describe('EditChainManager', () => {
  let db: Db;
  let chain: EditChainManager;

  beforeEach(async () => {
    db = await newDb();
    chain = new EditChainManager(key, db);
    await chain.init();
  });

  describe('init()', () => {
    it('creates the three tables, and a second call changes nothing', async () => {
      await chain.init();
      for (const table of ['Edits', 'MultiEdits', 'EditHistory']) {
        expect(await db.core.hasTable(`${key}${table}`)).toBe(true);
      }
    });
  });

  describe('append()', () => {
    it('writes a root entry and reads it back', async () => {
      const root = await chain.append({
        dataRef: 'tree0',
        previous: [],
        action: action('v0'),
      });

      expect(root.timeId).toMatch(/^\d+:/);
      expect(await chain.entry(root.head)).toMatchObject({
        head: root.head,
        timeId: root.timeId,
        dataRef: 'tree0',
        previous: [],
        action: { name: 'putTree', type: 'putTree', data: { tree: 'v0' } },
      });
    });

    it('records one previous entry for an ordinary edit', async () => {
      const root = await chain.append({
        dataRef: 'tree0',
        previous: [],
        action: action('v0'),
      });
      const next = await chain.append({
        dataRef: 'tree1',
        previous: [root.head],
        action: action('v1'),
      });

      const read = await chain.entry(next.head);
      expect(read?.previous).toEqual([root.head]);
      expect(read?.dataRef).toBe('tree1');
      expect(read?.action.data).toMatchObject({ tree: 'v1' });
    });

    it('records two previous entries for a merge', async () => {
      const root = await chain.append({
        dataRef: 'tree0',
        previous: [],
        action: action('v0'),
      });
      const a = await chain.append({
        dataRef: 'treeA',
        previous: [root.head],
        action: action('a'),
      });
      const b = await chain.append({
        dataRef: 'treeB',
        previous: [root.head],
        action: action('b'),
      });
      const merge = await chain.append({
        dataRef: 'treeAB',
        previous: [a.head, b.head],
        action: action('ab'),
      });

      expect((await chain.entry(merge.head))?.previous).toEqual([
        a.head,
        b.head,
      ]);
    });

    it('gives the same entry the same ref on two databases when the timeId is passed', async () => {
      const otherDb = await newDb();
      const other = new EditChainManager(key, otherDb);
      await other.init();

      const opts = {
        dataRef: 'tree0',
        previous: [],
        action: action('v0'),
        timeId: '0:tree0',
      };
      const here = await chain.append(opts);
      const there = await other.append(opts);

      expect(here.head).toBe(there.head);
      expect(here.timeId).toBe('0:tree0');
    });

    it('mints different timeIds, and so different refs, when none is passed', async () => {
      const opts = { dataRef: 'tree0', previous: [], action: action('v0') };
      const first = await chain.append(opts);
      const second = await chain.append(opts);
      expect(first.head).not.toBe(second.head);
    });
  });

  describe('entries()', () => {
    it('reads many entries at once and leaves out refs it cannot read', async () => {
      const a = await chain.append({
        dataRef: 'treeA',
        previous: [],
        action: action('a'),
      });
      const b = await chain.append({
        dataRef: 'treeB',
        previous: [a.head],
        action: action('b'),
      });

      const read = await chain.entries([a.head, 'unknownRef', b.head]);
      expect([...read.keys()]).toEqual([a.head, b.head]);
      expect(read.get(b.head)?.previous).toEqual([a.head]);
    });

    it('returns an empty map for no refs', async () => {
      expect((await chain.entries([])).size).toBe(0);
    });

    it('leaves out an entry whose multi edit is missing', async () => {
      const [{ [`${key}EditHistoryRef`]: head }] = (await db.addEditHistory(
        key,
        {
          timeId: '1:x',
          multiEditRef: 'missingMultiEdit',
          dataRef: 'tree',
          previous: [],
          _hash: '',
        } as EditHistory,
      )) as Array<Record<string, string>>;

      expect(await chain.entry(head)).toBeUndefined();
    });

    it('leaves out an entry whose edit is missing', async () => {
      const [{ [`${key}MultiEditsRef`]: multiEditRef }] =
        (await db.addMultiEdit(key, {
          previous: null,
          edit: 'missingEdit',
          _hash: '',
        } as MultiEdit)) as Array<Record<string, string>>;
      const [{ [`${key}EditHistoryRef`]: head }] = (await db.addEditHistory(
        key,
        {
          timeId: '1:y',
          multiEditRef,
          dataRef: 'tree',
          previous: [],
          _hash: '',
        } as EditHistory,
      )) as Array<Record<string, string>>;

      expect(await chain.entry(head)).toBeUndefined();
    });

    it('reads a root another writer stored with previous: null as having none', async () => {
      const written = await chain.append({
        dataRef: 'tree0',
        previous: [],
        action: action('v0'),
      });
      const history = (await chain.entry(written.head)) as {
        timeId: string;
      };
      const read = await db.core.readRowsByHashes(`${key}EditHistory`, [
        written.head,
      ]);
      const row = read.get(written.head) as EditHistory;

      const [{ [`${key}EditHistoryRef`]: head }] = (await db.addEditHistory(
        key,
        {
          timeId: history.timeId,
          multiEditRef: row.multiEditRef,
          dataRef: 'tree0',
          previous: null,
          _hash: '',
        } as unknown as EditHistory,
      )) as Array<Record<string, string>>;

      expect((await chain.entry(head))?.previous).toEqual([]);
    });
  });
});
