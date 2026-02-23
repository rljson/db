// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { IoMem, SocketMock } from '@rljson/io';
import {
  Conflict,
  createTreesTableCfg,
  Route,
  SyncConfig,
} from '@rljson/rljson';

import { beforeEach, describe, expect, it, vi } from 'vitest';

import { Connector } from '../../src/connector/connector.ts';
import { Db } from '../../src/db.ts';

describe('Conflict detection', () => {
  const treeKey = 'sharedTree';
  const historyTable = treeKey + 'InsertHistory';
  let db: Db;

  beforeEach(async () => {
    const io = new IoMem();
    await io.init();
    await io.isReady();
    db = new Db(io);
    const treeCfg = createTreesTableCfg(treeKey);
    await db.core.createTableWithInsertHistory(treeCfg);
  });

  // =========================================================================
  // Db.detectDagBranch()
  // =========================================================================

  describe('Db.detectDagBranch()', () => {
    it('returns null when InsertHistory table does not exist', async () => {
      const result = await db.detectDagBranch('nonExistentTable');
      expect(result).toBeNull();
    });

    it('returns null when history has fewer than 2 rows', async () => {
      // Empty history
      expect(await db.detectDagBranch(treeKey)).toBeNull();

      // Single row
      await db.core.import({
        [historyTable]: {
          _data: [
            {
              timeId: 'tid_A',
              sharedTreeRef: 'refA',
              route: '/sharedTree',
              previous: [],
            },
          ],
          _type: 'insertHistory',
        },
      });
      expect(await db.detectDagBranch(treeKey)).toBeNull();
    });

    it('returns null when history is linear (no branches)', async () => {
      await db.core.import({
        [historyTable]: {
          _data: [
            {
              timeId: 'tid_A',
              sharedTreeRef: 'refA',
              route: '/sharedTree',
              previous: [],
            },
            {
              timeId: 'tid_B',
              sharedTreeRef: 'refB',
              route: '/sharedTree',
              previous: ['tid_A'],
            },
          ],
          _type: 'insertHistory',
        },
      });

      expect(await db.detectDagBranch(treeKey)).toBeNull();
    });

    it('detects a DAG branch when two rows share the same previous', async () => {
      // Row A: root (no previous)
      // Row B: previous=[A]
      // Row C: previous=[A]  ← fork!
      await db.core.import({
        [historyTable]: {
          _data: [
            {
              timeId: 'tid_A',
              sharedTreeRef: 'refA',
              route: '/sharedTree',
              previous: [],
            },
            {
              timeId: 'tid_B',
              sharedTreeRef: 'refB',
              route: '/sharedTree',
              previous: ['tid_A'],
            },
            {
              timeId: 'tid_C',
              sharedTreeRef: 'refC',
              route: '/sharedTree',
              previous: ['tid_A'],
            },
          ],
          _type: 'insertHistory',
        },
      });

      const conflict = await db.detectDagBranch(treeKey);
      expect(conflict).not.toBeNull();
      expect(conflict!.type).toBe('dagBranch');
      expect(conflict!.table).toBe(treeKey);
      expect(conflict!.branches).toHaveLength(2);
      expect(conflict!.branches).toContain('tid_B');
      expect(conflict!.branches).toContain('tid_C');
      expect(typeof conflict!.detectedAt).toBe('number');
    });

    it('returns null after a branch is merged', async () => {
      // A → B and A → C (fork), then D merges both: previous=[B, C]
      await db.core.import({
        [historyTable]: {
          _data: [
            {
              timeId: 'tid_A',
              sharedTreeRef: 'refA',
              route: '/sharedTree',
              previous: [],
            },
            {
              timeId: 'tid_B',
              sharedTreeRef: 'refB',
              route: '/sharedTree',
              previous: ['tid_A'],
            },
            {
              timeId: 'tid_C',
              sharedTreeRef: 'refC',
              route: '/sharedTree',
              previous: ['tid_A'],
            },
            {
              timeId: 'tid_D',
              sharedTreeRef: 'refD',
              route: '/sharedTree',
              previous: ['tid_B', 'tid_C'],
            },
          ],
          _type: 'insertHistory',
        },
      });

      // D is the only tip now — no conflict
      expect(await db.detectDagBranch(treeKey)).toBeNull();
    });

    it('detects three-way branches', async () => {
      await db.core.import({
        [historyTable]: {
          _data: [
            {
              timeId: 'tid_A',
              sharedTreeRef: 'refA',
              route: '/sharedTree',
              previous: [],
            },
            {
              timeId: 'tid_B',
              sharedTreeRef: 'refB',
              route: '/sharedTree',
              previous: ['tid_A'],
            },
            {
              timeId: 'tid_C',
              sharedTreeRef: 'refC',
              route: '/sharedTree',
              previous: ['tid_A'],
            },
            {
              timeId: 'tid_D',
              sharedTreeRef: 'refD',
              route: '/sharedTree',
              previous: ['tid_A'],
            },
          ],
          _type: 'insertHistory',
        },
      });

      const conflict = await db.detectDagBranch(treeKey);
      expect(conflict).not.toBeNull();
      expect(conflict!.branches).toHaveLength(3);
      expect(conflict!.branches).toContain('tid_B');
      expect(conflict!.branches).toContain('tid_C');
      expect(conflict!.branches).toContain('tid_D');
    });
  });

  // =========================================================================
  // Db.registerConflictObserver / unregisterConflictObserver
  // =========================================================================

  describe('Db conflict observer registration', () => {
    it('fires conflict callback when _writeInsertHistory creates a branch', async () => {
      // Seed linear history
      await db.core.import({
        [historyTable]: {
          _data: [
            {
              timeId: 'tid_A',
              sharedTreeRef: 'refA',
              route: '/sharedTree',
              previous: [],
            },
            {
              timeId: 'tid_B',
              sharedTreeRef: 'refB',
              route: '/sharedTree',
              previous: ['tid_A'],
            },
          ],
          _type: 'insertHistory',
        },
      });

      let receivedConflict: Conflict | null = null;
      const route = Route.fromFlat(`/${treeKey}`);
      db.registerConflictObserver(route, (c) => {
        receivedConflict = c;
      });

      // Now trigger _writeInsertHistory via insertTrees with a branching row
      // Since insertTrees auto-generates timeIds, we manually create the branch
      // by importing a conflicting row via core.import, then using insert
      // to trigger _writeInsertHistory detection.

      // Import a row that creates a fork: previous=[tid_A] (same as tid_B's previous)
      // We need to go through the _writeInsertHistory path, which is private.
      // The easiest way is to use insert() on the tree table.

      // Actually, we can simulate this by calling insertTrees with actual data.
      // But that doesn't let us control `previous`.
      // Instead, let's write the branching row directly and then trigger detection
      // by calling insert with a new tree.

      // Approach: Write a conflicting InsertHistory row directly,
      // then write another one via insert() — which calls _writeInsertHistory
      // and triggers detectDagBranch.

      // First add a row that branches from tid_A
      await db.core.import({
        [historyTable]: {
          _data: [
            {
              timeId: 'tid_C',
              sharedTreeRef: 'refC',
              route: '/sharedTree',
              previous: ['tid_A'],
            },
          ],
          _type: 'insertHistory',
        },
      });

      // Now write via insertTrees to trigger _writeInsertHistory → detectDagBranch
      const tree = {
        _hash: '',
        id: 'root',
        isParent: false,
        meta: {},
        children: [],
      };

      await db.insertTrees(treeKey, [tree]);

      // The conflict callback should have fired
      expect(receivedConflict).not.toBeNull();
      expect(receivedConflict!.type).toBe('dagBranch');
      expect(receivedConflict!.table).toBe(treeKey);
      // At minimum tid_B and tid_C are tips, possibly the new row too
      expect(receivedConflict!.branches.length).toBeGreaterThanOrEqual(2);
    });

    it('unregisterConflictObserver removes specific callback', async () => {
      const route = Route.fromFlat(`/${treeKey}`);
      const cb1 = vi.fn();
      const cb2 = vi.fn();

      db.registerConflictObserver(route, cb1);
      db.registerConflictObserver(route, cb2);
      db.unregisterConflictObserver(route, cb1);

      // Seed a DAG branch
      await db.core.import({
        [historyTable]: {
          _data: [
            {
              timeId: 'tid_A',
              sharedTreeRef: 'refA',
              route: '/sharedTree',
              previous: [],
            },
            {
              timeId: 'tid_B',
              sharedTreeRef: 'refB',
              route: '/sharedTree',
              previous: ['tid_A'],
            },
            {
              timeId: 'tid_C',
              sharedTreeRef: 'refC',
              route: '/sharedTree',
              previous: ['tid_A'],
            },
          ],
          _type: 'insertHistory',
        },
      });

      // Trigger _writeInsertHistory
      await db.insertTrees(treeKey, [
        { _hash: '', id: 'r', isParent: false, meta: {}, children: [] },
      ]);

      expect(cb1).not.toHaveBeenCalled();
      expect(cb2).toHaveBeenCalled();
    });

    it('unregisterAllConflictObservers removes all callbacks', async () => {
      const route = Route.fromFlat(`/${treeKey}`);
      const cb = vi.fn();

      db.registerConflictObserver(route, cb);
      db.unregisterAllConflictObservers(route);

      // Seed a DAG branch
      await db.core.import({
        [historyTable]: {
          _data: [
            {
              timeId: 'tid_A',
              sharedTreeRef: 'refA',
              route: '/sharedTree',
              previous: [],
            },
            {
              timeId: 'tid_B',
              sharedTreeRef: 'refB',
              route: '/sharedTree',
              previous: ['tid_A'],
            },
            {
              timeId: 'tid_C',
              sharedTreeRef: 'refC',
              route: '/sharedTree',
              previous: ['tid_A'],
            },
          ],
          _type: 'insertHistory',
        },
      });

      await db.insertTrees(treeKey, [
        { _hash: '', id: 'r', isParent: false, meta: {}, children: [] },
      ]);

      expect(cb).not.toHaveBeenCalled();
    });
  });

  // =========================================================================
  // Connector.onConflict()
  // =========================================================================

  describe('Connector.onConflict()', () => {
    it('fires onConflict callback when DAG branch is detected', async () => {
      const route = Route.fromFlat(`/${treeKey}`);
      const socket = new SocketMock();
      const connector = new Connector(db, route, socket);

      let receivedConflict: Conflict | null = null;
      connector.onConflict((c) => {
        receivedConflict = c;
      });

      // Seed a DAG branch
      await db.core.import({
        [historyTable]: {
          _data: [
            {
              timeId: 'tid_A',
              sharedTreeRef: 'refA',
              route: '/sharedTree',
              previous: [],
            },
            {
              timeId: 'tid_B',
              sharedTreeRef: 'refB',
              route: '/sharedTree',
              previous: ['tid_A'],
            },
            {
              timeId: 'tid_C',
              sharedTreeRef: 'refC',
              route: '/sharedTree',
              previous: ['tid_A'],
            },
          ],
          _type: 'insertHistory',
        },
      });

      // Trigger _writeInsertHistory via insertTrees
      await db.insertTrees(treeKey, [
        { _hash: '', id: 'root', isParent: false, meta: {}, children: [] },
      ]);

      expect(receivedConflict).not.toBeNull();
      expect(receivedConflict!.type).toBe('dagBranch');
      expect(receivedConflict!.table).toBe(treeKey);

      connector.tearDown();
    });

    it('does not fire after tearDown()', async () => {
      const route = Route.fromFlat(`/${treeKey}`);
      const socket = new SocketMock();
      const connector = new Connector(db, route, socket);

      const cb = vi.fn();
      connector.onConflict(cb);

      // Tear down removes conflict observers
      connector.tearDown();

      // Seed a DAG branch and trigger write
      await db.core.import({
        [historyTable]: {
          _data: [
            {
              timeId: 'tid_A',
              sharedTreeRef: 'refA',
              route: '/sharedTree',
              previous: [],
            },
            {
              timeId: 'tid_B',
              sharedTreeRef: 'refB',
              route: '/sharedTree',
              previous: ['tid_A'],
            },
            {
              timeId: 'tid_C',
              sharedTreeRef: 'refC',
              route: '/sharedTree',
              previous: ['tid_A'],
            },
          ],
          _type: 'insertHistory',
        },
      });

      await db.insertTrees(treeKey, [
        { _hash: '', id: 'r', isParent: false, meta: {}, children: [] },
      ]);

      expect(cb).not.toHaveBeenCalled();
    });

    it('works with SyncConfig', async () => {
      const route = Route.fromFlat(`/${treeKey}`);
      const socket = new SocketMock();
      const syncConfig: SyncConfig = {
        includeClientIdentity: true,
        causalOrdering: true,
      };
      const connector = new Connector(db, route, socket, syncConfig);

      let receivedConflict: Conflict | null = null;
      connector.onConflict((c) => {
        receivedConflict = c;
      });

      // Seed a DAG branch
      await db.core.import({
        [historyTable]: {
          _data: [
            {
              timeId: 'tid_A',
              sharedTreeRef: 'refA',
              route: '/sharedTree',
              previous: [],
            },
            {
              timeId: 'tid_B',
              sharedTreeRef: 'refB',
              route: '/sharedTree',
              previous: ['tid_A'],
            },
            {
              timeId: 'tid_C',
              sharedTreeRef: 'refC',
              route: '/sharedTree',
              previous: ['tid_A'],
            },
          ],
          _type: 'insertHistory',
        },
      });

      await db.insertTrees(treeKey, [
        { _hash: '', id: 'root', isParent: false, meta: {}, children: [] },
      ]);

      expect(receivedConflict).not.toBeNull();
      expect(receivedConflict!.type).toBe('dagBranch');

      connector.tearDown();
    });
  });
});
