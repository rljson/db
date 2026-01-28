// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { IoMem, IoMulti } from '@rljson/io';
import { createTreesTableCfg, Route, treeFromObject } from '@rljson/rljson';

import { beforeEach, describe, expect, it } from 'vitest';

import { Core } from '../src/core.ts';
import { Db } from '../src/db.ts';

describe('IoMulti Tree Cascade', () => {
  let ioA: IoMem;
  let ioB: IoMem;
  let ioMulti: IoMulti;
  let db: Db;
  let coreA: Core;
  let coreB: Core;

  beforeEach(async () => {
    // Setup two separate Io instances
    ioA = new IoMem();
    await ioA.init();
    await ioA.isReady();

    ioB = new IoMem();
    await ioB.init();
    await ioB.isReady();

    // Create Core instances to properly create tables
    coreA = new Core(ioA);
    coreB = new Core(ioB);

    // Create tree table in both using Core
    const treeCfg = createTreesTableCfg('testTree');
    await coreA.createTableWithInsertHistory(treeCfg);
    await coreB.createTableWithInsertHistory(treeCfg);

    // Create IoMulti with ioA first (empty), ioB second (has data)
    ioMulti = new IoMulti([
      { io: ioA, read: true, write: true, dump: true, priority: 1 },
      { io: ioB, read: true, write: false, dump: false, priority: 2 },
    ]);
    await ioMulti.init();
    await ioMulti.isReady();

    // Create Db on top of IoMulti
    db = new Db(ioMulti);
  });

  it('Should cascade to second Io when first is empty', async () => {
    // Store tree in ioB only (priority 2)
    const trees = treeFromObject({ x: 10, y: { z: 20 } });
    const rootHash = trees[trees.length - 1]._hash;
    await coreB.import({ testTree: { _type: 'trees', _data: trees } });

    // Query by hash through IoMulti - should cascade to ioB
    const result = await db.get(Route.fromFlat('testTree'), {
      _hash: rootHash,
    });

    // Should get data from ioB (priority 2) since ioA (priority 1) is empty
    expect(result.rljson.testTree._data.length).toBeGreaterThan(0);

    // Verify it's the correct tree - root node from treeFromObject
    const rootNode = result.rljson.testTree._data.find(
      (node: any) => node._hash === rootHash,
    );
    expect(rootNode).toBeDefined();
    // Root node should have children (x and y)
    expect(rootNode.children).toBeDefined();
    expect(rootNode.children.length).toBeGreaterThan(0);
  });

  it('Should return data from first priority when available', async () => {
    // Store tree in BOTH ioA and ioB
    const treesA = treeFromObject({ a: 1 });
    const rootHashA = treesA[treesA.length - 1]._hash;
    await coreA.import({ testTree: { _type: 'trees', _data: treesA } });

    const treesB = treeFromObject({ b: 2 });
    await coreB.import({ testTree: { _type: 'trees', _data: treesB } });

    // Query for tree in ioA - should get it from ioA (priority 1), not ioB
    const result = await db.get(Route.fromFlat('testTree'), {
      _hash: rootHashA,
    });

    expect(result.rljson.testTree._data.length).toBeGreaterThan(0);
    const rootNode = result.rljson.testTree._data.find(
      (node: any) => node._hash === rootHashA,
    );
    expect(rootNode).toBeDefined();
    // Root node is now always 'root'
    expect(rootNode.id).toBe('root');
  });

  it('Should return empty when tree not found in any priority', async () => {
    // Don't store any data - both ioA and ioB are empty

    // Query for non-existent hash
    const result = await db.get(Route.fromFlat('testTree'), {
      _hash: 'nonExistentHash',
    });

    // Should return empty array after checking all priorities
    expect(result.rljson.testTree._data).toEqual([]);
  });

  it('Should cascade for Core.readRows() direct calls', async () => {
    // Store tree in ioB only
    const trees = treeFromObject({ direct: 'test' });
    const rootHash = trees[trees.length - 1]._hash;
    await coreB.import({ testTree: { _type: 'trees', _data: trees } });

    // Create Core directly on IoMulti
    const core = new Core(ioMulti);

    // Direct readRows call should also cascade
    const result = await core.readRows('testTree', { _hash: rootHash });

    expect(result.testTree._data.length).toBeGreaterThan(0);
    const rootNode = result.testTree._data.find(
      (node: any) => node._hash === rootHash,
    );
    expect(rootNode).toBeDefined();
  });

  it('Should handle large trees without heap overflow', async () => {
    // Create a tree with 50+ nodes - no explicit 'root' needed
    const largeObj: any = {};
    for (let i = 0; i < 50; i++) {
      largeObj[`child${i}`] = { value: i };
    }

    const trees = treeFromObject(largeObj);
    const rootHash = trees[trees.length - 1]._hash;
    await coreB.import({ testTree: { _type: 'trees', _data: trees } });

    // Query should not cause heap overflow
    const result = await db.get(Route.fromFlat('testTree'), {
      _hash: rootHash,
    });

    // Should get exactly one node (the root) due to path-based conditional expansion
    expect(result.rljson.testTree._data.length).toBe(1);
    expect(result.rljson.testTree._data[0]._hash).toBe(rootHash);
    expect(result.rljson.testTree._data[0].id).toBe('root');
  });

  it('Should expand children when path parameter is provided', async () => {
    // Store tree with children
    const trees = treeFromObject({ parent: { childA: 1, childB: 2 } });
    const rootHash = trees[trees.length - 1]._hash;
    await coreB.import({ testTree: { _type: 'trees', _data: trees } });

    // Get the controller to test direct get() with path parameter
    const controller = await db.getController('testTree');

    // Call get with path parameter - should expand children
    // Root node is now always 'root', not 'parent'
    const result = await controller.get(rootHash, undefined, 'root');

    // Should include root AND all children nodes
    expect(result.testTree._data.length).toBeGreaterThan(1);

    // Should include the root node
    const rootNode = result.testTree._data.find(
      (node: any) => node._hash === rootHash,
    );
    expect(rootNode).toBeDefined();
    expect(rootNode.id).toBe('root');
  });

  it('Should recursively fetch all tree children through IoMulti cascade', async () => {
    // Create a deeper tree: level1 -> level2 -> level3
    // No explicit 'root' key - treeFromObject creates root automatically
    const trees = treeFromObject({
      level1a: {
        level2a: {
          level3a: 'deepValue1',
        },
      },
      level1b: {
        level2b: 'deepValue2',
      },
    });
    const rootHash = trees[trees.length - 1]._hash;

    // Store ALL tree nodes in ioB (priority 2), ioA (priority 1) remains empty
    await coreB.import({ testTree: { _type: 'trees', _data: trees } });

    // Get the controller
    const controller = await db.getController('testTree');

    // Call get with path parameter to trigger children expansion
    // This should recursively fetch root and ALL descendants from ioB
    const result = await controller.get(rootHash, undefined, 'root');

    // Should get all nodes from the tree
    // Trees array contains: leaf values + parent nodes + auto-created root
    expect(result.testTree._data.length).toBe(trees.length);

    // Verify root node is present (auto-created by treeFromObject)
    const rootNode = result.testTree._data.find(
      (node: any) => node._hash === rootHash,
    );
    expect(rootNode).toBeDefined();
    expect(rootNode.id).toBe('root');

    // Verify level1a child is present
    const level1Child = result.testTree._data.find(
      (node: any) => node.id === 'level1a',
    );
    expect(level1Child).toBeDefined();

    // Verify deep child level3a is present
    const deepChild = result.testTree._data.find(
      (node: any) => node.id === 'level3a',
    );
    expect(deepChild).toBeDefined();
  });
});
