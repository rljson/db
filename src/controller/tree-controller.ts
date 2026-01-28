// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
import { hsh } from '@rljson/hash';
import { Json } from '@rljson/json';
// found in the LICENSE file in the root of this package.
import {
  InsertCommand,
  InsertHistoryRow,
  Ref,
  Rljson,
  Route,
  TableKey,
  timeId,
  Tree,
  TreesTable,
  TreeWithHash,
} from '@rljson/rljson';

import { Core } from '../core.ts';
import { Cell } from '../db.ts';

import { BaseController } from './base-controller.ts';
import { Controller, ControllerChildProperty } from './controller.ts';

export class TreeController<N extends string, C extends Tree>
  extends BaseController<TreesTable, C>
  implements Controller<TreesTable, C, N>
{
  constructor(
    protected readonly _core: Core,
    protected readonly _tableKey: TableKey,
  ) {
    super(_core, _tableKey);
    this._contentType = 'trees';
  }

  async init() {
    // Validate Table

    // TableKey must end with 'Tree'
    if (this._tableKey.endsWith('Tree') === false) {
      throw new Error(
        `Table ${this._tableKey} is not supported by TreeController.`,
      );
    }

    // Table must be of type trees
    const contentType = await this._core.contentType(this._tableKey);
    /* v8 ignore next -- @preserve */
    if (contentType !== 'trees') {
      throw new Error(`Table ${this._tableKey} is not of type trees.`);
    }

    //Get TableCfg
    this._tableCfg = await this._core.tableCfg(this._tableKey);
  }

  async insert(
    command: InsertCommand,
    value: Tree,
    origin?: Ref,
  ): Promise<InsertHistoryRow<any>[]> {
    // Validate command
    /* v8 ignore next -- @preserve */
    if (!command.startsWith('add') && !command.startsWith('remove')) {
      throw new Error(`Command ${command} is not supported by TreeController.`);
    }

    const rlJson = { [this._tableKey]: { _data: [value] } } as Rljson;

    //Write component to io
    await this._core.import(rlJson);

    //Create InsertHistoryRow
    const result = {
      //Ref to component
      [this._tableKey + 'Ref']: hsh(value as Json)._hash as string,

      //Data from edit
      route: '',
      origin,

      //Unique id/timestamp
      timeId: timeId(),
    } as InsertHistoryRow<any>;

    return [result];
  }

  async get(
    where: string | Json,
    filter?: Json,
    path?: string,
  ): Promise<Rljson> {
    const {
      [this._tableKey]: { _data: trees },
    } =
      typeof where === 'string'
        ? await this._getByHash(where, filter)
        : await this._getByWhere(where, filter);

    // Don't process empty results - return as-is to allow IoMulti cascade
    // If all IoMulti priorities returned empty, we should propagate that
    if (trees.length === 0) {
      return { [this._tableKey]: { _data: [], _type: 'trees' } } as Rljson;
    }

    // Only do tree-specific validation when we have data
    if (trees.length > 1) {
      throw new Error(
        `Multiple trees found for where clause. Please specify a more specific query.`,
      );
    }

    const treeRoute = Route.fromFlat(path || '');
    /* v8 ignore next -- @preserve */
    const treeId =
      treeRoute.segments.length > 0 ? treeRoute.top.tableKey : null;
    const tree = (trees as Tree[])[0];

    /* v8 ignore next -- @preserve */
    if (treeId && treeId !== tree.id) {
      return { [this._tableKey]: { _data: [], _type: 'trees' } } as Rljson;
    }

    // Expand children if:
    // path !== undefined: We're navigating a route (even if path is now "" after .deeper())
    // path === undefined: This is a WHERE clause query - return only the requested node
    const shouldExpandChildren = path !== undefined;

    if (!shouldExpandChildren) {
      // Return only the requested tree node without expanding children
      return {
        [this._tableKey]: {
          _data: [tree],
          _type: 'trees',
        },
      } as Rljson;
    }

    // Expand children for route navigation
    const children: any[] = [];
    for (const childRef of tree.children ?? []) {
      const child = await this.get(
        childRef,
        undefined,
        treeRoute.deeper().flat,
      );
      const childData = child[this._tableKey]._data;
      children.push(...childData);
    }
    return {
      [this._tableKey]: {
        _data: [...children, tree],
        _type: 'trees',
      },
    } as Rljson;
  }

  async buildTreeFromTrees(trees: Tree[]): Promise<Json> {
    if (trees.length === 0) {
      return {};
    }

    // Safety check: prevent processing excessively large trees
    /* v8 ignore next 5 -- @preserve */
    if (trees.length > 100000) {
      throw new Error(
        `TreeController.buildTreeFromTrees: Tree size exceeds limit (${trees.length} > 100000 nodes). ` +
          `This may indicate a performance issue or data structure problem.`,
      );
    }

    // Create a map of hash to tree for quick lookup
    const treeMap = new Map<string, Tree>();
    for (const tree of trees) {
      treeMap.set((tree as TreeWithHash)._hash, tree);
    }

    // Memoization map to prevent processing same subtree multiple times
    const memo = new Map<string, any>();

    let buildObjectCallCount = 0;
    const MAX_ITERATIONS = 1000000; // Safety limit to prevent infinite loops

    // Recursive function to build object from tree
    const buildObject = (tree: Tree, depth = 0): any => {
      buildObjectCallCount++;

      // Safety check: prevent infinite loops
      /* v8 ignore next 5 -- @preserve */
      if (buildObjectCallCount > MAX_ITERATIONS) {
        throw new Error(
          `TreeController.buildTreeFromTrees: Maximum iterations (${MAX_ITERATIONS}) exceeded. ` +
            `This likely indicates a bug. Processed ${buildObjectCallCount} nodes from ${trees.length} total.`,
        );
      }

      // Safety check: prevent stack overflow from deep nesting
      /* v8 ignore next 5 -- @preserve */
      if (depth > 10000) {
        throw new Error(
          `TreeController.buildTreeFromTrees: Tree depth exceeds limit (${depth} > 10000). ` +
            `This may indicate a circular reference or extremely deep structure.`,
        );
      }

      const hash = (tree as TreeWithHash)._hash;

      // Return memoized result if already processed
      /* v8 ignore next 3 -- @preserve */
      if (memo.has(hash)) {
        return memo.get(hash);
      }

      // Leaf node - return meta value
      if (!tree.isParent || !tree.children || tree.children.length === 0) {
        const result = tree;
        memo.set(hash, result);
        return result;
      }

      // Parent node - build object from children
      const result: any = {};
      for (const childHash of tree.children) {
        const childTree = treeMap.get(childHash as string);
        /* v8 ignore else -- @preserve */
        if (childTree && childTree.id) {
          result[childTree.id] = buildObject(childTree, depth + 1);
        }
      }

      // Cache the result before returning
      memo.set(hash, result);
      return result;
    };

    // Find root nodes (not referenced by any other tree)
    const referencedHashes = new Set<string>();
    for (const tree of trees) {
      if (tree.children) {
        for (const childHash of tree.children) {
          referencedHashes.add(childHash);
        }
      }
    }

    const rootTrees = trees.filter(
      (tree) => !referencedHashes.has((tree as TreeWithHash)._hash),
    );

    /* v8 ignore next -- @preserve */
    if (rootTrees.length === 0) {
      return {};
    }

    // If single root, return its object directly
    /* v8 ignore next -- @preserve */
    if (rootTrees.length === 1) {
      const rootTree = rootTrees[0];
      /* v8 ignore else -- @preserve */
      if (rootTree.id) {
        return { [rootTree.id]: buildObject(rootTree) };
      }
      /*v8 ignore next -- @preserve */
      return buildObject(rootTree);
    }

    // Multiple roots - combine into single object
    /*v8 ignore next -- @preserve */
    const result: any = {};
    /*v8 ignore next -- @preserve */
    for (const rootTree of rootTrees) {
      /* v8 ignore else -- @preserve */
      if (rootTree.id) {
        result[rootTree.id] = buildObject(rootTree);
      }
    }
    /*v8 ignore next -- @preserve */
    return result;
  }

  async buildCellsFromTree(trees: Tree[]): Promise<Cell[]> {
    const cells: Cell[] = [];

    if (trees.length === 0) {
      return cells;
    }

    // Create maps for quick lookup
    const treeMap = new Map<string, Tree>();
    const childToParentMap = new Map<string, string>();

    for (const tree of trees) {
      const treeHash = (tree as TreeWithHash)._hash;
      treeMap.set(treeHash, tree);

      if (tree.children) {
        for (const childHash of tree.children) {
          childToParentMap.set(childHash as string, treeHash);
        }
      }
    }

    // Find all hashes present in trees array
    const availableHashes = new Set<string>();
    for (const tree of trees) {
      availableHashes.add((tree as TreeWithHash)._hash);
    }

    // Find leaf nodes (whose children are not in the trees array)
    const leafNodes = trees.filter((tree) => {
      if (!tree.children || tree.children.length === 0) {
        return true;
      }
      const hasChildInTrees = tree.children.some((childHash) =>
        availableHashes.has(childHash as string),
      );
      return !hasChildInTrees;
    });

    // For each leaf, build path from root to leaf
    for (const leaf of leafNodes) {
      const pathIds: string[] = [];
      let currentHash = (leaf as TreeWithHash)._hash;

      // Build path backwards from leaf to root
      while (currentHash) {
        const current = treeMap.get(currentHash);
        /* v8 ignore next -- @preserve */
        if (!current) break;

        /* v8 ignore else -- @preserve */
        if (current.id) {
          pathIds.unshift(current.id);
        }

        const parentHash = childToParentMap.get(currentHash);
        if (!parentHash) break;
        currentHash = parentHash;
      }

      // Create route from path
      const routeStr = '/' + pathIds.join('/');
      const route = Route.fromFlat(routeStr);

      // Create cell
      cells.push({
        route,
        value: leaf,
        row: leaf,
        path: [[this._tableKey, '_data', 0, ...pathIds]],
      });
    }

    return cells;
  }

  async getChildRefs(
    where: string | Json,
    filter?: Json,
  ): Promise<ControllerChildProperty[]> {
    const childRefs: ControllerChildProperty[] = [];
    const { [this._tableKey]: table } = await this.get(where, filter);

    const trees = table._data as TreeWithHash[];
    for (const tree of trees) {
      /* v8 ignore next -- @preserve */
      for (const treeChildRef of tree.children ?? []) {
        childRefs.push({
          tableKey: this._tableKey,
          ref: treeChildRef as string,
        });
      }
    }

    return childRefs;
  }

  /* v8 ignore next -- @preserve */
  async filterRow(): Promise<boolean> {
    return false;
  }
}
