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

  /**
   * Inserts many pre-decomposed tree nodes in a single `core.import()`
   * call — one dedup pass, one sort — instead of calling `insert()` once
   * per node. Used by `Db.insertTrees` to bulk-load large file-sync
   * catalogs (e.g. ~184,000 nodes) without paying a per-node dedup+sort
   * cost.
   *
   * Observable behavior mirrors calling `insert()` once per node: the same
   * rows end up written (import is content-addressed, so dedup is
   * order-independent), but only ONE `InsertHistoryRow` is created/returned
   * — for the root node, which by convention (shared with `treeFromObject`
   * and `Db.insertTrees`) is the LAST element of `values`.
   *
   * @param values - Pre-decomposed Tree nodes, root LAST
   * @param origin - Optional origin ref recorded on the returned InsertHistoryRow
   */
  async insertMany(
    values: Tree[],
    origin?: Ref,
  ): Promise<InsertHistoryRow<any>[]> {
    if (values.length === 0) {
      return [];
    }

    const rlJson = { [this._tableKey]: { _data: values } } as Rljson;

    // Write every node in ONE import: a single dedup + single sort for
    // the whole batch, instead of one import per node.
    await this._core.import(rlJson);

    // Root is the last node (post-order convention).
    const rootValue = values[values.length - 1];

    //Create InsertHistoryRow for the root node only
    const result = {
      //Ref to root node
      [this._tableKey + 'Ref']: hsh(rootValue as Json)._hash as string,

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

    // Expand children for route navigation. Each level of the tree is
    // fetched with ONE batch read (one socket round trip on remote ios;
    // IoMulti cascades per row) served by Core's batch row cache on
    // repeats.
    //
    // While the path still has segments, only the child matching the
    // next segment id is expanded. Once the path is exhausted, the
    // whole subtree is expanded — mirrors the previous per-node
    // recursion including its post-order output.
    const children: any[] = [];
    const treeChildren = tree.children ?? [];
    if (treeChildren.length > 0) {
      type ExpandNode = {
        hash: string;
        path: string;
        row?: Tree;
        children: ExpandNode[];
      };

      const rootPath = treeRoute.deeper().flat;
      const roots: ExpandNode[] = treeChildren.map((childRef) => ({
        hash: childRef as string,
        path: rootPath,
        children: [],
      }));

      // Fetch level by level
      let frontier = roots;
      while (frontier.length > 0) {
        const rowsByHash = await this._core.readRowsByHashes(
          this._tableKey,
          frontier.map((node) => node.hash),
        );

        const next: ExpandNode[] = [];
        for (const node of frontier) {
          const row = rowsByHash.get(node.hash) as Tree | undefined;
          if (!row) continue;

          const childRoute = Route.fromFlat(node.path || '');
          /* v8 ignore next -- @preserve */
          const childId =
            childRoute.segments.length > 0 ? childRoute.top.tableKey : null;

          /* v8 ignore next -- @preserve */
          if (childId && childId !== row.id) {
            continue;
          }

          node.row = row;
          const deeperPath = childRoute.deeper().flat;
          for (const grandChildHash of row.children ?? []) {
            const grandChild: ExpandNode = {
              hash: grandChildHash as string,
              path: deeperPath,
              children: [],
            };
            node.children.push(grandChild);
            next.push(grandChild);
          }
        }
        frontier = next;
      }

      // Emit post-order: descendants first, then the node itself
      const emit = (node: ExpandNode): void => {
        if (!node.row) return;
        for (const child of node.children) {
          emit(child);
        }
        children.push(node.row);
      };
      for (const root of roots) {
        emit(root);
      }
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

    // Safety check: prevent processing excessively large trees.
    // Raised from 100_000 -> 10_000_000 to support file-sync catalogs of
    // ~184,000 nodes (and headroom beyond that scale).
    /* v8 ignore if -- @preserve */
    if (trees.length > 10_000_000) {
      throw new Error(
        `TreeController.buildTreeFromTrees: Tree size exceeds limit (${trees.length} > 10000000 nodes). ` +
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
    // Safety limit to prevent infinite loops. Raised proportionally to the
    // node-count cap above (100_000 -> 10_000_000, i.e. 100x).
    const MAX_ITERATIONS = 20_000_000;

    // Recursive function to build object from tree
    const buildObject = (tree: Tree, depth = 0): any => {
      buildObjectCallCount++;

      // Safety check: prevent infinite loops
      /* v8 ignore if -- @preserve */
      if (buildObjectCallCount > MAX_ITERATIONS) {
        throw new Error(
          `TreeController.buildTreeFromTrees: Maximum iterations (${MAX_ITERATIONS}) exceeded. ` +
            `This likely indicates a bug. Processed ${buildObjectCallCount} nodes from ${trees.length} total.`,
        );
      }

      // Safety check: prevent stack overflow from deep nesting.
      // Left unchanged: file-sync catalogs are wide (many siblings), not
      // deep, so the node/iteration caps above are the relevant limits.
      /* v8 ignore if -- @preserve */
      if (depth > 10000) {
        throw new Error(
          `TreeController.buildTreeFromTrees: Tree depth exceeds limit (${depth} > 10000). ` +
            `This may indicate a circular reference or extremely deep structure.`,
        );
      }

      const hash = (tree as TreeWithHash)._hash;

      // Return memoized result if already processed
      /* v8 ignore if -- @preserve */
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

  /**
   * Mirrors the hash-query behavior of getChildRefs: children of an
   * already fetched tree row are not expanded to prevent infinite
   * recursion in db._get().
   */
  async getChildRefsOfRow(): Promise<ControllerChildProperty[]> {
    return [];
  }

  async getChildRefs(
    where: string | Json,
    filter?: Json,
  ): Promise<ControllerChildProperty[]> {
    // When querying by hash, don't expand children
    // This prevents infinite recursion in db._get()
    /* v8 ignore if -- @preserve */
    if (typeof where === 'string') {
      return [];
    }

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
