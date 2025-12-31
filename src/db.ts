// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hsh, rmhsh } from '@rljson/hash';
import { Io } from '@rljson/io';
import { Json, JsonValue, merge } from '@rljson/json';
import {
  Cake, CakesTable, ComponentRef, ComponentsTable, ContentType, Edit, EditHistory, EditHistoryTable,
  EditsTable, getTimeIdTimestamp, Head, InsertHistoryRow, InsertHistoryTimeId, isTimeId, Layer,
  LayersTable, MultiEdit, MultiEditsTable, Ref, Rljson, Route, RouteSegment, SliceId, SliceIds,
  TableType, timeId
} from '@rljson/rljson';

import {
  Controller, ControllerChildProperty, ControllerRefs, ControllerRunFn, createController
} from './controller/controller.ts';
import { SliceIdController } from './controller/slice-id-controller.ts';
import { Core } from './core.ts';
import { Join, JoinColumn, JoinRow, JoinRows } from './join/join.ts';
import { ColumnSelection } from './join/selection/column-selection.ts';
import { Notify, NotifyCallback } from './notify.ts';
import { makeUnique } from './tools/make-unique.ts';


export type Cell = {
  route: Route;
  value: JsonValue | JsonValue[] | null;
  row: JsonValue | JsonValue[] | null;
  path: Array<Array<string | number>>;
};

export type Container = {
  rljson: Rljson;
  tree: Json;
  cell: Cell[];
};

export type ContainerWithControllers = Container & {
  controllers: Record<string, Controller<any, any, any>>;
};

/**
 * Access Rljson data
 */
export class Db {
  /**
   * Constructor
   * @param _io - The Io instance used to read and write data
   */
  constructor(private readonly _io: Io) {
    this.core = new Core(this._io);
    this.notify = new Notify();
  }

  /**
   * Core functionalities like importing data, setting and getting tables
   */
  readonly core: Core;

  /**
   * Notification system to register callbacks on data changes
   */
  readonly notify: Notify;

  private _cache: Map<string, Container> = new Map();

  // ...........................................................................
  /**
   * Get data from a route with optional filtering
   * @param route - The route to get data from
   * @param where - Optional filter to apply to the data
   * @param filter - Optional filter to apply to child entries in related tables
   * @param sliceIds - Optional slice IDs to filter the data
   * @returns An array of Rljson objects matching the route and filter
   * @throws {Error} If the route is not valid or if any controller cannot be created
   */
  async get(
    route: Route,
    where: string | Json,
    filter?: ControllerChildProperty[],
    sliceIds?: SliceId[],
  ): Promise<ContainerWithControllers> {
    // Validate Route
    if (!route.isValid) throw new Error(`Route ${route.flat} is not valid.`);

    //Isolate Property Key
    const isolatedRoute = await this.isolatePropertyKeyFromRoute(route);

    // Get Controllers
    const controllers = await this.indexedControllers(isolatedRoute);

    // Fetch Data
    const data = await this._get(
      isolatedRoute,
      where,
      controllers,
      filter,
      sliceIds,
    );

    const dataWithControllers: ContainerWithControllers = {
      ...data,
      ...{ controllers },
    };
    return dataWithControllers;
  }

  // ...........................................................................
  /**
   * Resolves the route and returns corresponding data for any segment of the route,
   * matching recursive filters and where clauses
   *
   * @param route - The route to get data from
   * @param where - The recursive filtering key/value pairs to apply to the data
   * @param controllers - The controllers to use for fetching data
   * @param filter - Optional filter to apply to the data at the current route segment
   * @param sliceIds - Optional slice IDs to filter the data at the current route segment
   * @returns - An Rljson object matching the route and filters
   */
  async _get(
    route: Route,
    where: string | Json,
    controllers: Record<string, Controller<any, any, any>>,
    filter?: ControllerChildProperty[],
    sliceIds?: SliceId[],
    routeAccumulator?: Route,
  ): Promise<Container> {
    //Activate Cache
    const params = {
      route: route.flat,
      where,
      filter,
      sliceIds,
      routeAccumulator: routeAccumulator ? routeAccumulator.flat : '',
    };
    const cacheHash = (hsh(rmhsh(params)) as any)._hash as string;

    const isCached = this._cache.has(cacheHash);
    if (isCached) {
      return this._cache.get(cacheHash)!;
    }

    const nodeTableKey = route.top.tableKey;
    const nodeRoute = route;
    const nodeRouteRef = await this._getReferenceOfRouteSegment(nodeRoute.top);
    const nodeController = controllers[nodeTableKey];

    const nodeSliceIds = nodeRoute.top.sliceIds ?? sliceIds;

    let nodeWhere = typeof where === 'object' ? { ...where } : where;

    // If not root, remove child prompts from where
    if (!route.isRoot && typeof nodeWhere === 'object') {
      delete nodeWhere[nodeRoute.deeper().top.tableKey];
    }

    // Add ref to where
    /* v8 ignore next -- @preserve */
    nodeWhere = nodeWhere
      ? typeof nodeWhere === 'string'
        ? { _hash: nodeWhere }
        : nodeWhere
      : {};

    //  Add route ref to where
    if (nodeRouteRef && nodeRouteRef.length > 0)
      nodeWhere = { _hash: nodeRouteRef };

    // Delete internal flags
    delete (nodeWhere as Json)['_through'];
    delete (nodeWhere as Json)['_tableKey'];

    // Fetch Node Data (actual access to underlying data model)
    const {
      [nodeTableKey]: { _data: nodeRows, _type: nodeType, _hash: nodeHash },
    } = await nodeController.get(nodeWhere);
    const nodeColumnCfgs = nodeController.tableCfg().columns;

    const nodeRowsFiltered: Json[] = [];
    for (const nodeRow of nodeRows) {
      const filterActive = filter && filter.length > 0;
      const sliceIdActive = nodeSliceIds && nodeSliceIds.length > 0;

      if (!filterActive && !sliceIdActive) {
        nodeRowsFiltered.push(nodeRow);
        continue;
      }

      // Apply Filters
      let filterResult = false;
      const filterProperties: ControllerChildProperty[] = [];
      if (filterActive) {
        for (const f of filter) {
          if (f.tableKey !== nodeTableKey) continue;
          if (nodeRow._hash === f.ref) {
            filterProperties.push(f);
            filterResult = true;
          }
        }
      } else {
        filterResult = true;
      }

      // Apply SliceIds
      let sliceIdResult = false;
      if (sliceIdActive) {
        switch (nodeType) {
          case 'cakes':
            const cake = nodeRow as Cake;
            const cakeSliceIds = await this._resolveSliceIds(
              cake.sliceIdsTable,
              cake.sliceIdsRow,
            );
            const cakeMatchesSliceIds = nodeSliceIds.filter((sId) =>
              cakeSliceIds.includes(sId),
            );
            if (cakeMatchesSliceIds.length > 0) sliceIdResult = true;
            break;

          case 'layers':
            const layer = nodeRow as Layer;
            const layerSliceIds = await this._resolveSliceIds(
              layer.sliceIdsTable,
              layer.sliceIdsTableRow,
            );
            const layerMatchesSliceIds = nodeSliceIds.filter((sId) =>
              layerSliceIds.includes(sId),
            );
            if (layerMatchesSliceIds.length > 0) sliceIdResult = true;
            break;
          case 'components':
            if (filterProperties.length > 0) {
              const componentSliceIds = filterProperties.flatMap(
                (f) => f.sliceIds,
              );
              const componentMatchesSliceIds = nodeSliceIds.filter((sId) =>
                componentSliceIds.includes(sId),
              );
              if (componentMatchesSliceIds.length > 0) {
                sliceIdResult = true;
              }
            }
            break;
          /* v8 ignore next -- @preserve */
          default:
            sliceIdResult = true;
            break;
        }
      } else {
        sliceIdResult = true;
      }

      if (filterResult && sliceIdResult) nodeRowsFiltered.push(nodeRow);
    }

    // Construct Node w/ only filtered rows
    const node = {
      [nodeTableKey]: {
        _data: nodeRowsFiltered,
        _type: nodeType,
        _hash: nodeHash,
      },
    } as Rljson;

    // Return if is root node (deepest level, base case)
    const nodeValue = node[nodeTableKey]._data.filter(
      (v) => v !== undefined && v !== null,
    );
    if (route.isRoot) {
      if (route.hasPropertyKey) {
        const isolatedNode = this.isolatePropertyFromComponents(
          node,
          route.propertyKey!,
        );

        const result = {
          rljson: isolatedNode,
          tree: { [nodeTableKey]: node[nodeTableKey] },
          cell: nodeValue.map(
            (v, idx) =>
              ({
                value: v[route.propertyKey!] ?? null,
                row: v,
                route: Route.fromFlat(
                  (routeAccumulator ? routeAccumulator.flat : nodeTableKey) +
                    (nodeHash ? `@${nodeHash}` : '') +
                    `/${route.propertyKey}`,
                ).toRouteWithProperty(),
                path: [[nodeTableKey, '_data', idx, route.propertyKey]],
              } as Cell),
          ) as Cell[],
        };

        //Set Cache
        this._cache.set(cacheHash, result);

        return result;
      }

      const result = {
        rljson: node,
        tree: { [nodeTableKey]: node[nodeTableKey] },
        cell: nodeValue.map(
          (v, idx) =>
            ({
              value: v[route.propertyKey!] ?? null,
              row: v,
              route: Route.fromFlat(
                (routeAccumulator ? routeAccumulator.flat : nodeTableKey) +
                  (nodeHash ? `@${nodeHash}` : ''),
              ),
              path: [[nodeTableKey, '_data', idx]],
            } as Cell),
        ) as Cell[],
      };
      //Set Cache
      this._cache.set(cacheHash, result);

      return result;
    }

    // Fetch Children Data
    const childrenRoute = route.deeper();
    const childrenTableKey = childrenRoute.top.tableKey;
    /* v8 ignore next -- @preserve */
    const childrenWhere = (
      typeof where === 'object' ? where[childrenTableKey] ?? {} : {}
    ) as Json | string;

    const childrenThroughProperty = (childrenWhere as any)?._through;

    const nodeChildrenArray = [];

    const nodeRowsMatchingChildrenRefs = new Map<
      string,
      {
        rljson: Json;
        tree: Json;
        cell: Cell[];
      }
    >();

    // Iterate over Node Rows to get Children
    for (let i = 0; i < nodeRowsFiltered.length; i++) {
      const nodeRow = nodeRowsFiltered[i];
      const nodeRowObj = { ...{}, ...nodeRow } as Json;

      // Child References of this Node Row = Filter for Children
      const childrenRefs = await nodeController.getChildRefs(
        (nodeRow as any)._hash,
      );

      // If cake is referenced, we have to collect all sliceIds from
      // childrenRefs and switch to them
      const childrenRefTypes = new Map<string, string>();
      const childrenRefSliceIds = new Set<SliceId>();

      for (const cr of childrenRefs) {
        if (!!cr.columnKey) {
          const childrenRefColumnCfg = nodeColumnCfgs.find(
            (c) => c.key === cr.columnKey,
          );

          /* v8 ignore else -- @preserve */
          if (childrenRefColumnCfg) {
            /* v8 ignore next -- @preserve */
            childrenRefTypes.set(
              childrenRefColumnCfg.key,
              childrenRefColumnCfg.ref?.type ?? '',
            );
          }

          if (cr.sliceIds && cr.sliceIds.length > 0) {
            //Cake is referenced, switch sliceIds
            for (const sId of cr.sliceIds) {
              childrenRefSliceIds.add(sId);
            }
          }
        }
      }

      const childrenRefTypesSet = new Set<string>([
        ...childrenRefTypes.values(),
      ]);

      if (childrenRefTypesSet.size > 1) {
        throw new Error(
          `Db._get: Multiple reference types found for children of node table "${nodeTableKey}" and row "${
            (nodeRow as any)._hash
          }". Found types: ${[...childrenRefTypesSet].join(', ')}.`,
        );
      }

      const childrenRefType =
        childrenRefTypesSet.size > 0 ? [...childrenRefTypesSet][0] : null;

      const cakeIsReferenced =
        childrenRefTypes.size > 0 && childrenRefType === 'cakes';

      const componentIsReferenced =
        childrenRefTypes.size > 0 &&
        nodeType === 'components' &&
        childrenRefType === 'components';

      const childrenSliceIds = cakeIsReferenced
        ? [...childrenRefSliceIds]
        : componentIsReferenced
        ? undefined
        : nodeSliceIds;

      const {
        rljson: rowChildrenRljson,
        tree: rowChildrenTree,
        cell: rowChildrenCell,
      } = await this._get(
        childrenRoute,
        childrenWhere,
        controllers,
        childrenRefs,
        childrenSliceIds,
        Route.fromFlat(
          (routeAccumulator ? routeAccumulator.flat : nodeTableKey) +
            (nodeHash ? `@${nodeHash}` : '') +
            '/' +
            childrenTableKey,
        ),
      );

      if (cakeIsReferenced) {
        const refKey = [...childrenRefTypes.keys()][0] as string;
        nodeRowObj[refKey] = rowChildrenTree;
      }

      // No Children found for where + route => skip
      if (rowChildrenRljson[childrenTableKey]._data.length === 0) continue;

      nodeChildrenArray.push(rowChildrenRljson);

      // Add Children as ThroughProperty value to Object representation
      if (childrenThroughProperty) {
        const resolvedChildrenHashes = rowChildrenRljson[
          childrenTableKey
        ]._data.map((rc) => rc._hash as string);
        for (const nr of nodeRowsFiltered) {
          {
            const throughHashesInRowCouldBeArray = (nr as any)[
              childrenThroughProperty
            ];
            const throughHashesInRow = Array.isArray(
              throughHashesInRowCouldBeArray,
            )
              ? throughHashesInRowCouldBeArray
              : [throughHashesInRowCouldBeArray];

            for (const th of throughHashesInRow) {
              if (resolvedChildrenHashes.includes(th)) {
                //Add Child as ThroughProperty value
                nodeRowObj[childrenThroughProperty] = {
                  ...(rowChildrenTree as any)[childrenTableKey],
                  ...{ _tableKey: childrenTableKey },
                };
              }
            }
          }
        }
      }

      const resolvedChildren = rowChildrenRljson[childrenTableKey]
        ._data as Json[];

      const childrenRefsOfRow = childrenRefs.filter(
        (cr) => cr.tableKey == childrenTableKey,
      );

      const matchingChildrenRefs = childrenRefsOfRow.filter(
        (cr) => !!resolvedChildren.find((ch) => cr.ref === ch._hash),
      );

      // If Layer, construct layer objects with sliceIds relations
      /* v8 ignore else -- @preserve */
      if (nodeType === 'layers') {
        const compChildrenTrees = (
          (rowChildrenTree as any)[childrenTableKey]! as ComponentsTable<Json>
        )._data;
        const compChildrenPaths = rowChildrenCell.map((c) => c.path);

        const components = compChildrenTrees.map((c, idx) => {
          return {
            tree: c,
            path: compChildrenPaths.filter((p) => p[0][2] == idx),
          };
        });

        const layerTreesAndPaths = components.map(({ tree: comp, path }) => {
          const sliceIds = matchingChildrenRefs.find(
            (cr) => cr.ref === comp._hash,
          )?.sliceIds;

          /* v8 ignore next -- @preserve */
          if (!sliceIds || sliceIds.length === 0) {
            throw new Error(
              `Db._get: No sliceIds found for component ${
                comp._hash
              } of layer ${(nodeRow as any)._hash}.`,
            );
          }
          /* v8 ignore next -- @preserve */
          if (sliceIds.length > 1) {
            throw new Error(
              `Db._get: Multiple sliceIds found for component ${
                comp._hash
              } of layer ${(nodeRow as any)._hash}.`,
            );
          }

          const sliceId = sliceIds[0];

          const pathsForSliceId = [
            ...path
              .map((p) => p[0])
              .map((p) => {
                const newPath = [...p];
                newPath[2] = 0;
                return ['add', sliceId, ...newPath];
              }),
          ];

          return {
            [sliceId]: {
              tree: {
                [childrenTableKey]: {
                  ...{ _data: [comp] },
                  ...{ _type: 'components' },
                },
              },
              path: pathsForSliceId,
            },
          };
        });

        const layer = layerTreesAndPaths
          .flatMap((ltap) =>
            Object.entries(ltap).map(([sliceId, { tree }]) => ({
              [sliceId]: tree,
            })),
          )
          .reduce((a, b) => ({ ...a, ...b }), {});

        const paths = layerTreesAndPaths.map(
          (ltap) => Object.values(ltap)[0].path,
        );

        nodeRowsMatchingChildrenRefs.set((nodeRow as any)._hash, {
          rljson: nodeRow,
          tree: {
            ...nodeRowObj,
            add: { ...(nodeRowObj.add as Json), ...layer },
          },
          cell: rowChildrenCell.map(
            (c, idx) =>
              ({
                ...c,
                ...{
                  path: [paths.flat()[idx]],
                },
              } as Cell),
          ),
        });
      } else if (nodeType === 'cakes') {
        nodeRowsMatchingChildrenRefs.set((nodeRow as any)._hash, {
          rljson: nodeRow,
          tree: {
            ...nodeRowObj,
            layers: { ...(nodeRowObj.layers as Json), ...rowChildrenTree },
          },
          cell: rowChildrenCell.map((c) => ({
            ...c,
            ...{
              path: c.path.map((p) => ['layers', ...p]),
            },
          })),
        });
      } else if (nodeType === 'components') {
        /* v8 ignore else -- @preserve */
        if (rowChildrenTree && Object.keys(rowChildrenTree).length > 0) {
          const columnReferenceMap = nodeColumnCfgs
            .filter((c) => c.ref?.tableKey === childrenTableKey)
            .filter(
              (c) => c.ref && ['components', 'cakes'].includes(c.ref.type),
            )
            .reduce((acc, curr) => {
              acc.set(curr.key, curr.ref!.tableKey);
              return acc;
            }, new Map<string, string>());

          const resolvedRefs: Record<string, { tree: Json; cell: Cell[] }> = {};
          for (const [colKey, childTableKey] of columnReferenceMap) {
            const tree = {
              ...(rowChildrenTree[childTableKey] as Json),
              ...{ _tableKey: childTableKey },
            };
            const cell = rowChildrenCell.map((c) => ({
              ...c,
              ...{
                path: c.path
                  .filter((p) => p[0] === childTableKey)
                  .map((p) => [colKey, ...p.slice(1)]),
              },
            }));

            resolvedRefs[colKey] = { tree, cell };
          }

          const resolvedProperties = Object.entries(resolvedRefs)
            .map(([colKey, { tree }]) => ({
              [colKey]: tree,
            }))
            .reduce((a, b) => ({ ...a, ...b }), {});

          const resolvedTree = {
            ...nodeRowObj,
            ...resolvedProperties,
          };

          const resolvedCell = Object.values(resolvedRefs)
            .map((r) => r.cell)
            .flat();

          nodeRowsMatchingChildrenRefs.set((nodeRow as any)._hash, {
            rljson: nodeRow,
            tree: resolvedTree,
            cell: resolvedCell,
          });
        } else {
          nodeRowsMatchingChildrenRefs.set((nodeRow as any)._hash, {
            rljson: nodeRow,
            tree: { ...nodeRowObj },
            cell: rowChildrenCell,
          });
        }
      } else {
        throw new Error(
          `Db._get: Unsupported node type ${nodeType} for getting children.`,
        );
      }
    }

    // Merge Children Data
    const nodeChildren = makeUnique(
      merge(...(nodeChildrenArray as Rljson[])) as Rljson,
    );

    // Return Node with matched Children
    const matchedNodeRows = Array.from(nodeRowsMatchingChildrenRefs.values());

    const result = {
      rljson: {
        ...node,
        ...{
          [nodeTableKey]: {
            ...{
              _data: matchedNodeRows.map((mr) => mr.rljson),
              _type: nodeType,
            },
            ...{
              ...(nodeHash ? { _hash: nodeHash } : {}),
            },
          },
        },
        ...nodeChildren,
      } as Rljson,
      tree: {
        [nodeTableKey]: {
          ...{
            _data: matchedNodeRows.map((mr) => mr.tree),
            _type: nodeType,
          },
          ...{
            ...(nodeHash ? { _hash: nodeHash } : {}),
          },
        },
      },
      cell: matchedNodeRows
        .map((mr, idx) =>
          mr.cell.map((c) => ({
            ...c,
            ...{ path: c.path.map((p) => [nodeTableKey, '_data', idx, ...p]) },
          })),
        )
        .flat(),
    };
    //Set Cache
    this._cache.set(cacheHash, result);

    return result;
  }

  // ...........................................................................
  /**
   * Get the reference (hash) of a route segment, considering default refs and insertHistory refs
   * @param segment - The route segment to get the reference for
   * @returns
   */
  private async _getReferenceOfRouteSegment(
    segment: RouteSegment<any>,
  ): Promise<string | null> {
    if (Route.segmentHasRef(segment)) {
      if (Route.segmentHasDefaultRef(segment)) {
        // Use given ref
        return Route.segmentRef(segment)!;
      } else {
        // Get ref from insertHistory
        return (await this.getRefOfTimeId(
          segment.tableKey,
          Route.segmentRef(segment)!,
        ))!;
      }
    }
    return null;
  }

  // ...........................................................................
  /**
   * Joins data from layers in an Rljson into a single dataset
   * @param rljson - The Rljson to join data for
   */
  async join(
    columnSelection: ColumnSelection,
    cakeKey: string,
    cakeRef: Ref,
  ): Promise<Join> {
    const {
      tree: { [cakeKey]: cakesTable },
    } = await this.get(Route.fromFlat(`${cakeKey}@${cakeRef}`), {});

    const cakes = (cakesTable as CakesTable)._data;

    /* v8 ignore next -- @preserve */
    if (cakes.length === 0) {
      throw new Error(
        `Db.join: Cake with ref "${cakeRef}" not found in cake table "${cakeKey}".`,
      );
    }
    /* v8 ignore next -- @preserve */
    if (cakes.length > 1) {
      throw new Error(
        `Db.join: Multiple cakes with ref "${cakeRef}" found in cake table "${cakeKey}".`,
      );
    }
    const cake = cakes[0] as Cake;

    const sliceIds = await this._resolveSliceIds(
      (cake as Cake).sliceIdsTable,
      (cake as Cake).sliceIdsRow,
    );
    const rows: JoinRows = {};
    for (const sliceId of sliceIds) {
      const row: JoinRow = [];

      for (const columnInfo of columnSelection.columns) {
        const columnRoute = Route.fromFlat(
          columnInfo.route,
        ).toRouteWithProperty();

        const columnContainer = await this.get(
          columnRoute,
          cakeRef,
          undefined,
          [sliceId],
        );

        const column: JoinColumn = {
          route: columnRoute,
          value: columnContainer,
          inserts: null,
        };
        row.push(column);
      }

      rows[sliceId] = row;
    }

    // Return Join
    return new Join(rows, columnSelection);
  }

  // ...........................................................................
  private async _resolveSliceIds(
    sliceIdTable: string,
    sliceIdRow: string,
  ): Promise<SliceId[]> {
    const sliceIdController: SliceIdController<any, any> =
      new SliceIdController(this.core, sliceIdTable);
    sliceIdController.init();

    const resolvedSliceIds: Set<SliceId> = new Set();

    const {
      [sliceIdTable]: { _data: sliceIds },
    } = await sliceIdController.get(sliceIdRow);

    for (const sliceId of sliceIds) {
      const baseSliceIds = await sliceIdController.resolveBaseSliceIds(
        sliceId as SliceIds,
      );
      for (const sId of baseSliceIds.add) {
        resolvedSliceIds.add(sId);
      }
    }

    return Array.from(resolvedSliceIds);
  }

  // ...........................................................................
  /**
   * Runs an Insert by executing the appropriate controller(s) based on the Insert's route
   * @param Insert - The Insert to run
   * @returns The result of the Insert as an InsertHistoryRow
   * @throws {Error} If the Insert is not valid or if any controller cannot be created
   */
  async insert(
    route: Route,
    tree: Json,
    options?: { skipNotification?: boolean; skipHistory?: boolean },
  ): Promise<InsertHistoryRow<any>[]> {
    const controllers = await this.indexedControllers(
      Route.fromFlat(route.flatWithoutRefs),
    );
    const runFns: Record<string, ControllerRunFn<any, any>> = {};
    for (const [tableKey, controller] of Object.entries(controllers)) {
      runFns[tableKey] = controller.insert.bind(controller);
    }
    return this._insert(route, tree, runFns, options);
  }

  // ...........................................................................
  /**
   * Recursively runs controllers based on the route of the Insert
   * @param insert - The Insert to run
   * @param route - The route of the Insert
   * @param runFns - A record of controller run functions, keyed by table name
   * @returns The result of the Insert
   * @throws {Error} If the route is not valid or if any controller cannot be created
   */
  private async _insert(
    route: Route,
    tree: Json,
    runFns: Record<string, ControllerRunFn<any, any>>,
    options?: { skipNotification?: boolean; skipHistory?: boolean },
  ): Promise<InsertHistoryRow<any>[]> {
    const results: InsertHistoryRow<any>[] = [];

    //Run parent controller with child refs as value
    const nodeRoute = route;
    const nodeSegment = nodeRoute.segment(0);
    const nodeTableKey = nodeSegment.tableKey;
    const nodeTree = (tree[nodeTableKey] as TableType)!;
    const nodeType = nodeTree._type as ContentType;

    /* v8 ignore next -- @preserve */
    if (nodeTree._data.length === 0) {
      throw new Error(
        `Db._insert: No data found for table "${nodeTableKey}" in route "${route.flat}".`,
      );
    }

    const previousHash = (nodeSegment as any)[nodeTableKey + 'Ref'] ?? null;
    const previousTimeId: string | null =
      (nodeSegment as any)[nodeTableKey + 'InsertHistoryRef'] ?? null;

    const previous: InsertHistoryTimeId[] = previousHash
      ? await this.getTimeIdsForRef(nodeTableKey, previousHash as string)
      : previousTimeId
      ? [previousTimeId]
      : [];

    //If not root, run nested controllers first
    if (!nodeRoute.isRoot) {
      //Run nested controller first
      const childRoute = nodeRoute.deeper(1);
      const childTableKey = childRoute.top.tableKey;

      if (nodeType === 'cakes') {
        const cakes = (nodeTree as CakesTable)._data;

        /* v8 ignore next -- @preserve */
        if (cakes.length > 1) {
          // throw new Error(
          //   `Db._insert: Multiple cakes found for cake table "${nodeTableKey}" when inserting into child table "${childTableKey}". Only single cake inserts are supported.`,
          // );
        }

        //Check if there is no cake or no childTree --> Add new one
        const cake = cakes[0] as Cake;
        const childTree = (cake.layers as Json)[childTableKey] as TableType;
        const childResults = await this._insert(
          childRoute,
          { [childTableKey]: childTree },
          runFns,
        );

        /* v8 ignore next -- @preserve */
        if (childResults.length > 1) {
          throw new Error(
            `Db._insert: Multiple inserts returned for child table "${childTableKey}" when inserting into cake table "${nodeTableKey}". Only single child inserts are supported.`,
          );
        }

        const childResult = childResults[0];

        const insertValue = {
          ...(cake as any as Json),
          ...{
            layers: {
              ...cake.layers,
              ...{
                [childTableKey]: (childResult as any)[childTableKey + 'Ref'],
              },
            },
          },
        };
        const runFn = runFns[nodeTableKey];
        const result = await runFn('add', rmhsh(insertValue), 'db.insert');
        results.push(
          ...result.map((r) => ({
            ...r,
            ...{ previous },
            ...{ route: route.flat },
          })),
        );
      }
      if (nodeType === 'layers') {
        const layers = (nodeTree as LayersTable)._data;
        for (const layer of layers) {
          const layerInsert: Record<SliceId, ComponentRef> = {};

          //Check what if there is no layer or no compomentTree --> Add new one

          for (const [sliceId, componentTree] of Object.entries(layer.add)) {
            if (sliceId === '_hash') continue;

            const writtenComponents = await this._insert(
              childRoute,
              componentTree as any,
              runFns,
            );

            /* v8 ignore next -- @preserve */
            if (writtenComponents.length > 1) {
              throw new Error(
                `Db._insert: Multiple components written for layer "${
                  (layer as any)._hash
                }" and sliceId "${sliceId}" is currently not supported.`,
              );
            }

            const writtenComponent = writtenComponents[0];

            /* v8 ignore next -- @preserve */
            if (
              !writtenComponent ||
              !(writtenComponent as any)[childTableKey + 'Ref']
            ) {
              throw new Error(
                `Db._insert: No component reference returned for layer "${
                  (layer as any)._hash
                }" and sliceId "${sliceId}".`,
              );
            }

            layerInsert[sliceId] = (writtenComponent as any)[
              childTableKey + 'Ref'
            ];
          }
          const runFn = runFns[nodeTableKey];
          const result = await runFn(
            'add',
            rmhsh({
              ...layer,
              ...{ add: layerInsert },
            }),
            'db.insert',
          );
          results.push(
            ...result.map((r) => ({
              ...r,
              ...{ previous },
              ...{ route: route.flat },
            })),
          );
        }
      }
      if (
        (
          [
            'components',
            'edits',
            'multiEdits',
            'editHistory',
            'head',
          ] as ContentType[]
        ).includes(nodeType)
      ) {
        const runFn = runFns[nodeTableKey];
        const components = (nodeTree as ComponentsTable<Json>)._data;
        for (const component of components) {
          const resolvedComponent = { ...component } as Json;
          for (const [property, value] of Object.entries(component)) {
            if (
              (value as any).hasOwnProperty('_tableKey') &&
              (value as any)._tableKey === childTableKey
            ) {
              const writtenReferences = await this._insert(
                childRoute,
                { [childTableKey]: value as any },
                runFns,
              );
              resolvedComponent[property] = writtenReferences.map(
                (wr) => (wr as any)[childTableKey + 'Ref'],
              );
            }
          }

          const result = await runFn(
            'add',
            rmhsh(resolvedComponent),
            'db.insert',
          );
          results.push(
            ...result.map((r) => ({
              ...r,
              ...{ previous },
              ...{ route: route.flat },
            })),
          );
        }
      }
    } else {
      //Run root controller
      const runFn = runFns[nodeTableKey];

      if (
        (
          [
            'components',
            'edits',
            'multiEdits',
            'editHistory',
            'head',
          ] as ContentType[]
        ).includes(nodeType)
      ) {
        const components = rmhsh(
          (tree as any)[nodeTableKey],
        ) as ComponentsTable<Json>;

        for (const component of components._data) {
          /* v8 ignore next -- @preserve */
          if (!component) continue;

          delete (component as any)._tableKey;
          delete (component as any)._type;

          const result = await runFn('add', component, 'db.insert');
          results.push(
            ...result.map((r) => ({
              ...r,
              ...{ previous },
              ...{ route: route.flat },
            })),
          );
        }
      }
      if (nodeType === 'layers') {
        const layers = rmhsh((tree as any)[nodeTableKey]);
        for (const layer of (layers as LayersTable)._data) {
          const result = await runFn('add', layer, 'db.insert');

          results.push(
            ...result.map((r) => ({
              ...r,
              ...{ previous },
              ...{ route: route.flat },
            })),
          );
        }
      }
      if (nodeType === 'cakes') {
        const cakes = rmhsh((tree as any)[nodeTableKey]);
        for (const cake of (cakes as CakesTable)._data) {
          const result = await runFn('add', cake, 'db.insert');

          results.push(
            ...result.map((r) => ({
              ...r,
              ...{ previous },
              ...{ route: route.flat },
            })),
          );
        }
      }
    }

    for (const result of results) {
      //Write insertHistory
      if (!options?.skipHistory)
        await this._writeInsertHistory(nodeTableKey, result);

      //Notify listeners
      if (!options?.skipNotification)
        this.notify.notify(Route.fromFlat(result.route), result);
    }

    return results;
  }

  // ...........................................................................
  /**
   * Registers a callback to be called when an Insert is made on the given route
   * @param route - The route to register the callback on
   * @param callback - The callback to be called when an Insert is made
   */
  registerObserver(route: Route, callback: NotifyCallback<any>) {
    this.notify.register(route, callback);
  }

  // ...........................................................................
  /**
   * Unregisters a callback from the given route
   * @param route - The route to unregister the callback from
   * @param callback - The callback to be unregistered
   */
  unregisterObserver(route: Route, callback: NotifyCallback<any>) {
    this.notify.unregister(route, callback);
  }

  // ...........................................................................
  /**
   * Unregisters all observers from all routes
   */
  unregisterAllObservers(route: Route) {
    this.notify.unregisterAll(route);
  }

  // ...........................................................................
  /**
   * Get a controller for a specific table
   * @param tableKey - The key of the table to get the controller for
   * @param refs - Optional references required by some controllers
   * @returns A controller for the specified table
   * @throws {Error} If the table does not exist or if the table type is not supported
   */
  async getController(tableKey: string, refs?: ControllerRefs) {
    // Validate Table
    const hasTable = await this.core.hasTable(tableKey);
    if (!hasTable) {
      throw new Error(`Db.getController: Table ${tableKey} does not exist.`);
    }

    // Get Content Type of Table
    const contentType = await this.core.contentType(tableKey);

    // Create Controller
    return createController(contentType, this.core, tableKey, refs);
  }

  // ...........................................................................
  public async indexedControllers(
    route: Route,
  ): Promise<Record<string, Controller<any, any, any>>> {
    // Create Controllers
    const controllers: Record<string, Controller<any, any, any>> = {};
    const isolatedRoute = await this.isolatePropertyKeyFromRoute(route);

    for (let i = 0; i < isolatedRoute.segments.length; i++) {
      const segment = isolatedRoute.segments[i];
      const tableKey = segment.tableKey;
      //Base is helpful for cake and layer controllers. It contains the ref of
      // the base cake for taking over default layers definitions
      const segmentRef = Route.segmentRef(segment);
      const base = segmentRef
        ? isTimeId(segmentRef)
          ? await this.getRefOfTimeId(tableKey, segmentRef)
          : segmentRef
        : null;

      controllers[tableKey] ??= await this.getController(
        tableKey,
        base ? ({ base } as ControllerRefs) : undefined,
      );
    }

    return controllers;
  }

  // ...........................................................................
  /**
   * Adds an InsertHistory row to the InsertHistory table of a table
   * @param table - The table the Insert was made on
   * @param InsertHistoryRow - The InsertHistory row to add
   * @throws {Error} If the InsertHistory table does not exist
   */
  private async _writeInsertHistory(
    table: string,
    insertHistoryRow: InsertHistoryRow<any>,
  ): Promise<void> {
    const insertHistoryTable = table + 'InsertHistory';

    //Write InsertHistory row to io
    await this.core.import({
      [insertHistoryTable]: {
        _data: [insertHistoryRow],
        _type: 'insertHistory',
      },
    });
  }

  // ...........................................................................
  /**
   * Add a head revision for a cake
   * @param cakeKey - The cake table key
   * @param cakeRef - The cake reference
   */
  public async addHeadRevision(cakeKey: string, cakeRef: Ref) {
    const cakeHeadKey = cakeKey + 'Heads';
    const cakeHeadController = await this.getController(cakeHeadKey);

    return await cakeHeadController.insert('add', {
      cakeRef,
      timeId: timeId(),
      _hash: '',
    } as Head);
  }

  // ...........................................................................
  /**
   * Add a multiEdit
   * @param cakeKey - The cake table key
   * @param multiEdit - The multiEdit to add
   */
  public async addMultiEdit(cakeKey: string, multiEdit: MultiEdit) {
    return this.insert(
      Route.fromFlat(cakeKey + 'MultiEdits'),
      {
        [cakeKey + 'MultiEdits']: {
          _data: [multiEdit],
          _type: 'multiEdits' as ContentType,
        } as MultiEditsTable,
      },
      { skipHistory: true },
    );
  }

  // ...........................................................................
  /**
   * Get multiEdits
   * @param cakeKey - The cake table key
   * @param where - The where clause to filter multiEdits
   */
  public async getMultiEdits(
    cakeKey: string,
    where: string | Json,
  ): Promise<MultiEdit[]> {
    const multiEditController = await this.getController(
      cakeKey + 'MultiEdits',
    );
    const { [cakeKey + 'MultiEdits']: result } = await multiEditController.get(
      where,
    );
    return result._data as MultiEdit[];
  }

  // ...........................................................................
  /**
   * Add an edit
   * @param cakeKey - The cake table key
   * @param edit - The edit to add
   */
  public async addEdit(cakeKey: string, edit: Edit) {
    return this.insert(
      Route.fromFlat(cakeKey + 'Edits'),
      {
        [cakeKey + 'Edits']: {
          _data: [edit],
          _type: 'edits' as ContentType,
        } as EditsTable,
      },
      { skipHistory: true },
    );
  }

  // ...........................................................................
  /**
   * Get edits
   * @param cakeKey - The cake table key
   * @param where - The where clause to filter edits
   */
  public async getEdits(
    cakeKey: string,
    where: string | Json,
  ): Promise<Edit[]> {
    const editController = await this.getController(cakeKey + 'Edits');
    const { [cakeKey + 'Edits']: result } = await editController.get(where);
    return result._data as Edit[];
  }

  // ...........................................................................
  /**
   * Add an edit history entry
   * @param cakeKey - The cake table key
   * @param editHistory - The edit history entry to add
   */
  public async addEditHistory(cakeKey: string, editHistory: EditHistory) {
    return this.insert(
      Route.fromFlat(cakeKey + 'EditHistory'),
      {
        [cakeKey + 'EditHistory']: {
          _data: [editHistory],
          _type: 'editHistory' as ContentType,
        } as EditHistoryTable,
      },
      { skipHistory: true },
    );
  }

  // ...........................................................................
  /**
   * Get edit history entries
   * @param cakeKey - The cake table key
   * @param where - The where clause to filter edit history entries
   */
  public async getEditHistories(
    cakeKey: string,
    where: string | Json,
  ): Promise<EditHistory[]> {
    const editHistoryController = await this.getController(
      cakeKey + 'EditHistory',
    );

    const { [cakeKey + 'EditHistory']: result } =
      await editHistoryController.get(where);

    /* v8 ignore next -- @preserve */
    return result._data.sort(
      (h1, h2) =>
        getTimeIdTimestamp(h2.timeId)! - getTimeIdTimestamp(h1.timeId)!,
    ) as EditHistory[];
  }

  // ...........................................................................
  /**
   * Get the InsertHistory of a table
   * @param table - The table to get the InsertHistory for
   * @throws {Error} If the InsertHistory table does not exist
   */
  async getInsertHistory(
    table: string,
    options?: { sorted?: boolean; ascending?: boolean },
  ): Promise<Rljson> {
    const insertHistoryTable = table + 'InsertHistory';
    const hasTable = await this.core.hasTable(insertHistoryTable);
    if (!hasTable) {
      throw new Error(`Db.getInsertHistory: Table ${table} does not exist`);
    }

    if (options === undefined) {
      options = { sorted: false, ascending: true };
    }

    if (options.sorted) {
      const dumpedTable = await this.core.dumpTable(insertHistoryTable);
      const tableData = dumpedTable[insertHistoryTable]
        ._data as InsertHistoryRow<any>[];

      //Sort table
      tableData.sort((a, b) =>
        options!.ascending
          ? a.timeId.localeCompare(b.timeId)
          : b.timeId.localeCompare(a.timeId),
      );

      return {
        [insertHistoryTable]: { _data: tableData, _type: 'insertHistory' },
      };
    }

    return this.core.dumpTable(insertHistoryTable);
  }

  // ...........................................................................
  /**
   * Get a specific InsertHistory row from a table
   * @param table - The table to get the InsertHistory row from
   * @param ref - The reference of the InsertHistory row to get
   * @returns The InsertHistory row or null if it does not exist
   * @throws {Error} If the Inserts table does not exist
   */
  async getInsertHistoryRowsByRef(
    table: string,
    ref: string,
  ): Promise<InsertHistoryRow<any>[]> {
    const insertHistoryTable = table + 'InsertHistory';
    const {
      [insertHistoryTable]: { _data: insertHistory },
    } = await this.core.readRows(insertHistoryTable, { [table + 'Ref']: ref });
    return insertHistory as InsertHistoryRow<any>[];
  }

  // ...........................................................................
  /**
   * Get a specific InsertHistory row from a table by its timeId
   * @param table - The table to get the InsertHistory row from
   * @param timeId - The timeId of the InsertHistory row to get
   * @returns The InsertHistory row or null if it does not exist
   * @throws {Error} If the Inserts table does not exist
   */
  async getInsertHistoryRowByTimeId(
    table: string,
    timeId: InsertHistoryTimeId,
  ): Promise<InsertHistoryRow<any>> {
    const insertHistoryTable = table + 'InsertHistory';
    const { [insertHistoryTable]: result } = await this.core.readRows(
      insertHistoryTable,
      {
        timeId,
      },
    );
    return result._data?.[0];
  }

  // ...........................................................................
  /**
   * Get all timeIds for a specific ref in a table
   * @param table - The table to get the timeIds from
   * @param ref - The reference to get the timeIds for
   * @returns An array of timeIds
   * @throws {Error} If the Inserts table does not exist
   */
  async getTimeIdsForRef(
    table: string,
    ref: Ref,
  ): Promise<InsertHistoryTimeId[]> {
    const insertHistoryTable = table + 'InsertHistory';
    const { [insertHistoryTable]: result } = await this.core.readRows(
      insertHistoryTable,
      {
        [table + 'Ref']: ref,
      },
    );
    return result._data?.map((r) => r.timeId);
  }

  // ...........................................................................
  /**
   * Get the ref for a specific timeId in a table
   * @param table - The table to get the ref from
   * @param timeId - The timeId to get the ref for
   * @returns The ref or null if it does not exist
   * @throws {Error} If the Inserts table does not exist
   */
  async getRefOfTimeId(
    table: string,
    timeId: InsertHistoryTimeId,
  ): Promise<Ref | null> {
    const insertHistoryTable = table + 'InsertHistory';
    const { [insertHistoryTable]: result } = await this.core.readRows(
      insertHistoryTable,
      {
        timeId,
      },
    );
    return (result._data?.[0] as any)?.[table + 'Ref'];
  }

  // ...........................................................................
  /**
   * Isolates the property key from the last segment of a route if it is a property of a component
   * @param route - The route to extract property key from
   * @returns A route with extracted property key
   */
  async isolatePropertyKeyFromRoute(route: Route): Promise<Route> {
    const segmentLength = route.segments.length;
    let propertyKey = '';
    let result: Route = route;
    for (let i = segmentLength; i > 0; i--) {
      const segment = route.segments[i - 1];
      const tableKey = segment.tableKey;
      const tableExists = await this._io.tableExists(tableKey);

      /* v8 ignore next -- @preserve */
      if (!tableExists) {
        propertyKey =
          propertyKey.length > 0
            ? segment.tableKey + '/' + propertyKey
            : segment.tableKey;
        result = result.upper();
        result.propertyKey = propertyKey;
      }
    }
    return result;
  }

  // ...........................................................................
  /**
   * Isolates a property from all components in an Rljson
   * @param rljson - The Rljson to isolate the property from
   * @param propertyKey - The property key to isolate
   * @returns A new Rljson with only the isolated property
   */
  isolatePropertyFromComponents(rljson: Rljson, propertyKey: string): Rljson {
    const result: Rljson = {};
    for (const tableKey of Object.keys(rljson)) {
      const table = rljson[tableKey];
      const newData = table._data.map((row: any) => {
        if (row.hasOwnProperty(propertyKey)) {
          return {
            [propertyKey]: row[propertyKey],
            _hash: row._hash,
          };
        } else {
          return row;
        }
      });
      result[tableKey] = {
        _type: table._type,
        _data: newData,
      };
    }
    return result;
  }

  // ...........................................................................
  /**
   * Clone the Db instance with a new Io instance
   * @param io - The new Io instance
   * @returns A new Db instance with the same cache as the current instance
   */
  clone(io: Io): Db {
    const newDb = new Db(io);
    newDb.setCache(new Map(this._cache));
    return newDb;
  }

  // ...........................................................................
  /**
   * Get the current cache of the Db instance
   */
  get cache() {
    return this._cache;
  }

  // ...........................................................................
  /**
   * Set the cache of the Db instance
   * @param cache - The new cache to set
   */
  setCache(cache: Map<string, Container>) {
    this._cache = cache;
  }
}
