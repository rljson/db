// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Io } from '@rljson/io';
import { Json, JsonValue, merge } from '@rljson/json';
import {
  Cake,
  CakesTable,
  ColumnCfg,
  ComponentsTable,
  Insert,
  InsertHistoryRow,
  InsertHistoryTimeId,
  isTimeId,
  Layer,
  Ref,
  Rljson,
  Route,
  RouteSegment,
  SliceId,
  SliceIds,
  TableType,
  validateInsert,
} from '@rljson/rljson';

import { traverse } from 'object-traversal';

import {
  Controller,
  ControllerChildProperty,
  ControllerRefs,
  ControllerRunFn,
  createController,
} from './controller/controller.ts';
import { SliceIdController } from './controller/slice-id-controller.ts';
import { Core } from './core.ts';
import { Join, JoinColumn, JoinRow, JoinRows } from './join/join.ts';
import {
  ColumnInfo,
  ColumnSelection,
} from './join/selection/column-selection.ts';
import { Notify } from './notify.ts';
import { makeUnique } from './tools/make-unique.ts';

export type DbRouteValueAndShadow = {
  route: Route;
  value: JsonValue | JsonValue[] | null;
  shadow: JsonValue | JsonValue[] | null;
};

export type DbGetResult = {
  rljson: Rljson;
  obj: Json;
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

  private _cache: Map<string, DbGetResult> = new Map();

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
  ): Promise<DbGetResult> {
    // Validate Route
    if (!route.isValid) throw new Error(`Route ${route.flat} is not valid.`);

    //Isolate Property Key
    const isolatedRoute = await this.isolatePropertyKeyFromRoute(route);

    const cacheHash = `${isolatedRoute.flat}|${JSON.stringify(
      where,
    )}|${JSON.stringify(filter)}|${JSON.stringify(sliceIds)}`;
    const isCached = this._cache.has(cacheHash);
    if (isCached) {
      return this._cache.get(cacheHash)!;
    } else {
      // Get Controllers
      const controllers = await this._indexedControllers(isolatedRoute);

      // Fetch Data
      const data = await this._get(
        isolatedRoute,
        where,
        controllers,
        filter,
        sliceIds,
      );

      // Cache Data
      this._cache.set(cacheHash, data);
      return data;
    }
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
  ): Promise<DbGetResult> {
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
      nodeWhere = { ...nodeWhere, ...{ _hash: nodeRouteRef } };

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
      let filterProperty: ControllerChildProperty | undefined;
      if (filterActive) {
        for (const f of filter) {
          if (f.tableKey !== nodeTableKey) continue;
          if (nodeRow._hash === f.ref) {
            filterProperty = f;
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
            if (filterProperty && filterProperty.sliceIds) {
              const componentSliceIds = filterProperty.sliceIds;
              const componentMatchesSliceIds = nodeSliceIds.filter((sId) =>
                componentSliceIds.includes(sId),
              );
              if (componentMatchesSliceIds.length > 0) sliceIdResult = true;
            }
            break;
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
    if (route.isRoot) {
      if (route.hasPropertyKey) {
        const isolatedNode = this.isolatePropertyFromComponents(
          node,
          route.propertyKey!,
        );
        return {
          rljson: isolatedNode,
          obj: { [nodeTableKey]: isolatedNode[nodeTableKey] },
        };
      }
      return {
        rljson: node,
        obj: { [nodeTableKey]: node[nodeTableKey] },
      };
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
      { row: Json; obj: Json }
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
        if (cr.columnKey && cr.sliceIds && cr.sliceIds.length > 0) {
          for (const sId of cr.sliceIds) {
            childrenRefSliceIds.add(sId);
          }
          const childrenRefColumnCfg = nodeColumnCfgs.find(
            (c) => c.key === cr.columnKey,
          );

          if (childrenRefColumnCfg) {
            childrenRefTypes.set(
              childrenRefColumnCfg.key,
              childrenRefColumnCfg.ref?.type ?? '',
            );
          }
        }
      }

      if (childrenRefTypes.size > 1) {
        throw new Error(
          `Db._get: Multiple reference types found for children of table ${nodeTableKey}.`,
        );
      }

      const cakeIsReferenced =
        childrenRefTypes.size === 1 &&
        [...childrenRefTypes.values()][0] === 'cakes';

      const { rljson: rowChildrenRljson, obj: rowChildrenObj } =
        await this._get(
          childrenRoute,
          childrenWhere,
          controllers,
          childrenRefs,
          cakeIsReferenced ? [...childrenRefSliceIds] : nodeSliceIds,
        );

      if (cakeIsReferenced) {
        const refKey = [...childrenRefTypes.keys()][0] as string;
        nodeRowObj[refKey] = rowChildrenObj;
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
                  ...(rowChildrenObj as any)[childrenTableKey],
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
      if (nodeType === 'layers') {
        const components = (
          (rowChildrenObj as any)[childrenTableKey]! as ComponentsTable<Json>
        )._data;

        const layer = components
          .map((comp) => {
            const sliceIds = matchingChildrenRefs.find(
              (cr) => cr.ref === comp._hash,
            )?.sliceIds;

            if (!sliceIds || sliceIds.length === 0) {
              throw new Error(
                `Db._get: No sliceIds found for component ${
                  comp._hash
                } of layer ${(nodeRow as any)._hash}.`,
              );
            }
            if (sliceIds.length > 1) {
              throw new Error(
                `Db._get: Multiple sliceIds found for component ${
                  comp._hash
                } of layer ${(nodeRow as any)._hash}.`,
              );
            }

            const sliceId = sliceIds[0];
            return {
              [sliceId]: {
                [childrenTableKey]: { ...comp, ...{ _type: 'components' } },
              },
            };
          })
          .reduce((acc, curr) => ({ ...acc, ...curr }), {});

        nodeRowsMatchingChildrenRefs.set((nodeRow as any)._hash, {
          row: nodeRow,
          obj: { ...nodeRowObj, add: layer },
        });
      } else if (nodeType === 'cakes') {
        nodeRowsMatchingChildrenRefs.set((nodeRow as any)._hash, {
          row: nodeRow,
          obj: { ...nodeRowObj, layers: rowChildrenObj },
        });
      } else if (nodeType === 'components') {
        nodeRowsMatchingChildrenRefs.set((nodeRow as any)._hash, {
          row: nodeRow,
          obj: { ...nodeRowObj },
        });
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

    return {
      rljson: {
        ...node,
        ...{
          [nodeTableKey]: {
            ...{
              _data: matchedNodeRows.map((mr) => mr.row),
              _type: nodeType,
            },
            ...{
              ...(nodeHash ? { _hash: nodeHash } : {}),
            },
          },
        },
        ...nodeChildren,
      } as Rljson,
      obj: {
        [nodeTableKey]: {
          ...{
            _data: matchedNodeRows.map((mr) => mr.obj),
            _type: nodeType,
          },
          ...{
            ...(nodeHash ? { _hash: nodeHash } : {}),
          },
        },
      },
    };
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
      obj: { [cakeKey]: cakesTable },
    } = await this.get(Route.fromFlat(`${cakeKey}@${cakeRef}`), {});

    const cakes = (cakesTable as CakesTable)._data;

    if (cakes.length === 0) {
      throw new Error(
        `Db.join: Cake with ref "${cakeRef}" not found in cake table "${cakeKey}".`,
      );
    }
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

        const { obj: columnRawData } = await this.get(
          columnRoute.toRouteWithoutProperty(),
          cakeRef,
          undefined,
          [sliceId],
        );

        const columnRawDataParsed = await this._objectParser(
          columnRawData,
          columnRoute,
        );

        if (
          !columnRawDataParsed ||
          !Array.isArray(columnRawDataParsed) ||
          columnRawDataParsed.length === 0
        ) {
          throw new Error(
            `Db.join: Column data not found for column route "${columnRoute.flat}" in cake "${cakeKey}@${cakeRef}" for sliceId "${sliceId}".`,
          );
        }

        if (columnRawDataParsed.length > 1) {
          throw new Error(
            `Db.join: Multiple column data entries found for column route "${columnRoute.flat}" in cake "${cakeKey}@${cakeRef}" for sliceId "${sliceId}".`,
          );
        }

        const column: JoinColumn<any> = {
          ...columnRawDataParsed[0],
          ...{ insert: null },
        };
        row.push(column);
      }

      rows[sliceId] = row;
    }

    debugger;

    // Return Join
    return new Join(
      joinRows,
      new ColumnSelection(Array.from(joinColumnInfos.values())),
      objectMap,
    ).select(columnSelection);
  }

  private async _objectParser(
    obj: Json,
    route: Route,
    objRoute?: Route,
  ): Promise<DbRouteValueAndShadow | DbRouteValueAndShadow[] | null> {
    const segmentObj = obj[route.top.tableKey] as TableType;
    const segmentRef = Route.segmentHasRef(route.top)
      ? await this._getReferenceOfRouteSegment(route.top)
      : null;

    if (!segmentObj) return null;

    if (segmentObj._type === 'cakes') {
      const segmentData = segmentObj._data.filter((d) =>
        segmentRef ? d._hash === segmentRef : true,
      );

      if (route.isRoot)
        return {
          value: {
            ...segmentObj,
            ...{ _data: segmentData },
          },
          shadow: {
            ...segmentObj,
            ...{ _data: segmentData },
          },
          route: Route.fromFlat(
            (objRoute ? objRoute.flat : '') + '/' + route.top.tableKey,
          ),
        };

      const result = [];
      for (const cake of segmentData) {
        const resolvedRoute = Route.fromFlat(
          (objRoute ? objRoute.flat : '') +
            '/' +
            (route.top.tableKey + `@${cake._hash}`),
        );

        const resolved = await this._objectParser(
          cake.layers,
          route.deeper(),
          resolvedRoute,
        );

        result.push(...(Array.isArray(resolved) ? resolved! : [resolved!]));
      }
      return result;
    }
    if (segmentObj._type === 'layers') {
      const segmentData = segmentObj._data.filter((d) =>
        segmentRef ? d._hash === segmentRef : true,
      );

      if (route.isRoot)
        return {
          value: {
            ...segmentObj,
            ...{ _data: segmentData },
          },
          shadow: {
            ...segmentObj,
            ...{ _data: segmentData },
          },
          route: Route.fromFlat(
            (objRoute ? objRoute.flat : '') + '/' + route.top.tableKey,
          ),
        };

      const result = [];
      for (const layer of segmentData) {
        for (const comp of Object.values(layer.add)) {
          const resolvedRoute = Route.fromFlat(
            (objRoute ? objRoute.flat : '') +
              '/' +
              (route.top.tableKey + `@${layer._hash}`),
          );

          const resolved = await this._objectParser(
            comp as any,
            route.deeper(),
            resolvedRoute,
          );

          result.push(...(Array.isArray(resolved) ? resolved! : [resolved!]));
        }
      }
      return result;
    }
    if (segmentObj._type === 'components') {
      if (segmentRef && segmentObj._hash !== segmentRef) return null;

      if (route.isRoot) {
        const compRoute = Route.fromFlat(
          (objRoute ? objRoute.flat : '') + '/' + route.top.tableKey,
        );
        if (route.hasPropertyKey) {
          const propertyRoute = new Route(compRoute.segments);
          propertyRoute.propertyKey = route.propertyKey;

          return {
            value: (segmentObj as Json)[route.propertyKey!]!,
            shadow: (segmentObj as Json)!,
            route: propertyRoute,
          };
        }
        return {
          value: segmentObj as Json,
          shadow: (segmentObj as Json)!,
          route: compRoute,
        };
      }
    }

    return null;
  }

  private _resolveComponentProperties(
    componentData: Json,
    objectMapOrRoute: Json | string,
    baseData: Rljson,
  ): Json {
    let result = {};

    for (const [propertyKey, propertyObjectMap] of Object.entries(
      objectMapOrRoute,
    )) {
      if (propertyKey === '_tableKey') continue;
      if (typeof propertyObjectMap === 'object') {
        const refs = Array.isArray(componentData[propertyKey])
          ? (componentData[propertyKey] as Ref[])
          : [componentData[propertyKey] as Ref];

        const refTableKey = (propertyObjectMap as Json)['_tableKey'] as string;
        const refCompTable = baseData[refTableKey] as ComponentsTable<Json>;

        if (!refCompTable) continue;

        const prefixedResolvedRefCompData: Record<string, Json[]> = {};
        for (const ref of refs) {
          const refCompData = refCompTable._data.find((r) => r._hash === ref);

          if (refCompData) {
            const resolvedRefCompData = this._resolveComponentProperties(
              refCompData,
              propertyObjectMap as Json,
              baseData,
            );

            for (const [refPropKey, value] of Object.entries(
              resolvedRefCompData,
            )) {
              if (
                !prefixedResolvedRefCompData[`${refTableKey}/${refPropKey}`]
              ) {
                prefixedResolvedRefCompData[`${refTableKey}/${refPropKey}`] =
                  [];
              }

              prefixedResolvedRefCompData[`${refTableKey}/${refPropKey}`].push({
                _ref: ref,
                _value: value,
              });
            }
          }
          result = {
            ...result,
            ...prefixedResolvedRefCompData,
          };
        }
      }
      result = {
        ...result,
        ...{ [propertyKey]: componentData[propertyKey] },
      };
    }
    return result;
  }

  // ...........................................................................
  /**
   * Resolve a component's columns, including referenced components
   *
   * @param baseRoute - The base route for the component
   * @param componentKey - The component's table key
   * @returns - The resolved column configurations, column infos, and object map
   */
  private async _resolveComponent(
    baseRoute: string,
    componentKey: string,
  ): Promise<{
    columnCfgs: ColumnCfg[];
    columnInfos: ColumnInfo[];
    objectMap: Json;
  }> {
    const { columns: colCfgs } = await this.core.tableCfg(componentKey);

    const objectMap: Json = {};
    const columnCfgs: ColumnCfg[] = [];
    const columnInfos: ColumnInfo[] = [];

    for (let i = 0; i < colCfgs.length; i++) {
      if (colCfgs[i].key === '_hash') continue;

      const colCfg = colCfgs[i];

      if (colCfg.ref) {
        const columnCfgsAndInfosForRef = await this._resolveComponent(
          baseRoute + `/${componentKey}`,
          colCfg.ref.tableKey,
        );

        objectMap[colCfg.key] = {
          _tableKey: colCfg.ref.tableKey,
          ...columnCfgsAndInfosForRef.objectMap,
        };

        const columnCfgsForRef = columnCfgsAndInfosForRef.columnCfgs.map(
          (cc) => ({
            ...cc,
            key: colCfg.ref!.tableKey + '/' + cc.key,
          }),
        );
        const columnInfosForRef = columnCfgsAndInfosForRef.columnInfos.map(
          (cc) => ({
            ...cc,
            key: colCfg.ref!.tableKey + '/' + cc.key,
          }),
        );

        columnCfgs.push(...columnCfgsForRef);
        columnInfos.push(...columnInfosForRef);
      }

      /* v8 ignore next -- @preserve */
      const columnRoute = Route.fromFlat(
        baseRoute.length > 0
          ? `${baseRoute}/${componentKey}/${colCfg.key}`
          : `/${componentKey}/${colCfg.key}`,
      ).flat.slice(1);

      if (!objectMap[colCfg.key]) objectMap[colCfg.key] = columnRoute;

      columnCfgs.push(colCfg);
      columnInfos.push({
        ...colCfg,
        alias: `${colCfg.key}`,
        route: columnRoute,
        titleShort: colCfg.key,
        titleLong: colCfg.key,
      });
    }

    return { columnCfgs, columnInfos, objectMap };
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
   * Fetches data for the given ColumnSelection
   * @param columnSelection - The ColumnSelection to fetch data for
   */
  private async _getBaseDataForColumnSelection(
    columnSelection: ColumnSelection,
  ) {
    //Make Component Routes unique
    const uniqueComponentRoutes: Set<string> = new Set();
    for (const colInfo of columnSelection.columns) {
      const componentRoute = Route.fromFlat(colInfo.route);
      const isolatedComponentRoute = await this.isolatePropertyKeyFromRoute(
        componentRoute,
      );
      uniqueComponentRoutes.add(
        isolatedComponentRoute.toRouteWithoutProperty().flat,
      );
    }

    // Fetch Data from all controller routes
    const data: Rljson = {};
    for (const compRouteFlat of uniqueComponentRoutes) {
      const uniqueComponentRoute = Route.fromFlat(compRouteFlat);
      const { rljson: componentData } = await this.get(
        uniqueComponentRoute,
        {},
      );

      Object.assign(data, componentData);
    }
    return data;
  }

  // ...........................................................................
  /**
   * Runs an Insert by executing the appropriate controller(s) based on the Insert's route
   * @param Insert - The Insert to run
   * @returns The result of the Insert as an InsertHistoryRow
   * @throws {Error} If the Insert is not valid or if any controller cannot be created
   */
  async insert(
    insert: Insert<any>,
    options?: { skipNotification?: boolean; skipHistory?: boolean },
  ): Promise<InsertHistoryRow<any>[]> {
    const initialRoute = Route.fromFlat(insert.route);
    const runs = await this._resolveInsert(insert);
    const errors = validateInsert(insert);
    if (!!errors.hasErrors) {
      throw new Error(
        `Db.insert: Insert is not valid:\n${JSON.stringify(errors, null, 2)}`,
      );
    }

    return this._insert(insert, initialRoute, runs, options);
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
    insert: Insert<any>,
    route: Route,
    runFns: Record<string, ControllerRunFn<any, any>>,
    options?: { skipNotification?: boolean; skipHistory?: boolean },
  ): Promise<InsertHistoryRow<any>[]> {
    let results: InsertHistoryRow<any>[];
    let tableKey: string;

    //Run parent controller with child refs as value
    const segment = route.segment(0);
    tableKey = segment.tableKey;

    let previous: InsertHistoryTimeId[] = [];
    if (Route.segmentHasRef(segment)) {
      const routeRef: InsertHistoryTimeId = Route.segmentRef(segment)!;
      if (Route.segmentHasInsertHistoryRef(segment)) {
        //Collect previous refs from child results
        previous = [...previous, routeRef];
      }
      if (Route.segmentHasDefaultRef(segment)) {
        const timeIds = await this.getTimeIdsForRef(
          tableKey,
          Route.segmentRef(segment)!,
        );
        previous = [...previous, ...timeIds];
      }
    }

    //If not root, run nested controllers first
    if (!route.isRoot) {
      //Run nested controller first
      const childRoute = route.deeper(1);

      //Iterate over child values and create Inserts for each
      const childKeys = this._childKeys(insert.value);
      const childRefs: Record<string, string[]> = {};

      for (const k of childKeys) {
        const childValue = (insert.value as any)[k];
        const childInsert: Insert<any> = { ...insert, value: childValue };
        const childResults = await this._insert(
          childInsert,
          childRoute,
          runFns,
        );
        const childRefKey = childRoute.top.tableKey + 'Ref';
        const childRefArray = childResults.map(
          (childResult) => (childResult as any)[childRefKey] as string,
        );

        childRefs[k] = childRefArray;
      }

      //Run parent controller with child refs as value
      const runFn = runFns[tableKey];

      results = [
        ...(
          await runFn(
            insert.command,
            {
              ...insert.value,
              ...childRefs,
            },
            insert.origin,
          )
        ).map((r) => ({ ...r, ...{ previous } })),
      ];
    } else {
      //Run root controller
      tableKey = route.root.tableKey;
      const runFn = runFns[tableKey];

      const insertValue = insert.value;
      for (const [propertyKey, propertyValue] of Object.entries(insert.value)) {
        if (
          propertyValue &&
          typeof propertyValue === 'object' &&
          !!(propertyValue as any)._tableKey
        ) {
          const referenceRoute = (propertyValue as any)._tableKey;

          // Remove _tableKey from propertyValue to avoid issues during insert
          delete (propertyValue as any)._tableKey;

          const referenceInsert: Insert<any> = {
            command: insert.command,
            route: referenceRoute,
            value: propertyValue,
          };
          const referencesWritten = (
            await this._insert(
              referenceInsert,
              Route.fromFlat(referenceRoute),
              runFns,
            )
          ).map((h) => (h as any)[referenceRoute + 'Ref']);

          insertValue[propertyKey] =
            referencesWritten.length === 1
              ? referencesWritten[0]
              : referencesWritten;
        }
      }

      //Run on controller, get InsertHistoryRow from return, pass previous revisions
      results = [
        ...(await runFn(insert.command, insertValue, insert.origin)).map(
          (r) => ({ ...r, previous }),
        ),
      ];
    }

    for (const result of results) {
      //Write route to result
      result.route = insert.route;

      //Write insertHistory
      if (!options?.skipHistory)
        await this._writeInsertHistory(tableKey, result);

      //Notify listeners
      if (!options?.skipNotification)
        this.notify.notify(Route.fromFlat(insert.route), result);
    }

    return results;
  }

  // ...........................................................................
  /**
   * Registers a callback to be called when an Insert is made on the given route
   * @param route - The route to register the callback on
   * @param callback - The callback to be called when an Insert is made
   */
  registerObserver(
    route: Route,
    callback: (InsertHistoryRow: InsertHistoryRow<any>) => void,
  ) {
    this.notify.register(route, callback);
  }

  // ...........................................................................
  /**
   * Unregisters a callback from the given route
   * @param route - The route to unregister the callback from
   * @param callback - The callback to be unregistered
   */
  unregisterObserver(
    route: Route,
    callback: (InsertHistoryRow: InsertHistoryRow<any>) => void,
  ) {
    this.notify.unregister(route, callback);
  }

  // ...........................................................................
  /**
   * Resolves an Insert by returning the run functions of all controllers involved in the Insert's route
   * @param insert - The Insert to resolve
   * @returns A record of controller run functions, keyed by table name
   * @throws {Error} If the route is not valid or if any controller cannot be created
   */
  private async _resolveInsert(
    insert: Insert<any>,
  ): Promise<Record<string, ControllerRunFn<any, any>>> {
    // Get Controllers and their Run Functions
    const controllers = await this._indexedControllers(
      Route.fromFlat(insert.route),
    );

    // Add Controllers for unrelated component references
    const referencedComponentTableKeys: Set<string> = new Set();

    traverse({ ...insert.value }, ({ key, parent }) => {
      if (key == '_tableKey')
        referencedComponentTableKeys.add(parent![key] as string);
    });
    for (const tableKey of referencedComponentTableKeys) {
      controllers[tableKey] ??= await this.getController(tableKey);
    }

    const runFns: Record<string, ControllerRunFn<any, any>> = {};
    for (const tableKey of Object.keys(controllers)) {
      runFns[tableKey] = controllers[tableKey].insert.bind(
        controllers[tableKey],
      );
    }

    return runFns;
  }

  // ...........................................................................
  /**
   * Returns the keys of child refs in a value based on a route
   * @param value - The value to check
   * @returns An array of keys of child refs in the value
   */
  private _childKeys(value: Json): string[] {
    const keys = Object.keys(value);
    const childKeys: string[] = [];
    for (const k of keys) {
      if (typeof (value as any)[k] !== 'object') continue;

      childKeys.push(k);
    }
    return childKeys;
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
  private async _indexedControllers(
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
   * Get the current cache of the Db instance
   */
  get cache() {
    return this._cache;
  }
}
