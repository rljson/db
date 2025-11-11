// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Io } from '@rljson/io';
import { Json, merge } from '@rljson/json';
import {
  CakesTable, ColumnCfg, ComponentsTable, Insert, InsertHistoryRow, InsertHistoryTimeId, isTimeId,
  Layer, LayersTable, Ref, Rljson, Route, RouteSegment, SliceId, SliceIds, validateInsert
} from '@rljson/rljson';

import { traverse } from 'object-traversal';

import {
  Controller, ControllerRefs, ControllerRunFn, createController
} from './controller/controller.ts';
import { Core } from './core.ts';
import { Join, JoinColumn, JoinRows } from './join/join.ts';
import { ColumnInfo, ColumnSelection } from './join/selection/column-selection.ts';
import { Notify } from './notify.ts';


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

  private _cache: Map<string, Rljson> = new Map();

  // ...........................................................................
  /**
   * Get data from a route with optional filtering
   * @param route - The route to get data from
   * @param where - Optional filter to apply to the data
   * @returns An array of Rljson objects matching the route and filter
   * @throws {Error} If the route is not valid or if any controller cannot be created
   */
  async get(route: Route, where: string | Json): Promise<Rljson> {
    // Validate Route
    if (!route.isValid) throw new Error(`Route ${route.flat} is not valid.`);

    //Isolate Property Key
    const isolatedRoute = await this.isolatePropertyKeyFromRoute(route);

    const cacheHash = `${isolatedRoute.flat}|${JSON.stringify(where)}`;
    const isCached = this._cache.has(cacheHash);
    if (isCached) {
      return this._cache.get(cacheHash)!;
    } else {
      // Get Controllers
      const controllers = await this._indexedControllers(isolatedRoute);

      // Fetch Data
      const data = await this._get(isolatedRoute, where, controllers);

      // Cache Data
      this._cache.set(cacheHash, data);
      return data;
    }
  }

  async _get(
    route: Route,
    where: string | Json,
    controllers: Record<string, Controller<any, any>>,
    segmentLevel?: number,
  ): Promise<Rljson> {
    //Start at top segment
    if (segmentLevel === undefined) segmentLevel = 0;

    const segment = route.segments[segmentLevel];
    const segmentIsDeepest = segmentLevel === route.segments.length - 1;
    const segmentController = controllers[segment.tableKey];
    const segmentRef = await this._getReferenceOfRouteSegment(segment);

    const hashRequested = typeof where === 'string';

    //Build where for segment
    //If string given -> build _hash object
    let segmentWhere = hashRequested ? ({ _hash: where } as Json) : where;

    //If ref through route given, add to where object
    segmentWhere = segmentRef
      ? ({ ...segmentWhere, ...{ _hash: segmentRef } } as Json)
      : segmentWhere;

    //If encapsulated, drill into object
    segmentWhere =
      segment.tableKey in segmentWhere
        ? (segmentWhere[segment.tableKey] as Json)
        : segmentWhere;

    const childSegmentLevel = segmentLevel + 1;
    const childSegment = route.segments[childSegmentLevel];

    const segmentWhereWithoutChildWhere = { ...segmentWhere };
    //Delete child segment key from where
    if (!segmentIsDeepest && childSegment.tableKey in segmentWhere)
      delete segmentWhereWithoutChildWhere[childSegment.tableKey];

    let parent = await segmentController.get(segmentWhereWithoutChildWhere, {});

    //Isolate Property if deepest segment has property key
    if (segmentIsDeepest && route.hasPropertyKey) {
      parent = this.isolatePropertyFromComponents(parent, route.propertyKey!);
    }

    if (!segmentIsDeepest) {
      const children: Rljson[] = [];
      const filteredParentRows: Map<string, Json> = new Map();

      if (hashRequested) {
        //Get all children of hash
        const childrenOfHash = await this._get(
          route,
          {},
          controllers,
          childSegmentLevel,
        );
        children.push(childrenOfHash);
      } else {
        //Get children with given where
        const childWhere =
          (segmentWhere[childSegment.tableKey] as Json | string) ?? {};

        const childrenByWhere = await this._get(
          route,
          childWhere,
          controllers,
          childSegmentLevel,
        );

        //Get this child's data from all children
        const segmentChildrenByWhere = childrenByWhere[childSegment.tableKey];

        //These are the children from deeper levels that we do not need
        // to filter against, but we have to keep them
        const otherChildrenByWhere = { ...childrenByWhere };
        delete otherChildrenByWhere[childSegment.tableKey];
        children.push(otherChildrenByWhere);

        //Get child refs from parent
        const childRefs = await segmentController.getChildRefs(
          segmentWhereWithoutChildWhere,
          {},
        );

        for (const { tableKey, columnKey, ref } of childRefs) {
          if (tableKey !== childSegment.tableKey) continue;

          const childRefIsInWhere = segmentChildrenByWhere._data.find(
            (c) => ref === c._hash,
          );

          if (!childRefIsInWhere) continue;

          const child = {
            [tableKey]: { _data: [childRefIsInWhere] },
          } as Rljson;

          children.push(child);

          // const childrenOfReferencedChild = await this._get(
          //   route,
          //   ref,
          //   controllers,
          //   childSegmentLevel,
          // );
          // delete childrenOfReferencedChild[tableKey];

          // if (
          //   !otherChildrenByWhere ||
          //   (Object.keys(otherChildrenByWhere).length == 0 &&
          //     Object.keys(childrenOfReferencedChild).length > 0)
          // ) {
          //   children.push(childrenOfReferencedChild);
          // }

          //Filter parent to only include rows that have the child ref

          for (const childObjs of child[tableKey]._data) {
            const childRef = (childObjs as Json)['_hash'] as string;
            for (const row of parent[segment.tableKey]._data) {
              if (filteredParentRows.has((row as Json)['_hash'] as string))
                continue;

              const includesChild = segmentController.filterRow(
                row,
                columnKey ?? tableKey,
                childRef,
              );
              if (includesChild) {
                filteredParentRows.set(
                  (row as Json)['_hash'] as string,
                  row as Json,
                );
              }
            }
          }
        }
      }

      //Build final parent with filtered rows
      const parentWithFilteredRows = {
        [segment.tableKey]: {
          ...parent[segment.tableKey],
          ...{
            _data: Array.from(filteredParentRows.values()),
          },
        },
      };
      return merge(parentWithFilteredRows, ...children) as Rljson;
    }

    return parent;
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
    //Fetch Data for ColumnSelection
    const data = await this._getBaseDataForColumnSelection(columnSelection);

    //Get Cake
    const cakesTable = data[cakeKey] as CakesTable;
    const cake = cakesTable._data.find((c) => c._hash === cakeRef);
    if (!cake) {
      throw new Error(
        `Db.join: Cake with ref "${cakeRef}" not found in cake table "${cakeKey}".`,
      );
    }

    //Get Layers
    const layers: Map<string, Layer> = new Map();
    for (const layerKey of Object.keys(cake.layers)) {
      if (!data[layerKey]) continue;

      const layersTable = data[layerKey] as LayersTable;
      const layer = layersTable._data.find(
        (l) => l._hash === cake.layers[layerKey],
      );

      // We can expect that the layer exists because it
      // passed get validation from _getDataForColumnSelection
      layers.set(layerKey, layer!);
    }

    //Get Components
    const components: Map<string, ComponentsTable<Json>> = new Map();
    for (const [tableKey, table] of Object.entries(data)) {
      if (table._type !== 'components') continue;
      components.set(tableKey, table as ComponentsTable<Json>);
    }

    //Merge Layers Slice Ids,
    const mergedSliceIds: Set<SliceId> = new Set();
    for (const layer of layers.values()) {
      const sliceIdsTable = layer.sliceIdsTable;
      const sliceIdsTableRow = layer.sliceIdsTableRow;
      const {
        [sliceIdsTable]: { _data: sliceIds },
      } = await this.core.readRows(sliceIdsTable, { _hash: sliceIdsTableRow });

      //Merge Slice Ids
      for (const sid of sliceIds as SliceIds[]) {
        for (const s of sid.add) {
          mergedSliceIds.add(s as SliceId);
        }
      }
    }

    // Build ColumnCfgs and Infos for Layer referenced Components
    const columnCfgs: Map<string, ColumnCfg[]> = new Map();
    const columnInfos: Map<string, ColumnInfo[]> = new Map();
    let objectMap: Json = {};
    for (const [layerKey, layer] of layers.entries()) {
      const componentKey = layer.componentsTable;
      const componentResolved = await this._resolveComponent(
        `${cakeKey}/${layerKey}/`,
        componentKey,
      );

      objectMap = { ...objectMap, ...componentResolved.objectMap };

      columnInfos.set(componentKey, componentResolved.columnInfos);

      columnCfgs.set(componentKey, componentResolved.columnCfgs);
    }

    //Join Rows to SliceIds
    const rowMap: Map<SliceId, JoinColumn<any>[]> = new Map();
    const joinColumnInfos: Map<string, ColumnInfo> = new Map();

    for (const sliceId of mergedSliceIds) {
      const sliceIdRow: JoinColumn<any>[] = [];

      for (const [layerKey, layer] of layers.entries()) {
        const layerRef = layer._hash;
        const componentKey = layer.componentsTable;
        const componentRef = layer.add[sliceId];
        const componentsTable = data[componentKey] as ComponentsTable<Json>;
        const rowComponentProperties = componentsTable._data.find(
          (r) => r._hash === componentRef,
        )!;

        const resolvedProperties = this._resolveComponentProperties(
          rowComponentProperties,
          objectMap,
          data,
        );

        const joinColumns: JoinColumn<any>[] = [];
        for (const [
          resolvedPropertyKey,
          resolvedPropertyValue,
        ] of Object.entries(resolvedProperties)) {
          const propertyRoute =
            cakeKey +
            '/' +
            layerKey +
            '/' +
            componentKey +
            '/' +
            resolvedPropertyKey;

          const propertyColumnInfo = columnInfos
            .get(componentKey)!
            .find((cI) => cI.route === propertyRoute);

          if (!!propertyColumnInfo) {
            const joinColumnRoute = Route.fromFlat(
              `${cakeKey}@${cakeRef}/${layerKey}@${layerRef}/${componentKey}@${componentRef}`,
            );
            joinColumnRoute.propertyKey = resolvedPropertyKey;

            joinColumnInfos.set(resolvedPropertyKey, {
              ...propertyColumnInfo,
              key: resolvedPropertyKey,
              route: joinColumnRoute.flatWithoutRefs.slice(1),
            });

            joinColumns.push({
              route: joinColumnRoute,
              value: resolvedPropertyValue ?? null,
              insert: null,
            } as JoinColumn<any>);
          }
        }

        sliceIdRow.push(...joinColumns);
      }

      rowMap.set(sliceId, sliceIdRow);
    }

    //Build Result
    const joinRows: JoinRows = {};
    for (const [sliceId, joinColumns] of rowMap.entries()) {
      Object.assign(joinRows, {
        [sliceId]: joinColumns as JoinColumn<any>[],
      });
    }

    // Return Join
    return new Join(
      joinRows,
      new ColumnSelection(Array.from(joinColumnInfos.values())),
      objectMap,
      Route.fromFlat(`${cakeKey}@${cakeRef}`),
    ).select(columnSelection);
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
      const componentData = await this.get(uniqueComponentRoute, {});

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
    runFns: Record<string, ControllerRunFn<any>>,
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
   * @param Insert - The Insert to resolve
   * @returns A record of controller run functions, keyed by table name
   * @throws {Error} If the route is not valid or if any controller cannot be created
   */
  private async _resolveInsert(
    Insert: Insert<any>,
  ): Promise<Record<string, ControllerRunFn<any>>> {
    // Get Controllers and their Run Functions
    const controllers = await this._indexedControllers(
      Route.fromFlat(Insert.route),
    );

    // Add Controllers for unrelated component references
    const referencedComponentTableKeys: Set<string> = new Set();
    traverse(Insert.value, ({ key, parent }) => {
      if (key == '_tableKey')
        referencedComponentTableKeys.add(parent![key] as string);
    });
    for (const tableKey of referencedComponentTableKeys) {
      controllers[tableKey] ??= await this.getController(tableKey);
    }

    const runFns: Record<string, ControllerRunFn<any>> = {};
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
  ): Promise<Record<string, Controller<any, any>>> {
    // Create Controllers
    const controllers: Record<string, Controller<any, any>> = {};
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
