// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Io } from '@rljson/io';
import { Json, merge } from '@rljson/json';
import {
  CakesTable,
  ColumnCfg,
  ComponentsTable,
  Insert,
  InsertHistoryRow,
  InsertHistoryTimeId,
  isTimeId,
  Layer,
  LayersTable,
  Ref,
  Rljson,
  Route,
  RouteSegment,
  SliceId,
  SliceIds,
  validateInsert,
} from '@rljson/rljson';

import {
  Controller,
  ControllerRefs,
  ControllerRunFn,
  createController,
} from './controller/controller.ts';
import { Core } from './core.ts';
import { Join, JoinColumn, JoinRows } from './join/join.ts';
import {
  ColumnInfo,
  ColumnSelection,
} from './join/selection/column-selection.ts';
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

    //Build where for segment
    //If string given -> build _hash object
    let segmentWhere =
      typeof where === 'object' ? where : ({ _hash: where } as Json);

    //If encapsulated, drill into object
    segmentWhere =
      segment.tableKey in segmentWhere
        ? (segmentWhere[segment.tableKey] as Json)
        : segmentWhere;

    //If ref through route given, add to where object
    segmentWhere = segmentRef
      ? ({ ...segmentWhere, ...{ _hash: segmentRef } } as Json)
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

    const children: Rljson[] = [];
    const filteredParentRows: Map<string, Json> = new Map();

    if (!segmentIsDeepest) {
      const childRefs = await segmentController.getChildRefs(
        segmentWhereWithoutChildWhere,
        {},
      );
      for (const { tableKey, columnKey, ref } of childRefs) {
        if (tableKey !== childSegment.tableKey) continue;

        const child = await this._get(
          route,
          { ...segmentWhere, ...{ _hash: ref } },
          controllers,
          childSegmentLevel,
        );
        children.push(child);

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

    // Build ColumnCfgs
    const columnCfgs: Map<string, ColumnCfg[]> = new Map();
    const columnInfos: Map<string, ColumnInfo[]> = new Map();
    for (const [layerKey, layer] of layers.entries()) {
      const componentKey = layer.componentsTable;
      const { columns: colCfgs } = await this.core.tableCfg(componentKey);

      const columnCfg: ColumnCfg[] = [];
      const columnInfo: ColumnInfo[] = [];
      for (let i = 0; i < colCfgs.length; i++) {
        if (colCfgs[i].key === '_hash') continue;

        const colCfg = colCfgs[i];
        columnCfg.push(colCfg);
        columnInfo.push({
          ...colCfg,
          alias: `${colCfg.key}`,
          route: Route.fromFlat(
            `/${cakeKey}/${layerKey}/${componentKey}/${colCfg.key}`,
          ).flat.slice(1),
          titleShort: colCfg.key,
          titleLong: colCfg.key,
        });
      }

      columnInfos.set(componentKey, columnInfo);
      columnCfgs.set(componentKey, columnCfg);
    }

    //Join Rows to SliceIds
    const rowMap: Map<SliceId, JoinColumn<any>[]> = new Map();
    for (const sliceId of mergedSliceIds) {
      let sliceIdRow: JoinColumn<any>[] = [];
      for (const [layerKey, layer] of layers.entries()) {
        const layerRef = layer._hash;
        const componentKey = layer.componentsTable;
        const componentRef = layer.add[sliceId];

        const componentsTable = data[componentKey] as ComponentsTable<Json>;
        const component = componentsTable._data.find(
          (r) => r._hash === componentRef,
        )!;
        const colCfgs = columnCfgs.get(componentKey)!;

        // Build Join Columns by Column Configs (for loop)
        const joinColumns: JoinColumn<any>[] = [];
        for (let i = 0; i < colCfgs.length; i++) {
          const columnCfg = colCfgs[i];
          joinColumns.push({
            route: Route.fromFlat(
              `${cakeKey}@${cakeRef}/${layerKey}@${layerRef}/${componentKey}@${componentRef}/${columnCfg.key}`,
            ).toRouteWithProperty(),
            value: component[columnCfg.key] ?? null,
            insert: null,
          } as JoinColumn<any>);
        }

        sliceIdRow = [...sliceIdRow, ...joinColumns];
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

    // Build ColumnSelection
    const joinColumnInfos = Array.from(columnInfos.values()).flat();
    const joinColumnSelection = new ColumnSelection(joinColumnInfos);

    // Return Join
    return new Join(joinRows, joinColumnSelection).select(columnSelection);
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
      const route = Route.fromFlat(colInfo.route).toRouteWithProperty();
      uniqueComponentRoutes.add(route.toRouteWithoutProperty().flat);
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
    options?: { skipNotification?: boolean },
  ): Promise<InsertHistoryRow<any>> {
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
    options?: { skipNotification?: boolean },
  ): Promise<InsertHistoryRow<any>> {
    let result: InsertHistoryRow<any>;
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
      const childRefs: Record<string, string> = {};

      for (const k of childKeys) {
        const childValue = (insert.value as any)[k];
        const childInsert: Insert<any> = { ...insert, value: childValue };
        const childResult = await this._insert(childInsert, childRoute, runFns);
        const childRefKey = childRoute.top.tableKey + 'Ref';
        const childRef = (childResult as any)[childRefKey] as string;

        childRefs[k] = childRef;
      }

      //Run parent controller with child refs as value
      const runFn = runFns[tableKey];
      result = {
        ...(await runFn(
          insert.command,
          {
            ...insert.value,
            ...childRefs,
          },
          insert.origin,
        )),
        previous,
      };
    } else {
      //Run root controller
      tableKey = route.root.tableKey;
      const runFn = runFns[tableKey];

      //Run on controller, get InsertHistoryRow from return, pass previous revisions
      result = {
        ...(await runFn(insert.command, insert.value, insert.origin)),
        previous,
      };
    }

    //Write route to result
    result.route = insert.route;

    //Write insertHistory
    await this._writeInsertHistory(tableKey, result);

    //Notify listeners
    if (!options?.skipNotification)
      this.notify.notify(Route.fromFlat(insert.route), result);

    return result;
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
        base ? { base } : undefined,
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
      tableData.sort((a, b) => {
        const aTime = a.timeId.split(':')[1];
        const bTime = b.timeId.split(':')[1];
        if (options.ascending) {
          return aTime.localeCompare(bTime);
        } else {
          return bTime.localeCompare(aTime);
        }
      });

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
    //Check if last segment is property of component
    const lastSegment = route.segments[route.segments.length - 1];
    const tableKey = lastSegment.tableKey;
    const tableExists = await this._io.tableExists(tableKey);
    if (!Route.segmentHasRef(lastSegment) && !tableExists) {
      //Last Segment is probably property of component
      const result = route.upper();
      result.propertyKey = lastSegment.tableKey;
      return result;
    }
    return route;
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
