// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Io, IoMem } from '@rljson/io';
import { Json } from '@rljson/json';
import {
  CakesTable,
  ColumnCfg,
  ComponentsTable,
  HistoryRow,
  HistoryTimeId,
  Insert,
  isTimeId,
  Layer,
  LayersTable,
  Ref,
  Rljson,
  Route,
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
import { Edit } from './edit/edit/edit.ts';
import { Join, JoinColumn, JoinRows } from './edit/join/join.ts';
import { SetValue } from './edit/join/set-value/set-value.ts';
import {
  ColumnInfo,
  ColumnSelection,
} from './edit/selection/column-selection.ts';
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

  // ...........................................................................
  /**
   * Get data from a route with optional filtering
   * @param route - The route to get data from
   * @param where - Optional filter to apply to the data
   * @returns An array of Rljson objects matching the route and filter
   * @throws {Error} If the route is not valid or if any controller cannot be created
   */
  async get(route: Route, where: string | Json): Promise<Rljson[]> {
    // Validate Route
    if (!route.isValid) throw new Error(`Route ${route.flat} is not valid.`);

    //Isolate Property Key
    const isolatedRoute = await this.isolatePropertyKeyFromRoute(route);

    // Get Controllers
    const controllers = await this._indexedControllers(isolatedRoute);

    // Fetch Data
    return this._get(isolatedRoute, where, controllers);
  }

  // ...........................................................................
  /**
   * Recursively fetches data from the given route using the provided controllers
   * @param route - The route to fetch data from
   * @param where - The filter to apply to the root table
   * @param controllers - A record of controllers keyed by table name
   * @returns An array of Rljson objects matching the route and filter
   */
  private async _get(
    route: Route,
    where: string | Json,
    controllers: Record<string, Controller<any, any>>,
  ): Promise<Rljson[]> {
    //Extract given Revision
    let filter = {};
    if (Route.segmentHasRef(route.root)) {
      let revision: Ref = '';
      if (Route.segmentHasDefaultRef(route.root)) {
        // Use given ref
        revision = Route.segmentRef(route.root)!;
      } else {
        // Get ref from history
        revision = (await this.getRefOfTimeId(
          route.root.tableKey,
          Route.segmentRef(route.root)!,
        ))!;
      }
      // Build where clause
      filter = { _hash: revision };
    }

    //Fetch Root Data
    const rootRljson: Rljson = await controllers[route.root.tableKey].get(
      where,
      filter,
    );
    //Extract Root Refs
    const rootRefs: Ref[] = rootRljson[route.root.tableKey]._data.map(
      (r) => r._hash,
    );

    //Base Case: If route has only one segment, return root data
    if (route.segments.length === 1) {
      if (route.hasPropertyKey) {
        return [
          this.isolatePropertyFromComponents(rootRljson, route.propertyKey!),
        ];
      }

      return [rootRljson];
    }

    //Recursive Case: Fetch Child Data for each Root Ref
    const results: Rljson[] = [];
    const superiorMap = new Map<string, Rljson>();
    for (const ref of rootRefs) {
      //Build superior route
      const superiorRoute = new Route(route.segments.slice(0, -1));

      if (superiorMap.has(superiorRoute.flat)) continue; //Already fetched

      //Fetch Superior Data
      const res = await this._get(
        superiorRoute,
        {
          [route.root.tableKey + 'Ref']: ref,
        },
        controllers,
      );

      //Add to Map
      superiorMap.set(superiorRoute.flat, res[0]);
    }

    results.push(...superiorMap.values());
    return results.map((r) => ({ ...r, ...rootRljson }));
  }

  // ...........................................................................
  /**
   * Runs an Edit by applying its actions to all rows matching its filter
   *
   * @param edit - The Edit to run
   * @param cakeKey - The cake table key
   * @param cakeRef - The cake reference
   * @returns void
   * @throws {Error} If the Edit is not valid or if any controller cannot be created
   */
  async saveEdit(
    edit: Edit,
    cakeKey: string,
    cakeRef: Ref,
  ): Promise<HistoryRow<any>[]> {
    //Get ColumnSelection from Edit
    const columnSelection = await this._getColumnSelectionFromEdit(edit);
    const join = await this.join(columnSelection, cakeKey, cakeRef);

    //Get Row Filter from Edit
    const filter = edit.filter;

    //Build SetValues from Edit Actions
    const setValues = edit.actions.map(
      (a) => ({ route: a.route, value: a.setValue } as SetValue),
    );

    //Apply Edit
    const editedJoin = join.filter(filter).setValues(setValues);

    //Insert changed data
    return await editedJoin.insert();
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
    const data = await this._getDataForColumnSelection(columnSelection);

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

      if (!layer) {
        throw new Error(
          `Db.join: Layer with ref "${cake.layers[layerKey]}" not found in layers table "${layerKey}".`,
        );
      }

      layers.set(layerKey, layer);
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

    //TODO: What about encapsulated components?

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
        );
        const colCfgs = columnCfgs.get(componentKey)!;

        // Build Join Columns by Column Configs (for loop)
        const joinColumns: JoinColumn<any>[] = [];
        for (let i = 0; i < colCfgs.length; i++) {
          const columnCfg = colCfgs[i];
          joinColumns.push({
            route: Route.fromFlat(
              `${cakeKey}@${cakeRef}/${layerKey}@${layerRef}/${componentKey}@${componentRef}/${columnCfg.key}`,
            ).toRouteWithProperty(),
            value: component ? component[columnCfg.key] ?? null : null,
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
    return new Join(joinRows, joinColumnSelection, this);
  }

  // ...........................................................................
  /**
   * Get the ColumnSelection for an Edit
   * @param edit - The Edit to get the ColumnSelection for
   * @returns
   */
  private async _getColumnSelectionFromEdit(edit: Edit) {
    // Determine routes from filter
    const routes = edit.filter.columnFilters;
    const columnInfos: ColumnInfo[] = [];
    for (const cf of routes) {
      const route = Route.fromFlat(cf.column).toRouteWithProperty();
      const tableKey = route.root.tableKey;
      const tableCfg = await this.core.tableCfg(tableKey);
      const columnCfg = tableCfg.columns.find(
        (c) => c.key === route.propertyKey,
      );
      if (!columnCfg) {
        throw new Error(
          `Db.getColumnSelection: Column "${route.propertyKey}" not found in table "${tableKey}".`,
        );
      }
      columnInfos.push({
        ...columnCfg,
        alias: `${columnCfg.key}`,
        route: route.flat.slice(1),
        titleShort: columnCfg.key,
        titleLong: columnCfg.key,
      });
    }
    return new ColumnSelection(columnInfos);
  }

  // ...........................................................................
  /**
   * Fetches data for the given ColumnSelection
   * @param columnSelection - The ColumnSelection to fetch data for
   */
  private async _getDataForColumnSelection(columnSelection: ColumnSelection) {
    // Fetch Data from all controller routes
    const data: Rljson = {};
    for (const colInfo of columnSelection.columns) {
      const route = Route.fromFlat(colInfo.route).toRouteWithProperty();
      Object.assign(data, ...(await this.get(route, {})));
    }
    return data;
  }

  // ...........................................................................
  /**
   * Runs an Insert by executing the appropriate controller(s) based on the Insert's route
   * @param Insert - The Insert to run
   * @returns The result of the Insert as an HistoryRow
   * @throws {Error} If the Insert is not valid or if any controller cannot be created
   */
  async insert(
    insert: Insert<any>,
    options?: { skipNotification?: boolean },
  ): Promise<HistoryRow<any>> {
    const initialRoute = Route.fromFlat(insert.route);
    const runs = await this._resolveInsert(insert);
    const errors = validateInsert(insert);
    if (!!errors.hasErrors) {
      throw new Error(
        `Insert is not valid:\n${JSON.stringify(errors, null, 2)}`,
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
  ): Promise<HistoryRow<any>> {
    let result: HistoryRow<any>;
    let tableKey: string;

    //Run parent controller with child refs as value
    const segment = route.segment(0);
    tableKey = segment.tableKey;

    let previous: HistoryTimeId[] = [];
    if (Route.segmentHasRef(segment)) {
      const routeRef: HistoryTimeId = Route.segmentRef(segment)!;
      if (Route.segmentHasHistoryRef(segment)) {
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
      const childKeys = this._childKeys(route, insert.value);
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

      //Run on controller, get HistoryRow from return, pass previous revisions
      result = {
        ...(await runFn(insert.command, insert.value, insert.origin)),
        previous,
      };
    }

    //Write route to result
    result.route = insert.route;

    //Write history
    await this._writeHistory(tableKey, result);

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
    callback: (HistoryRow: HistoryRow<any>) => void,
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
    callback: (HistoryRow: HistoryRow<any>) => void,
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
      runFns[tableKey] = controllers[tableKey].run.bind(controllers[tableKey]);
    }

    return runFns;
  }

  // ...........................................................................
  /**
   * Returns the keys of child refs in a value based on a route
   * @param route - The route to check
   * @param value - The value to check
   * @returns An array of keys of child refs in the value
   */
  private _childKeys(route: Route, value: Json): string[] {
    const keys = Object.keys(value);
    const childKeys: string[] = [];
    for (const k of keys) {
      if (typeof (value as any)[k] !== 'object') continue;
      /* v8 ignore start */
      if (k.endsWith('Ref') && route.next?.tableKey + 'Ref' !== k) continue;
      /* v8 ignore end */

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
    //Guard: tableKey must be a non-empty string
    if (typeof tableKey !== 'string' || tableKey.length === 0) {
      throw new Error('TableKey must be a non-empty string.');
    }

    // Validate Table
    const tableExists = this.core.hasTable(tableKey);
    if (!tableExists) {
      throw new Error(`Table ${tableKey} does not exist.`);
    }

    // Get Content Type of Table
    const contentType = await this.core.contentType(tableKey);
    if (!contentType) {
      throw new Error(`Table ${tableKey} does not have a valid content type.`);
    }

    // Create Controller
    return createController(contentType, this.core, tableKey, refs);
  }

  // ...........................................................................
  private async _indexedControllers(
    route: Route,
  ): Promise<Record<string, Controller<any, any>>> {
    // Validate Route
    if (!route.isValid) throw new Error(`Route ${route.flat} is not valid.`);

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
   * Adds an History row to the History table of a table
   * @param table - The table the Insert was made on
   * @param HistoryRow - The History row to add
   * @throws {Error} If the History table does not exist
   */
  private async _writeHistory(
    table: string,
    historyRow: HistoryRow<any>,
  ): Promise<void> {
    const historyTable = table + 'History';
    const hasTable = await this.core.hasTable(historyTable);
    if (!hasTable) {
      throw new Error(`Table ${table} does not exist`);
    }

    //Write History row to io
    await this.core.import({
      [historyTable]: {
        _data: [historyRow],
        _type: 'history',
      },
    });
  }

  // ...........................................................................
  /**
   * Get the History of a table
   * @param table - The table to get the History for
   * @throws {Error} If the History table does not exist
   */
  async getHistory(
    table: string,
    options?: { sorted?: boolean; ascending?: boolean },
  ): Promise<Rljson> {
    const historyTable = table + 'History';
    const hasTable = await this.core.hasTable(historyTable);
    if (!hasTable) {
      throw new Error(`Table ${table} does not exist`);
    }

    if (options === undefined) {
      options = { sorted: false, ascending: true };
    }

    if (options.sorted) {
      const dumpedTable = await this.core.dumpTable(historyTable);
      const tableData = dumpedTable[historyTable]._data as HistoryRow<any>[];

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

      return { [historyTable]: { _data: tableData, _type: 'history' } };
    }

    return this.core.dumpTable(historyTable);
  }

  // ...........................................................................
  /**
   * Get a specific History row from a table
   * @param table - The table to get the History row from
   * @param ref - The reference of the History row to get
   * @returns The History row or null if it does not exist
   * @throws {Error} If the Inserts table does not exist
   */
  async getHistoryRowsByRef(
    table: string,
    ref: string,
  ): Promise<HistoryRow<any>[] | null> {
    const historyTable = table + 'History';
    const {
      [historyTable]: { _data: history },
    } = await this.core.readRows(historyTable, { [table + 'Ref']: ref });
    return (history as HistoryRow<any>[]) || null;
  }

  // ...........................................................................
  /**
   * Get a specific History row from a table by its timeId
   * @param table - The table to get the History row from
   * @param timeId - The timeId of the History row to get
   * @returns The History row or null if it does not exist
   * @throws {Error} If the Inserts table does not exist
   */
  async getHistoryRowByTimeId(
    table: string,
    timeId: HistoryTimeId,
  ): Promise<HistoryRow<any> | null> {
    const historyTable = table + 'History';
    const { [historyTable]: result } = await this.core.readRows(historyTable, {
      timeId,
    });
    return result._data?.[0] || null;
  }

  // ...........................................................................
  /**
   * Get all timeIds for a specific ref in a table
   * @param table - The table to get the timeIds from
   * @param ref - The reference to get the timeIds for
   * @returns An array of timeIds
   * @throws {Error} If the Inserts table does not exist
   */
  async getTimeIdsForRef(table: string, ref: Ref): Promise<HistoryTimeId[]> {
    const historyTable = table + 'History';
    const { [historyTable]: result } = await this.core.readRows(historyTable, {
      [table + 'Ref']: ref,
    });
    return result._data?.map((r) => r.timeId) || [];
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
    timeId: HistoryTimeId,
  ): Promise<Ref | null> {
    const historyTable = table + 'History';
    const { [historyTable]: result } = await this.core.readRows(historyTable, {
      timeId,
    });
    return (result._data?.[0] as any)?.[table + 'Ref'] || null;
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
          return { [propertyKey]: row[propertyKey] };
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

  /**
   * Example
   * @returns A new Db instance for test purposes
   */
  static example = async () => {
    const io = new IoMem();
    return new Db(io);
  };
}
