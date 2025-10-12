// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Io, IoMem } from '@rljson/io';
import { Json } from '@rljson/json';
import {
  Edit,
  EditProtocolRow,
  EditProtocolTimeId,
  isTimeId,
  Ref,
  Rljson,
  Route,
  validateEdit,
} from '@rljson/rljson';

import {
  Controller,
  ControllerRefs,
  ControllerRunFn,
  createController,
} from './controller/controller.ts';
import { Core } from './core.ts';
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

    // Get Controllers
    const controllers = await this._indexedControllers(route);

    // Fetch Data
    return this._get(route, where, controllers);
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
        // Get ref from protocol
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
      return [rootRljson];
    }

    //Recursive Case: Fetch Child Data for each Root Ref
    const results: Rljson[] = [];
    for (const ref of rootRefs) {
      //Build superior route
      const superiorRoute = new Route(route.segments.slice(0, -1));

      //Fetch Superior Data
      const res = await this._get(
        superiorRoute,
        {
          [route.root.tableKey + 'Ref']: ref,
        },
        controllers,
      );
      //Merge Results
      results.push(...res);
    }
    return results.map((r) => ({ ...r, ...rootRljson }));
  }

  // ...........................................................................
  /**
   * Runs an edit by executing the appropriate controller(s) based on the edit's route
   * @param edit - The edit to run
   * @returns The result of the edit as an EditProtocolRow
   * @throws {Error} If the edit is not valid or if any controller cannot be created
   */
  async run(
    edit: Edit<any>,
    options?: { skipNotification?: boolean },
  ): Promise<EditProtocolRow<any>> {
    const initialRoute = Route.fromFlat(edit.route);
    const runs = await this._resolveRuns(edit);
    const errors = validateEdit(edit);
    if (!!errors.hasErrors) {
      throw new Error(`Edit is not valid:\n${JSON.stringify(errors, null, 2)}`);
    }

    return this._run(edit, initialRoute, runs, options);
  }

  // ...........................................................................
  /**
   * Recursively runs controllers based on the route of the edit
   * @param edit - The edit to run
   * @param route - The route of the edit
   * @param runFns - A record of controller run functions, keyed by table name
   * @returns The result of the edit
   * @throws {Error} If the route is not valid or if any controller cannot be created
   */
  private async _run(
    edit: Edit<any>,
    route: Route,
    runFns: Record<string, ControllerRunFn<any>>,
    options?: { skipNotification?: boolean },
  ): Promise<EditProtocolRow<any>> {
    let result: EditProtocolRow<any>;
    let tableKey: string;

    //Run parent controller with child refs as value
    const segment = route.segment(0);
    tableKey = segment.tableKey;

    let previous: EditProtocolTimeId[] = [];
    if (Route.segmentHasRef(segment)) {
      const routeRef: EditProtocolTimeId = Route.segmentRef(segment)!;
      if (Route.segmentHasProtocolRef(segment)) {
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

      //Iterate over child values and create edits for each
      const childKeys = this._childKeys(route, edit.value);
      const childRefs: Record<string, string> = {};

      for (const k of childKeys) {
        const childValue = (edit.value as any)[k];
        const childEdit: Edit<any> = { ...edit, value: childValue };
        const childResult = await this._run(childEdit, childRoute, runFns);
        const childRefKey = childRoute.top.tableKey + 'Ref';
        const childRef = (childResult as any)[childRefKey] as string;

        childRefs[k] = childRef;
      }

      //Run parent controller with child refs as value
      const runFn = runFns[tableKey];
      result = {
        ...(await runFn(
          edit.command,
          {
            ...edit.value,
            ...childRefs,
          },
          edit.origin,
        )),
        previous,
      };
    } else {
      //Run root controller
      tableKey = route.root.tableKey;
      const runFn = runFns[tableKey];

      //Run on controller, get EditProtocolRow from return, pass previous revisions
      result = {
        ...(await runFn(edit.command, edit.value, edit.origin)),
        previous,
      };
    }

    //Write route to result
    result.route = edit.route;

    //Write protocol
    await this._writeProtocol(tableKey, result);

    //Notify listeners
    if (!options?.skipNotification)
      this.notify.notify(Route.fromFlat(edit.route), result);

    return result;
  }

  // ...........................................................................
  /**
   * Registers a callback to be called when an edit is made on the given route
   * @param route - The route to register the callback on
   * @param callback - The callback to be called when an edit is made
   */
  registerObserver(
    route: Route,
    callback: (EditProtocolRow: EditProtocolRow<any>) => void,
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
    callback: (EditProtocolRow: EditProtocolRow<any>) => void,
  ) {
    this.notify.unregister(route, callback);
  }

  // ...........................................................................
  /**
   * Resolves an edit by returning the run functions of all controllers involved in the edit's route
   * @param edit - The edit to resolve
   * @returns A record of controller run functions, keyed by table name
   * @throws {Error} If the route is not valid or if any controller cannot be created
   */
  private async _resolveRuns(
    edit: Edit<any>,
  ): Promise<Record<string, ControllerRunFn<any>>> {
    // Get Controllers and their Run Functions
    const controllers = await this._indexedControllers(
      Route.fromFlat(edit.route),
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
    for (let i = 0; i < route.segments.length; i++) {
      const segment = route.segments[i];
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
   * Adds an edit protocol row to the edits table of a table
   * @param table - The table the edit was made on
   * @param editProtocolRow - The edit protocol row to add
   * @throws {Error} If the edits table does not exist
   */
  private async _writeProtocol(
    table: string,
    editProtocolRow: EditProtocolRow<any>,
  ): Promise<void> {
    const protocolTable = table + 'Edits';
    const hasTable = await this.core.hasTable(protocolTable);
    if (!hasTable) {
      throw new Error(`Table ${table} does not exist`);
    }

    //Write edit protocol row to io
    await this.core.import({
      [protocolTable]: {
        _data: [editProtocolRow],
        _type: 'edits',
      },
    });
  }

  // ...........................................................................
  /**
   * Get the edit protocol of a table
   * @param table - The table to get the edit protocol for
   * @throws {Error} If the edits table does not exist
   */
  async getProtocol(
    table: string,
    options?: { sorted?: boolean; ascending?: boolean },
  ): Promise<Rljson> {
    const protocolTable = table + 'Edits';
    const hasTable = await this.core.hasTable(protocolTable);
    if (!hasTable) {
      throw new Error(`Table ${table} does not exist`);
    }

    if (options === undefined) {
      options = { sorted: false, ascending: true };
    }

    if (options.sorted) {
      const dumpedTable = await this.core.dumpTable(protocolTable);
      const tableData = dumpedTable[protocolTable]
        ._data as EditProtocolRow<any>[];

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

      return { [protocolTable]: { _data: tableData, _type: 'edits' } };
    }

    return this.core.dumpTable(protocolTable);
  }

  // ...........................................................................
  /**
   * Get a specific edit protocol row from a table
   * @param table - The table to get the edit protocol row from
   * @param ref - The reference of the edit protocol row to get
   * @returns The edit protocol row or null if it does not exist
   * @throws {Error} If the edits table does not exist
   */
  async getProtocolRowsByRef(
    table: string,
    ref: string,
  ): Promise<EditProtocolRow<any>[] | null> {
    const protocolTable = table + 'Edits';
    const {
      [protocolTable]: { _data: protocol },
    } = await this.core.readRows(protocolTable, { [table + 'Ref']: ref });
    return (protocol as EditProtocolRow<any>[]) || null;
  }

  // ...........................................................................
  /**
   * Get a specific edit protocol row from a table by its timeId
   * @param table - The table to get the edit protocol row from
   * @param timeId - The timeId of the edit protocol row to get
   * @returns The edit protocol row or null if it does not exist
   * @throws {Error} If the edits table does not exist
   */
  async getProtocolRowByTimeId(
    table: string,
    timeId: EditProtocolTimeId,
  ): Promise<EditProtocolRow<any> | null> {
    const protocolTable = table + 'Edits';
    const { [protocolTable]: result } = await this.core.readRows(
      protocolTable,
      { timeId },
    );
    return result._data?.[0] || null;
  }

  // ...........................................................................
  /**
   * Get all timeIds for a specific ref in a table
   * @param table - The table to get the timeIds from
   * @param ref - The reference to get the timeIds for
   * @returns An array of timeIds
   * @throws {Error} If the edits table does not exist
   */
  async getTimeIdsForRef(
    table: string,
    ref: Ref,
  ): Promise<EditProtocolTimeId[]> {
    const protocolTable = table + 'Edits';
    const { [protocolTable]: result } = await this.core.readRows(
      protocolTable,
      { [table + 'Ref']: ref },
    );
    return result._data?.map((r) => r.timeId) || [];
  }

  // ...........................................................................
  /**
   * Get the ref for a specific timeId in a table
   * @param table - The table to get the ref from
   * @param timeId - The timeId to get the ref for
   * @returns The ref or null if it does not exist
   * @throws {Error} If the edits table does not exist
   */
  async getRefOfTimeId(
    table: string,
    timeId: EditProtocolTimeId,
  ): Promise<Ref | null> {
    const protocolTable = table + 'Edits';
    const { [protocolTable]: result } = await this.core.readRows(
      protocolTable,
      { timeId },
    );
    return (result._data?.[0] as any)?.[table + 'Ref'] || null;
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
