// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Io, IoMem } from '@rljson/io';
import { Edit, EditProtocolRow, Rljson, Route } from '@rljson/rljson';

import {
  Controller,
  ControllerRefs,
  ControllerRunFn,
  createController,
} from './controller/controller.ts';
import { Core } from './core.ts';

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
  }

  /**
   * Core functionalities like importing data, setting and getting tables
   */
  readonly core: Core;

  async resolve(edit: Edit<any>): Promise<EditProtocolRow<any>> {
    const route = Route.fromFlat(edit.route);
    const runs = await this._resolveRuns(edit);

    return this._run(edit, route, runs);
  }

  private async _run(
    edit: Edit<any>,
    route: Route,
    runFns: Record<string, ControllerRunFn<any>>,
  ): Promise<EditProtocolRow<any>> {
    if (!route.isRoot) {
      //Run nested controller first
      const childRoute = route.deeper(1);

      //Run child controller
      const childValues = Object.entries(edit.value);
      const childRefs: Record<string, string> = {};
      for (const [k, v] of childValues) {
        const childEdit: Edit<any> = { ...edit, value: v };
        const childResult = await this._run(childEdit, childRoute, runFns);
        const childRefKey = childRoute.segment() + 'Ref';
        const childRef = (childResult as any)[childRefKey] as string;

        childRefs[k] = childRef;
      }

      const runFn = runFns[route.segment(0)];
      if (!runFn) {
        throw new Error(`No controller found for route ${route.root}`);
      }

      const result = await runFn(
        edit.command,
        childRefs,
        edit.origin,
        edit.previous,
      );
      return { ...result, route: edit.route };
    } else {
      //Run root controller
      const runFn = runFns[route.root];
      if (!runFn) {
        throw new Error(`No controller found for route ${route.root}`);
      }
      const result = await runFn(
        edit.command,
        edit.value,
        edit.origin,
        edit.previous,
      );
      return { ...result, route: edit.route };
    }
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
    const controllers = await this.indexedControllers(
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
  private async indexedControllers(
    route: Route,
  ): Promise<Record<string, Controller<any, any>>> {
    // Validate Route
    if (!route.isValid) throw new Error(`Route ${route.flat} is not valid.`);

    // Create Controllers
    const controllers: Record<string, Controller<any, any>> = {};
    for (let i = 0; i < route.segments.length; i++) {
      const tableKey = route.segments[i];
      controllers[tableKey] ??= await this.getController(tableKey);
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
  async protocol(
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

  /**
   * Example
   * @returns A new Db instance for test purposes
   */
  static example = async () => {
    const io = new IoMem();
    return new Db(io);
  };
}
