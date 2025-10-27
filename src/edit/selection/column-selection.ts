// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Hash, hip } from '@rljson/hash';
import { ColumnCfgWithRoute, Ref, Route } from '@rljson/rljson';

export type ColumnRoute = string | string[] | number;

export interface ColumnInfo extends ColumnCfgWithRoute {
  alias: string;
  routeHash?: Ref;
  index?: number;
  value?: any;
}

export class ColumnSelection {
  constructor(columns: ColumnInfo[]) {
    this._throwOnWrongAlias(columns);

    this.routes = columns.map((column) => column.route);
    this.routeHashes = this.routes.map(ColumnSelection.calcHash);

    this.aliases = columns.map((column) => column.alias);
    this.columns = this._initColumns(columns);

    ColumnSelection.check(this.aliases, this.routes);
  }

  // ...........................................................................
  /**
   * Returns unique routes from a list of routes
   * @param routes - The list of routes
   * @returns Unique routes
   */
  static uniqueRoutes(routes: Route[]): Route[] {
    return Array.from(new Set(routes.map((r) => r.flat))).map((flat) =>
      Route.fromFlat(flat),
    );
  }

  // ...........................................................................
  /**
   * Returns a ColumnSelection from a list of route segments
   * @param routeSegmentsList - A list of route segments
   * @returns A ColumnSelection object
   */
  static fromRoutes(routes: Route[]): ColumnSelection {
    const definition: ColumnInfo[] = [];
    const aliasCountMap: Record<string, number> = {};

    for (const route of this.uniqueRoutes(routes)) {
      const alias = route.root.tableKey;
      let uniqueAlias = alias;
      const aliasCount = aliasCountMap[alias] ?? 0;
      if (aliasCount > 0) {
        uniqueAlias = `${alias}${aliasCount}`;
      }
      aliasCountMap[alias] = aliasCount + 1;

      definition.push({
        key: uniqueAlias,
        type: 'jsonValue',
        alias: uniqueAlias,
        route: route.flat.slice(1),
        titleLong: '',
        titleShort: '',
      });
    }

    return new ColumnSelection(definition);
  }

  // ...........................................................................
  readonly columns: ColumnInfo[];
  readonly routes: string[];
  readonly aliases: string[];

  readonly routeHashes: string[];

  metadata(key: string): any[] {
    return this.columns.map((column) => column[key]);
  }

  // ...........................................................................
  static merge(columnSelections: ColumnSelection[]): ColumnSelection {
    //Flatten all routes from all selections
    const routes = columnSelections.map((selection) => selection.routes).flat();

    //Store strings here, because Set does not allow duplicates (on basic types)
    const routesWithoutDuplicates = Array.from(new Set(routes));

    //Create Route objects from flat (unique) strings
    return ColumnSelection.fromRoutes(
      routesWithoutDuplicates.map((route) => Route.fromFlat(route)),
    );
  }

  // ...........................................................................
  static calcHash(str: string): string {
    return Hash.default.calcHash(str);
  }

  route(aliasRouteOrHash: ColumnRoute): string {
    return this.column(aliasRouteOrHash).route;
  }

  // ...........................................................................
  alias(aliasRouteOrHash: ColumnRoute): string {
    return this.column(aliasRouteOrHash).alias;
  }

  // ...........................................................................
  columnIndex(
    hashAliasOrRoute: ColumnRoute,
    throwIfNotExisting: boolean = true,
  ): number {
    if (typeof hashAliasOrRoute === 'number') {
      return hashAliasOrRoute;
    }

    const str = Array.isArray(hashAliasOrRoute)
      ? hashAliasOrRoute.join('/')
      : hashAliasOrRoute;

    const hashIndex = this.routeHashes.indexOf(str);
    if (hashIndex >= 0) {
      return hashIndex;
    }

    const aliasIndex = this.aliases.indexOf(str);
    if (aliasIndex >= 0) {
      return aliasIndex;
    }

    const routeIndex = this.routes.indexOf(str);

    if (routeIndex < 0) {
      if (throwIfNotExisting) {
        throw new Error(`Unknown column alias or route: ${str}`);
      }
      return -1;
    }

    return routeIndex;
  }

  /***
   * Returns the column config for a specific alias, route or hash.
   */
  column(aliasRouteOrHash: ColumnRoute): ColumnInfo {
    const index = this.columnIndex(aliasRouteOrHash);
    return this.columns[index];
  }

  // ...........................................................................
  get count(): number {
    return this.aliases.length;
  }

  // ...........................................................................
  addedColumns(columnSelection: ColumnSelection): string[] {
    const a = this.routes.filter(
      (route) => !columnSelection.routes.includes(route),
    );

    return a;
  }

  // ...........................................................................
  static check(aliases: string[], routes: string[]) {
    // Make shure all keys are lowercase camel case
    // Numbers are not allowed at the beginning
    const camelCaseRegex = /^[a-z][a-zA-Z0-9]*$/;
    const invalidKeys = aliases.filter((key) => !camelCaseRegex.test(key));
    if (invalidKeys.length > 0) {
      throw new Error(
        `Invalid alias "${invalidKeys[0]}". ` +
          'Aliases must be lower camel case.',
      );
    }

    // Values must only be letters, numbers and slashes
    const validValueRegex = /^[a-zA-Z0-9/]*$/;
    const invalidValues = routes.filter(
      (value) => !validValueRegex.test(value),
    );
    if (invalidValues.length > 0) {
      throw new Error(
        `Invalid route "${invalidValues}". ` +
          'Routes must only contain letters, numbers and slashes.',
      );
    }

    // All path parts must be lower camel case
    const pathParts = routes.map((value) => value.split('/')).flat();
    const invalidPathParts = pathParts.filter(
      (part) => !camelCaseRegex.test(part),
    );

    if (invalidPathParts.length > 0) {
      throw new Error(
        `Invalid route segment "${invalidPathParts[0]}". ` +
          'Route segments must be lower camel case.',
      );
    }

    // Routes must not occur more than once
    const routeCountMap: Record<string, number> = {};
    routes.forEach((value) => {
      routeCountMap[value] = (routeCountMap[value] ?? 0) + 1;
    });

    const duplicateRoutes = Object.entries(routeCountMap)
      .filter(([, count]) => count > 1)
      .map(([route]) => route);

    if (duplicateRoutes.length > 0) {
      throw new Error(
        `Duplicate route ${duplicateRoutes[0]}. A column must only occur once.`,
      );
    }
  }

  // ######################
  // Private
  // ######################

  private _throwOnWrongAlias(columns: ColumnInfo[]): void {
    const aliases = new Set<string>();
    for (const column of columns) {
      if (aliases.has(column.alias)) {
        throw new Error(`Duplicate alias: ${column.alias}`);
      }
      aliases.add(column.alias);
    }
  }

  private _initColumns(columns: ColumnInfo[]): ColumnInfo[] {
    let i = 0;
    return columns.map((column) =>
      hip({
        ...column,
        routeHash: this.routeHashes[i],
        index: i++,
        _hash: '',
      }),
    );
  }

  // ######################
  // Example
  // ######################

  static example(): ColumnSelection {
    return new ColumnSelection([
      {
        key: 'stringCol',
        alias: 'stringCol',
        route: 'basicTypes/stringsRef/value',
        type: 'string',
        titleLong: 'String values',
        titleShort: 'Strings',
      },
      {
        key: 'intCol',
        alias: 'intCol',
        route: 'basicTypes/numbersRef/intsRef/value',
        type: 'number',
        titleLong: 'Int values',
        titleShort: 'Ints',
      },
      {
        key: 'floatCol',
        alias: 'floatCol',
        route: 'basicTypes/numbersRef/floatsRef/value',
        type: 'number',
        titleLong: 'Float values',
        titleShort: 'Floats',
      },
      {
        key: 'booleanCol',
        alias: 'booleanCol',
        route: 'basicTypes/booleansRef/value',
        type: 'boolean',
        titleLong: 'Boolean values',
        titleShort: 'Booleans',
      },
      {
        key: 'jsonObjectCol',
        alias: 'jsonObjectCol',
        route: 'complexTypes/jsonObjectsRef/value',
        type: 'json',
        titleLong: 'Json objects',
        titleShort: 'JO',
      },
      {
        key: 'jsonArrayCol',
        alias: 'jsonArrayCol',
        route: 'complexTypes/jsonArraysRef/value',
        type: 'jsonArray',
        titleLong: 'Array values',
        titleShort: 'JA',
      },
      {
        key: 'jsonValueCol',
        alias: 'jsonValueCol',
        route: 'complexTypes/jsonValuesRef/value',
        type: 'jsonValue',
        titleLong: 'Json values',
        titleShort: 'JV',
      },
    ]);
  }

  static exampleBroken(): ColumnInfo[] {
    return [
      {
        key: 'stringCol',
        alias: 'stringCol',
        route: 'basicTypes/stringsRef/value',
        type: 'string',
        titleLong: 'String values',
        titleShort: 'Strings',
      },
      {
        key: 'stringCol2',
        alias: 'stringCol', // ⚠️ Duplicate alias
        route: 'basicTypes/stringsRef/value',
        type: 'string',
        titleLong: 'String values',
        titleShort: 'Strings',
      },
    ];
  }

  // ...........................................................................
  static empty(): ColumnSelection {
    return new ColumnSelection([]);
  }
}
