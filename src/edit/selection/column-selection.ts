// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Hash, hip } from '@rljson/hash';
import { ColumnCfgWithRoute, Ref, Route } from '@rljson/rljson';

export type ColumnAddress = string | string[] | number;

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
   * Returns a ColumnSelection from a list of address segments
   * @param addressSegmentsList - A list of address segments
   * @returns A ColumnSelection object
   */
  static fromRoutes(routes: Route[]): ColumnSelection {
    const definition: ColumnInfo[] = [];
    const aliasCountMap: Record<string, number> = {};

    for (const route of routes) {
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
        route: route.flat,
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

  route(aliasAddressOrHash: ColumnAddress): string {
    return this.column(aliasAddressOrHash).route;
  }

  // ...........................................................................
  alias(aliasAddressOrHash: ColumnAddress): string {
    return this.column(aliasAddressOrHash).alias;
  }

  // ...........................................................................
  columnIndex(
    hashAliasOrAddress: ColumnAddress,
    throwIfNotExisting: boolean = true,
  ): number {
    if (typeof hashAliasOrAddress === 'number') {
      return hashAliasOrAddress;
    }

    const str = Array.isArray(hashAliasOrAddress)
      ? hashAliasOrAddress.join('/')
      : hashAliasOrAddress;

    const hashIndex = this.routeHashes.indexOf(str);
    if (hashIndex >= 0) {
      return hashIndex;
    }

    const aliasIndex = this.aliases.indexOf(str);
    if (aliasIndex >= 0) {
      return aliasIndex;
    }

    const addressIndex = this.routes.indexOf(str);

    if (addressIndex < 0) {
      if (throwIfNotExisting) {
        throw new Error(`Unknown column alias or address: ${str}`);
      }
      return -1;
    }

    return addressIndex;
  }

  /***
   * Returns the column config for a specific alias, address or hash.
   */
  column(aliasAddressOrHash: ColumnAddress): ColumnInfo {
    const index = this.columnIndex(aliasAddressOrHash);
    return this.columns[index];
  }

  // ...........................................................................
  get count(): number {
    return this.aliases.length;
  }

  // ...........................................................................
  addedColumns(columnSelection: ColumnSelection): string[] {
    const a = this.routes.filter(
      (address) => !columnSelection.routes.includes(address),
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
        `Invalid address "${invalidValues}". ` +
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
        `Invalid address segment "${invalidPathParts[0]}". ` +
          'Address segments must be lower camel case.',
      );
    }

    // Routes must not occur more than once
    const addressCountMap: Record<string, number> = {};
    routes.forEach((value) => {
      addressCountMap[value] = (addressCountMap[value] ?? 0) + 1;
    });

    const duplicateRoutes = Object.entries(addressCountMap)
      .filter(([, count]) => count > 1)
      .map(([address]) => address);

    if (duplicateRoutes.length > 0) {
      throw new Error(
        `Duplicate address ${duplicateRoutes[0]}. A column must only occur once.`,
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
        key: 'brand',
        alias: 'stringCol',
        route: 'carGeneral/brand',
        type: 'string',
        titleLong: 'String values',
        titleShort: 'Strings',
      },
      {
        key: 'doors',
        alias: 'intCol',
        route: 'carGeneral/doors',
        type: 'number',
        titleLong: 'Int values',
        titleShort: 'Ints',
      },
      {
        key: 'energyConsumption',
        alias: 'floatCol',
        route: 'carGeneral/energyConsumption',
        type: 'number',
        titleLong: 'Float values',
        titleShort: 'Floats',
      },
      {
        key: 'isElectric',
        alias: 'booleanCol',
        route: 'carGeneral/isElectric',
        type: 'boolean',
        titleLong: 'Boolean values',
        titleShort: 'Booleans',
      },
      {
        key: 'energyUnit',
        alias: 'jsonObjectCol',
        route: 'carGeneral/energyUnit',
        type: 'json',
        titleLong: 'Json objects',
        titleShort: 'JO',
      },
      {
        key: 'serviceIntervals',
        alias: 'jsonArrayCol',
        route: 'carGeneral/serviceIntervals',
        type: 'jsonArray',
        titleLong: 'Array values',
        titleShort: 'JA',
      },
      {
        key: 'meta',
        alias: 'jsonValueCol',
        route: 'carGeneral/meta',
        type: 'jsonValue',
        titleLong: 'Json values',
        titleShort: 'JV',
      },
    ]);
  }

  // ...........................................................................
  static empty(): ColumnSelection {
    return new ColumnSelection([]);
  }
}
