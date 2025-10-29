// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Hash } from '@rljson/hash';
import { Json, JsonValueType } from '@rljson/json';
import {
  ContentType,
  HistoryRow,
  Insert,
  Ref,
  Route,
  SliceId,
} from '@rljson/rljson';

import { Db } from '../../db.ts';

import { Edit } from '../edit/edit.ts';
import { ColumnSelection } from '../selection/column-selection.ts';
import { View } from '../view/view.ts';

import { RowFilterProcessor } from './filter/row-filter-processor.ts';
import { RowFilter } from './filter/row-filter.ts';
import { JoinView } from './join-view.ts';
import { SetValue } from './set-value/set-value.ts';

export type JoinProcessType = 'filter' | 'setValue' | 'selection' | 'sort';

export type JoinProcess = {
  type: JoinProcessType;
  instance: RowFilter | Edit | ColumnSelection | SetValue;
  data: JoinRowsHashed;
  columnSelection: ColumnSelection;
};

export interface JoinColumn<T extends ContentType> {
  route: Route;
  value: T | null;
  insert: T | null;
}

export type JoinRow = JoinColumn<any>[];
export type JoinRows = Record<SliceId, JoinRow>;

export type JoinRowHashed = {
  rowHash: Ref;
  columns: JoinColumn<any>[];
};
export type JoinRowsHashed = Record<SliceId, JoinRowHashed>;

export type CakeInsertObject = {
  [cakeRoute: string]: {
    route: Route;
    value: {
      [layerRoute: string]: {
        [sliceId: string]: Json | null;
      };
    };
  };
};

export type LayerInsertObject = {
  [layerRoute: string]: {
    route: Route;
    value: {
      [sliceId: string]: Json | null;
    };
  };
};

export type ComponentInsertObject = {
  [componentRoute: string]: {
    route: Route;
    value: Json | null;
  };
};

export class Join {
  private _base: JoinRowsHashed = {};
  private _processes: JoinProcess[] = [];

  constructor(
    baseRows: JoinRows,
    private _baseColumnSelection: ColumnSelection,
    private _db: Db,
  ) {
    // Hash the rows
    this._base = this._hashedRows(baseRows);
  }

  // ...........................................................................
  /**
   * Applies a filter to the join and returns the filtered view
   *
   * @param filter The filter to apply
   */
  filter(filter: RowFilter): Join {
    const proc = RowFilterProcessor.fromModel(filter);

    const data = proc.applyToJoin(this);

    // Create the process entry
    const process: JoinProcess = {
      type: 'filter',
      instance: filter,
      data,
      columnSelection: this.columnSelection,
    };

    // Store the process
    this._processes.push(process);

    return this;
  }

  // ...........................................................................
  /**
   * Applies a set value action to the join and returns the edited join
   *
   * @param setValue The set value action to apply
   */
  setValue(setValue: SetValue): Join {
    const data: JoinRowsHashed = {};

    for (const [sliceId, joinRowH] of Object.entries(this.data)) {
      const cols = [...joinRowH.columns];

      for (const col of cols) {
        if (Route.fromFlat(setValue.route).equalsWithoutRefs(col.route))
          col.insert = setValue.value;
      }

      data[sliceId] = {
        rowHash: Hash.default.calcHash(cols.map((c) => c.value) as any[]),
        columns: cols,
      };
    }

    // Create the process entry
    const process: JoinProcess = {
      type: 'setValue',
      instance: setValue,
      data,
      columnSelection: this.columnSelection,
    };

    // Store the process
    this._processes.push(process);

    return this;
  }

  // ...........................................................................
  /**
   * Applies multiple set value actions to the join and returns the edited join
   *
   * @param setValues The set value actions to apply
   */
  setValues(setValues: SetValue[]): Join {
    // Apply the set values one by one to a copy of this join
    let result: Join = this.clone();

    for (const setValue of setValues) {
      result = result.setValue(setValue);
    }

    return result;
  }

  // ...........................................................................
  /**
   * Inserts changed values into RLJSON DB
   */
  async insert(): Promise<HistoryRow<any>[]> {
    const historyRows: HistoryRow<any>[] = [];
    const cakeInserts = this._insertCakeObject(this.data);

    for (const cakeInsert of cakeInserts) {
      const route = cakeInsert[this.cakeRoute.root.tableKey].route.flat;
      const value = cakeInsert[this.cakeRoute.root.tableKey].value;

      const insert: Insert<any> = {
        command: 'add',
        route,
        value,
      };

      const historyRow = await this._db.insert(insert);
      historyRows.push(historyRow);
    }

    return historyRows;
  }

  // ...........................................................................
  /**
   * Builds cake insert objects from the given join rows
   * @param rows - The join rows
   * @returns The cake insert objects
   */
  private _insertCakeObject(rows: JoinRowsHashed): CakeInsertObject[] {
    const cakeInsertObjects: CakeInsertObject[] = [];
    const cakeRoute = this.cakeRoute;

    for (const [sliceId, row] of Object.entries(rows)) {
      const layerInsertObjectList = this._insertLayerObjects(
        sliceId,
        row.columns,
      );

      for (const layerInsertObject of layerInsertObjectList) {
        const cakeInsertObject: CakeInsertObject = {};
        for (const [layerRoute, layerInsertObj] of Object.entries(
          layerInsertObject,
        )) {
          cakeInsertObject[cakeRoute.root.tableKey] = {
            route: layerInsertObj.route,
            value: {
              [layerRoute]: layerInsertObj.value,
            },
          };
        }
        cakeInsertObjects.push(cakeInsertObject);
      }
    }

    return cakeInsertObjects;
  }

  // ...........................................................................
  /**
   * Wraps component insert objects into layer insert objects
   * @param sliceId - The slice id
   * @param componentInsertObjects - The component insert objects
   * @returns
   */
  private _insertLayerObjects(
    sliceId: SliceId,
    row: JoinRow,
  ): LayerInsertObject[] {
    const layerRoutes: Route[] = this.layerRoutes;
    const layerInsertObjects: LayerInsertObject[] = [];
    const insertComponentObjects = this._insertComponentObjects(row);

    for (const layerRoute of layerRoutes) {
      for (const [compRouteFlat, compInsertObj] of Object.entries(
        insertComponentObjects,
      )) {
        const compRoute = Route.fromFlat(compRouteFlat);

        if (layerRoute.includes(compRoute)) {
          const layerInsertObj: {
            [layerRoute: string]: {
              route: Route;
              value: {
                [sliceId: string]: Json | null;
              };
            };
          } = {};
          layerInsertObj[layerRoute.root.tableKey] = {
            route: compInsertObj.route,
            value: {
              [sliceId]: compInsertObj.value,
            },
          };
          layerInsertObjects.push(layerInsertObj);
        }
      }
    }
    return layerInsertObjects;
  }
  // ...........................................................................
  /**
   * Merges columns into component insert objects
   * @param columns - The columns to merge
   * @returns
   */
  private _insertComponentObjects(
    columns: JoinColumn<any>[],
  ): ComponentInsertObject {
    // Find all component routes
    const componentRoutes = this.componentRoutes;

    // Create insert objects for all component routes
    const result = {} as ComponentInsertObject;
    for (const compRoute of componentRoutes) {
      let compChanged: boolean = false;
      const compInsert = {} as Json;

      // Find all columns that belong to the component
      for (const c of columns) {
        if (compRoute.includes(c.route)) {
          if (c.insert !== null) {
            compChanged = true;
            compInsert[c.route.propertyKey! as string] = c.insert;
          } else {
            compInsert[c.route.propertyKey! as string] = c.value;
          }
        }
      }
      result[compRoute.flat] = {
        route: compRoute,
        value: compChanged ? compInsert : null,
      };
    }

    return result;
  }

  // ...........................................................................
  /**
   * Returns all component routes of the join
   */
  get componentRoutes(): Route[] {
    return Array.from(
      new Set(
        Object.values(this.data)
          .flatMap((r) => r.columns)
          .map((c) => c.route.flatWithoutPropertyKey),
      ),
    ).map((r) => Route.fromFlat(r));
  }

  // ...........................................................................
  /**
   * Returns all layer routes of the join
   */
  get layerRoutes(): Route[] {
    return Array.from(
      new Set(this.componentRoutes.map((r) => r.upper().flat)),
    ).map((r) => Route.fromFlat(r));
  }

  // ...........................................................................
  /**
   * Returns the cake route of the join
   */
  get cakeRoute(): Route {
    const cakeRoute = Array.from(
      new Set(this.layerRoutes.map((r) => r.upper().flat)),
    ).map((r) => Route.fromFlat(r));
    if (cakeRoute.length !== 1) {
      throw new Error(
        `Join: Error while getting cake route: ` +
          `The join has ${cakeRoute.length} different cake routes. ` +
          `Cannot determine a single cake route.`,
      );
    }
    return cakeRoute[0];
  }

  // ...........................................................................
  /**
   * Hashes the given join rows
   *
   * @param rows The join rows to hash
   * @returns The hashed join rows
   */
  private _hashedRows(rows: JoinRows) {
    const sliceIds = Object.keys(rows);
    const hashedRows: JoinRowsHashed = {};
    for (const sliceId of sliceIds) {
      const columns = rows[sliceId];
      const rowHash = Hash.default.calcHash(
        columns.map((col) => col.value) as any[],
      );
      hashedRows[sliceId] = {
        rowHash,
        columns,
      };
    }
    return hashedRows;
  }

  // ...........................................................................
  /**
   * Returns the value at the given row and column index
   *
   * @param row The row index
   * @param column The column index
   * @returns The value at the given row and column
   */
  value(row: number, column: number): any {
    return this.rows[row][column];
  }

  // ...........................................................................
  /**
   * Clones the join
   *
   * @returns The cloned join
   */
  clone(): Join {
    const cloned = Object.create(this);
    cloned._data = this._base;
    cloned._processes = [...this._processes];
    return cloned;
  }

  // ...........................................................................
  /**
   * Returns the number of rows in the join
   */
  get rowCount(): number {
    return Object.keys(this.data).length;
  }

  // ...........................................................................
  /**
   * Returns the number of columns in the join
   */
  get columnCount(): number {
    return this._baseColumnSelection.count;
  }

  // ...........................................................................
  /**
   * Returns a view representation of the join
   */
  get view(): View {
    return new JoinView(this);
  }

  // ...........................................................................
  /**
   * Returns the data of the join
   */
  get data(): JoinRowsHashed {
    if (this._processes.length > 0) {
      return this._processes[this._processes.length - 1].data;
    }
    return this._base;
  }

  // ...........................................................................
  /**
   * Returns the column types of the join
   */
  get columnTypes(): JsonValueType[] {
    return this.columnSelection.columns.map((col) => col.type);
  }

  // ...........................................................................
  /**
   * Returns the column selection of the join
   */
  get columnSelection(): ColumnSelection {
    if (this._processes.length > 0) {
      return this._processes[this._processes.length - 1].columnSelection;
    }
    return this._baseColumnSelection;
  }

  // ...........................................................................
  /**
   * Returns all rows of the join w/ nulled missing values
   *
   * @return The rows of the join
   */
  get rows(): any[][] {
    const result: any[][] = [];
    const sliceIds = Object.keys(this.data);
    for (const sliceId of sliceIds) {
      const dataColumns = (this.data[sliceId] as JoinRowHashed).columns;
      const row: any[] = [];
      for (const colInfo of this._baseColumnSelection.columns) {
        const joinCol = dataColumns.find((dataCol) => {
          const colInfoRoute = Route.fromFlat(colInfo.route);
          const dataColRoute = dataCol.route;
          return colInfoRoute.equalsWithoutRefs(dataColRoute);
        });
        row.push(joinCol ? joinCol.value : null);
      }
      result.push(row);
    }
    return result;
  }
}
