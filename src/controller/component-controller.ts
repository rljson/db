// @license
// Copyright (c) 2025 Rljson
//
import { hsh } from '@rljson/hash';
// Use of this source code is governed by terms that can be
import { equals, Json, JsonValue, merge } from '@rljson/json';
// found in the LICENSE file in the root of this package.
import {
  ColumnCfg,
  ComponentsTable,
  HistoryRow,
  Ref,
  Rljson,
  TableCfg,
  TableKey,
  timeId,
} from '@rljson/rljson';

import { Core } from '../core.ts';

import { BaseController } from './base-controller.ts';
import {
  Controller,
  ControllerCommands,
  ControllerRefs,
} from './controller.ts';

export class ComponentController<N extends string, T extends Json>
  extends BaseController<ComponentsTable<T>>
  implements Controller<ComponentsTable<T>, N>
{
  private _tableCfg: TableCfg | null = null;
  private _resolvedColumns: {
    base: ColumnCfg[];
    references: Record<TableKey, ColumnCfg[]>;
  } | null = null;
  private _refTableKeyToColumnKeyMap: Record<TableKey, string[]> | null = null;

  constructor(
    protected readonly _core: Core,
    protected readonly _tableKey: TableKey,
    private _refs?: ControllerRefs,
  ) {
    super(_core, _tableKey);
  }

  async init() {
    // Validate Table
    if (!!this._refs && !this._refs.base) {
      // No specific refs required for components table
      throw new Error(`Refs are not required on ComponentController.`);
    }

    // Table must be of type components
    const rljson = await this._core.dumpTable(this._tableKey);
    const table = rljson[this._tableKey];
    if (table._type !== 'components') {
      throw new Error(`Table ${this._tableKey} is not of type components.`);
    }

    this._tableCfg = await this._core.tableCfg(this._tableKey);
    this._resolvedColumns = await this._resolveReferenceColumns({
      base: this._tableCfg.columns,
    });
    this._refTableKeyToColumnKeyMap = this._createRefTableKeyToColumnKeyMap();
  }

  async insert(
    command: ControllerCommands,
    value: Json,
    origin?: Ref,
    refs?: ControllerRefs,
  ): Promise<HistoryRow<N>> {
    // Validate command
    if (!command.startsWith('add')) {
      throw new Error(
        `Command ${command} is not supported by ComponentController.`,
      );
    }
    if (!!refs) {
      throw new Error(`Refs are not supported on ComponentController.`);
    }

    //Value to add
    const component = value as JsonValue & { _hash?: string };
    const rlJson = { [this._tableKey]: { _data: [component] } } as Rljson;

    //Write component to io
    await this._core.import(rlJson);

    return {
      //Ref to component
      [this._tableKey + 'Ref']: hsh(component as Json)._hash as string,

      //Data from edit
      route: '',
      origin,
      //Unique id/timestamp
      timeId: timeId(),
    } as any as HistoryRow<N>;
  }

  // ...........................................................................
  /**
   * Retrieves references to child entries in related tables based on a condition.
   * @param where - The condition to filter the data.
   * @param filter - Optional filter to apply to the data.
   * @returns
   */
  async getChildRefs(
    where: string | Json,
    filter?: Json,
  ): Promise<Array<{ tableKey: TableKey; columnKey?: string; ref: Ref }>> {
    const { [this._tableKey]: table } = await this.get(where, filter);
    const { columns } = await this._core.tableCfg(this._tableKey);

    const childRefs: Array<{
      tableKey: TableKey;
      columnKey?: string;
      ref: Ref;
    }> = [];
    for (const colCfg of columns) {
      if (!colCfg.ref || colCfg.ref === undefined) continue;

      const propertyKey = colCfg.key;
      const childRefTableKey = colCfg.ref.tableKey;

      for (const row of table._data) {
        const refValue = (row as any)[propertyKey];
        if (typeof refValue === 'string') {
          childRefs.push({
            tableKey: childRefTableKey,
            columnKey: propertyKey,
            ref: refValue,
          });
        } else if (Array.isArray(refValue)) {
          for (const refItem of refValue) {
            if (typeof refItem === 'string') {
              childRefs.push({
                tableKey: childRefTableKey,
                columnKey: propertyKey,
                ref: refItem,
              });
            }
          }
        }
      }
    }

    return childRefs;
  }

  // ...........................................................................
  /**
   * Fetches a specific entry from the table by a partial match. Resolves references as needed.
   * @param where - An object representing a partial match.
   * @returns A promise that resolves to an array of entries matching the criteria, or null if no entries are found.
   */
  protected async _getByWhere(where: Json, filter?: Json): Promise<Rljson> {
    // If reference columns are present, resolve them
    // Check if where clause contains reference columns
    let consolidatedWhere: Json = { ...this._getWhereBase(where) };
    const hasReferenceColumns = this._hasReferenceColumns(where);
    if (hasReferenceColumns) {
      const resolvedReferences = await this._resolveReferences(
        this._getWhereReferences(where),
      );

      consolidatedWhere = {
        ...consolidatedWhere,
        ...this._referencesToWhereClause(resolvedReferences),
      };
    }

    const {
      [this._tableKey]: { _data: rows },
    } = await this._core.readRows(this._tableKey, {
      ...consolidatedWhere,
      ...filter,
    } as { [column: string]: JsonValue });

    const resultRows = rows;

    // Return result
    return {
      [this._tableKey]: {
        _data: resultRows,
        _type: 'components',
      } as ComponentsTable<T>,
    };
  }

  private _referencesToWhereClause(references: Record<TableKey, Ref[]>): Json {
    const whereClause: Json = {};
    for (const [tableKey, refs] of Object.entries(references)) {
      const wherePropertyKeys =
        this._refTableKeyToColumnKeyMap?.[tableKey as TableKey] || [];
      for (const propKey of wherePropertyKeys) {
        whereClause[propKey] = refs;
      }
    }
    return whereClause;
  }

  private _createRefTableKeyToColumnKeyMap(): Record<TableKey, string[]> {
    const map: Record<TableKey, string[]> = {};
    const columns = this._tableCfg?.columns;

    for (const colCfg of columns || []) {
      if (colCfg.ref) {
        const tableKey = colCfg.ref.tableKey;
        if (!map[tableKey]) {
          map[tableKey] = [];
        }
        map[tableKey].push(colCfg.key);
      }
    }
    return map;
  }

  // ...........................................................................
  /**
   * Extracts reference columns from the where clause.
   * @param where - The condition to filter the data.
   * @returns An object representing only the reference columns in the where clause.
   */
  private _getWhereReferences(where: Json): Json {
    const whereRefs: Json = {};
    for (const colCfg of this.referenceColumns) {
      if (colCfg.key in where) {
        whereRefs[colCfg.key] = where[colCfg.key];
      }
    }
    return whereRefs;
  }

  // ...........................................................................
  /**
   * Removes reference columns from the where clause.
   * @param where - The condition to filter the data.
   * @returns An object representing the where clause without reference columns.
   */
  private _getWhereBase(where: Json): Json {
    const whereWithoutRefs: Json = { ...where };
    for (const colCfg of this.referenceColumns) {
      if (colCfg.key in whereWithoutRefs) {
        delete whereWithoutRefs[colCfg.key];
      }
    }
    return whereWithoutRefs;
  }

  // ...........................................................................
  /**
   * Retrieves all base columns from the resolved columns.
   * @returns An array of ColumnCfg representing the base columns.
   */
  private get baseColumns(): ColumnCfg[] {
    if (!this._resolvedColumns) {
      throw new Error(
        `Resolved columns are not available for table ${this._tableKey}. You must call init() first.`,
      );
    }
    return this._resolvedColumns.base;
  }

  // ...........................................................................
  /**
   * Retrieves all reference columns from the resolved columns.
   * @returns An array of ColumnCfg representing the reference columns.
   */
  private get referenceColumns(): ColumnCfg[] {
    if (!this._resolvedColumns) {
      throw new Error(
        `Resolved columns are not available for table ${this._tableKey}. You must call init() first.`,
      );
    }
    const references: ColumnCfg[] = [];
    for (const refCols of Object.values(this._resolvedColumns.references)) {
      references.push(...refCols);
    }
    return references;
  }

  // ...........................................................................
  /**
   * Checks if the where clause contains any reference columns.
   * @param where - The condition to filter the data.
   * @returns A promise that resolves to true if reference columns are present, false otherwise.
   */
  private _hasReferenceColumns(where: Json): boolean {
    if (!this._resolvedColumns) {
      throw new Error(
        `Resolved columns are not available for table ${this._tableKey}. You must call init() first.`,
      );
    }

    for (const colCfg of this.referenceColumns) {
      if (colCfg.key in where) {
        return true;
      }
    }
    return false;
  }

  // ...........................................................................
  /**
   * Resolves reference columns in the where clause.
   * @param columns - The columns to resolve.
   * @returns A promise that resolves to an object containing base and reference columns.
   */
  private async _resolveReferenceColumns(columns: {
    base: ColumnCfg[];
    references?: Record<TableKey, ColumnCfg[]>;
  }): Promise<{
    base: ColumnCfg[];
    references: Record<TableKey, ColumnCfg[]>;
  }> {
    const base: ColumnCfg[] = [];
    const references: Record<TableKey, ColumnCfg[]> = {};

    for (const col of columns.base) {
      // If column has a ref, fetch referenced table columns
      if (!!col.ref) {
        const refTableKey = col.ref.tableKey;
        const { columns: refColumns } = await this._core.tableCfg(refTableKey);

        if (!references[refTableKey]) {
          references[refTableKey] = [];
        }

        // Check if referenced columns have refs themselves and resolve them too
        const refsHaveRefs = refColumns.some((c) => !!c.ref);
        if (refsHaveRefs) {
          const resolvedRefColumns = await this._resolveReferenceColumns({
            base: refColumns,
          });
          references[refTableKey].push(...resolvedRefColumns.base);
        }

        references[refTableKey].push(...refColumns);
      } else {
        base.push(col);
      }
    }

    return { base, references };
  }

  // ...........................................................................
  /**
   * Resolves references based on the where clause.
   * @param where - The condition to filter the data.
   * @returns - A promise that resolves to an object containing resolved references.
   */
  private async _resolveReferences(
    where: Json,
  ): Promise<Record<TableKey, Ref[]>> {
    if (!this._resolvedColumns) {
      throw new Error(
        `Resolved columns are not available for table ${this._tableKey}. You must call init() first.`,
      );
    }

    const resolvedReferences: Record<TableKey, Ref[]> = {};
    const references = this._resolvedColumns.references;
    for (const [tableKey, refColumns] of Object.entries(references)) {
      const whereForTable: Json = {};
      for (const colCfg of refColumns) {
        if (colCfg.key in where) {
          whereForTable[colCfg.key] = where[colCfg.key];
        }
      }

      if (Object.keys(whereForTable).length === 0) {
        continue; // No where clause for this table
      }

      const refRows = await this._readRowsWithReferences(
        tableKey as TableKey,
        whereForTable as { [column: string]: JsonValue },
      );

      const refs: Ref[] = refRows[tableKey as TableKey]._data.map(
        (r) => (r as any)._hash,
      );

      resolvedReferences[tableKey as TableKey] = refs;
    }

    return resolvedReferences;
  }

  private async _readRowsWithReferences(
    table: string,
    where: { [column: string]: JsonValue },
  ): Promise<Rljson> {
    //Split where clauses with array values into multiple queries
    const splitted: { [column: string]: JsonValue }[] = [];
    for (const [key, value] of Object.entries(where)) {
      if (Array.isArray(value)) {
        for (const v of value) {
          splitted.push({ ...where, ...{ [key]: v as JsonValue } });
        }
      }
    }

    //If we have multiple where clauses, merge the results
    if (splitted.length > 0) {
      const results = [];
      for (const s of splitted) {
        results.push(await this._core.readRows(table, s));
      }
      return merge(...results) as Rljson;
    } else {
      return this._core.readRows(table, where);
    }
  }

  filterRow(row: Json, key: string, value: JsonValue): boolean {
    for (const [propertyKey, propertyValue] of Object.entries(row)) {
      if (propertyKey === key && equals(propertyValue, value)) {
        return true;
      }
    }
    return false;
  }
}
