// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hsh } from '@rljson/hash';
import { Json, JsonValue, JsonValueType, merge } from '@rljson/json';
import { Insert, InsertHistoryRow, TableCfg } from '@rljson/rljson';

import * as fs from 'fs';
import path from 'path';

import { Db } from '../db.ts';

import { exampleTableCfgs } from './example-data-table-cfgs.ts';

type DataMap = Record<
  string,
  Record<
    string,
    | {
        [key: string]: {
          cols?: number[];
          through?: string;
          type?:
            | 'number'
            | 'string'
            | 'boolean'
            | 'json'
            | 'jsonArray'
            | 'jsonValue';
          fn?: (v: any) => string;
        };
      }
    | {
        _route: string;
        _skipLayerCreation?: boolean;
        layers?: Record<string, string>;
      }
  >
>;

export class ExampleGenerator {
  private _dataMap: DataMap = {
    car: {
      carSliceId: {
        _route: '/carSliceId',
        sliceId: {
          cols: [0],
        },
      },
      carGeneral: {
        _route: '/carGeneralLayer/carGeneral',
        sliceId: {
          cols: [0],
        },
        brand: {
          cols: [1],
        },
        type: {
          cols: [2],
        },
        doors: {
          cols: [3],
        },
        energyConsumption: {
          cols: [4],
        },
        units: {
          cols: [5],
          fn: (v: any) => {
            return JSON.stringify({ energy: v, _hash: '' });
          },
        },
        serviceIntervals: {
          cols: [6],
          fn: (v: any) => {
            return JSON.stringify(v.split(',').map((s: string) => s.trim()));
          },
        },
        isElectric: {
          cols: [7],
        },
        meta: {
          cols: [8],
          fn: (v: any) => {
            return JSON.stringify({ pressTexts: [v], _hash: '' });
          },
        },
      },
      carDimensions: {
        _route: '/carDimensions',
        _skipLayerCreation: true,
        length: {
          cols: [9],
        },
        width: {
          cols: [10],
        },
        height: {
          cols: [11],
        },
      },
      carTechnical: {
        _route: '/carTechnicalLayer/carTechnical',
        sliceId: {
          cols: [0],
        },
        engine: {
          cols: [12],
        },
        transmission: {
          cols: [13],
        },
        gears: {
          cols: [14],
        },
        repairedByWorkshop: {
          cols: [15],
        },
        dimensions: {
          through: 'carDimensions',
        },
      },
      carColor: {
        _route: '/carColorLayer/carColor',
        sliceId: {
          cols: [0],
        },
        sides: {
          cols: [16],
        },
        roof: {
          cols: [17],
        },
        highlights: {
          cols: [18],
        },
      },
      carCake: {
        _route: '/carCake',
        layers: {
          carGeneralLayer: '/carGeneralLayer/carGeneral',
          carTechnicalLayer: '/carTechnicalLayer/carTechnical',
          carColorLayer: '/carColorLayer/carColor',
        },
      },
    },
  };

  // Cache for parsed CSV data and column configurations
  private _csvCache = new Map<string, string[][]>();
  private _columnCfgCache = new Map<string, Map<string, any>>();

  constructor(private _db: Db) {}

  async init() {
    // Create Tables for TableCfgs in carsExample
    const tableCfgs = exampleTableCfgs();

    // Parallel table creation
    await Promise.all(
      Array.from(tableCfgs.values()).map((tableCfg) =>
        this._db.core.createTableWithInsertHistory(tableCfg),
      ),
    );

    const inserts = await this.inserts(tableCfgs);
    const results = new Map<string, InsertHistoryRow<any>[]>();

    const insertPromises = Array.from(inserts.values())
      .flat()
      .map(async (insert) => {
        const insertHistoryRows = await this._db.insert(insert);

        results.set(insert.route as string, insertHistoryRows);

        return { route: insert.route, rows: insertHistoryRows };
      });

    await Promise.all(insertPromises);

    const mergedDataMap = merge(...Object.values(this._dataMap)) as any;
    const cakeInserts: Insert<any>[] = [];

    for (const tableCfg of tableCfgs.values()) {
      if (tableCfg.type !== 'cakes') continue;

      const cakeMap = mergedDataMap[tableCfg.key];
      const cakeLayers = cakeMap?.layers as Record<string, string> | undefined;
      if (!cakeLayers) continue;

      const insertValue: Record<string, string> = {};

      for (const [layerKey, insertRoute] of Object.entries(cakeLayers)) {
        const layerInserts = results.get(insertRoute);
        if (!layerInserts || layerInserts.length === 0) continue;

        if (layerInserts.length > 1) {
          throw new Error(
            `Expected only one insert for cake layer ${layerKey} at route ${insertRoute}, but got ${layerInserts.length}.`,
          );
        }

        const layerHash = (layerInserts[0] as any)[layerKey + 'Ref'];
        insertValue[layerKey] = layerHash;
      }

      cakeInserts.push({
        command: 'add',
        route: `/${tableCfg.key}`,
        value: insertValue,
      });
    }

    // Insert all cakes in parallel
    if (cakeInserts.length > 0) {
      await Promise.all(cakeInserts.map((ci) => this._db.insert(ci)));
    }
  }

  async inserts(
    tableCfgs: Map<string, TableCfg>,
  ): Promise<Map<string, Insert<any>[]>> {
    if (!this._db) {
      throw new Error('DB is not initialized.');
    }

    // Pre-cache column configurations
    this._preloadColumnConfigs(tableCfgs);

    const inserts: Map<string, Insert<any>[]> = new Map();

    // Process in parallel by type
    const typePromises = Object.entries(this._dataMap).map(
      async ([typeKey, typeMap]) => {
        const data = this._readCsv(
          path.join(__dirname, `./data/${typeKey}.csv`),
        );
        const typeInserts = new Map<string, Insert<any>[]>();

        // Process tables in parallel
        const tablePromises = Object.entries(typeMap).map(
          async ([tableKey, tableMap]) => {
            const tableCfg = tableCfgs.get(tableKey);
            if (!tableCfg) {
              throw new Error(`TableCfg for tableKey ${tableKey} not found.`);
            }
            const type = tableCfg.type;

            if (type === 'cakes') return;

            const route = tableMap._route;
            const skipLayerCreation = tableMap._skipLayerCreation;

            // Optimized row processing with pre-allocated arrays
            const rowValues: Json[] = new Array(data.length);
            const columnConfigs = this._columnCfgCache.get(tableKey);

            for (let i = 0; i < data.length; i++) {
              const row = data[i];
              const rowValue = {} as Json;

              // Process columns more efficiently
              for (const [colKey, colMap] of Object.entries(tableMap)) {
                if (
                  colKey === '_type' ||
                  colKey === '_route' ||
                  colKey === '_skipLayerCreation'
                )
                  continue;

                let colValue: JsonValue;

                if (colMap.fn) {
                  // Handle fn transformation
                  colValue = colMap.fn(row[colMap.cols ? colMap.cols[0] : 0]);
                } else if (colMap.cols) {
                  // Handle direct cols
                  colValue = row[colMap.cols[0]];
                } else if (colMap.through) {
                  const throughTableKey = colMap.through;
                  const throughValues =
                    typeInserts.get(throughTableKey)?.[0]?.value;
                  const throughValue = throughValues
                    ? (throughValues as Json[])[i]
                    : null;
                  colValue = (
                    throughValue ? hsh(throughValue) : null
                  ) as JsonValue;
                } else {
                  continue;
                }

                // Use cached column config
                const colCfg = columnConfigs?.get(colKey);
                const type =
                  colKey === 'sliceId' ? 'string' : colCfg?.type || 'string';

                rowValue[colKey] = this._parseValue(colValue as string, type);
              }

              rowValues[i] = rowValue;
            }

            // Create inserts based on type
            let tableInserts: Insert<any>[];

            if (type === 'sliceIds') {
              tableInserts = [
                {
                  command: 'add',
                  value: rowValues.map((rv) => rv.sliceId),
                  route: route as string,
                },
              ];
            } else if (skipLayerCreation === true && type === 'components') {
              tableInserts = rowValues.map((rv) => {
                const cleanRv = { ...rv };
                delete (cleanRv as any).sliceId;
                return {
                  command: 'add',
                  route: route as string,
                  value: cleanRv,
                };
              });
            } else if (!skipLayerCreation) {
              // Optimize object creation
              const valueObject = Object.create(null);
              for (const rv of rowValues) {
                const sliceId = (rv as any).sliceId as string;
                const rvCopy = { ...rv };
                delete (rvCopy as any).sliceId;
                valueObject[sliceId] = rvCopy;
              }

              tableInserts = [
                {
                  command: 'add',
                  value: valueObject,
                  route: route as string,
                },
              ];
            } else {
              return;
            }

            typeInserts.set(tableKey, tableInserts);
          },
        );

        await Promise.all(tablePromises);
        return typeInserts;
      },
    );

    // Merge results
    const allTypeInserts = await Promise.all(typePromises);
    for (const typeInserts of allTypeInserts) {
      for (const [key, value] of typeInserts) {
        inserts.set(key, value);
      }
    }

    return inserts;
  }

  private _preloadColumnConfigs(tableCfgs: Map<string, TableCfg>) {
    for (const [tableKey, tableCfg] of tableCfgs) {
      const columnMap = new Map();
      for (const colCfg of tableCfg.columns) {
        columnMap.set(colCfg.key, colCfg);
      }
      this._columnCfgCache.set(tableKey, columnMap);
    }
  }

  private _readCsv(filePath: string): string[][] {
    // Use cache to avoid re-reading files
    if (this._csvCache.has(filePath)) {
      return this._csvCache.get(filePath)!;
    }

    const csv = fs
      .readFileSync(filePath, 'utf-8')
      .split('\n')
      .map((line) => line.trim())
      .filter((line, idx) => idx > 0 && line.length > 0)
      .map((line) => line.split(';').map((cell) => cell.trim()));

    this._csvCache.set(filePath, csv);
    return csv;
  }

  // Optimized parsing with pre-compiled regex and type checking
  private _parseValue(value: string, type: JsonValueType): JsonValue {
    switch (type) {
      case 'number':
        const num = Number(value);
        if (isNaN(num)) {
          throw new Error(`Value "${value}" is not a valid number.`);
        }
        return num;
      case 'boolean':
        return value === 'true'; // Faster than toLowerCase()
      case 'json':
      case 'jsonArray':
      case 'jsonValue':
        try {
          return JSON.parse(value);
        } catch (e: any) {
          throw new Error(`Invalid JSON value: "${value}". ${e.message}`);
        }
      case 'string':
      default:
        return value;
    }
  }
}
