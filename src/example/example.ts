// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { IoMem } from '@rljson/io';
import { JsonH, JsonValueH } from '@rljson/json';
import { ContentType, Insert, SliceId } from '@rljson/rljson';

import * as fs from 'fs';
import path from 'path';

import { Db } from '../db.ts';

import { exampleTableCfgs } from './example-table-cfgs.ts';

export interface CarGeneral extends JsonH {
  brand: string;
  type: string;
  doors: number;
  energyConsumption: number;
  units: JsonValueH;
  serviceIntervals: number[];
  isElectric: boolean;
}

export interface CarDimension extends JsonH {
  height: number;
  width: number;
  length: number;
}

export interface CarTechnical extends JsonH {
  engine: string;
  transmission: string;
  gears: number;
  carDimensionsRef: string;
}

export class example {
  db?: Db;

  private _meta: Map<
    string,
    { type: ContentType; file: string; route: string }
  > = new Map([
    [
      'carSliceId',
      {
        type: 'slices',
        file: 'car-slice-id.csv',
        route: 'carSliceId',
      },
    ],
    [
      'carGeneral',
      {
        type: 'components',
        file: 'car-general.csv',
        route: 'carCake/carGeneralLayer/carGeneral',
      },
    ],
  ]);

  constructor() {}

  async init() {
    //Init io
    const io = new IoMem();
    await io.init();
    await io.isReady();

    //Init Core
    this.db = new Db(io);

    //Create Tables for TableCfgs in carsExample
    for (const tableCfg of exampleTableCfgs().values()) {
      await this.db.core.createTableWithInsertHistory(tableCfg);
    }

    //Bootstrap Data
    await this.bootstrapData();
  }

  async bootstrapData() {
    if (!this.db) {
      throw new Error('DB is not initialized.');
    }

    for (const [tableKey, meta] of this._meta.entries()) {
      const tableCfg = exampleTableCfgs().get(tableKey);
      if (!tableCfg) {
        throw new Error(`TableCfg for tableKey ${tableKey} not found.`);
      }

      const csvPath = path.resolve(__dirname, `data/${meta.file}`);
      const csv = (await fs.readFileSync(csvPath, 'utf-8'))
        .split('\n')
        .map((line) => line.trim())
        .filter((line, idx) => idx > 0 && line.length > 0)
        .map((line) => line.split(';').map((cell) => cell.trim()));

      const processedData = this._createInsertData(tableKey, csv);
      const insert: Insert<any> = {
        command: 'add',
        value: processedData,
        route: meta.route,
      };

      const inserted = await this.db.insert(insert);
      debugger;
    }
  }

  private _createInsertData(tableKey: string, data: string[][]) {
    if (tableKey === 'carSliceId') {
      const sliceIds: SliceId[] = data.map((row) => row[0]);
      const insertData = sliceIds;
      return insertData;
    }
    if (tableKey === 'carGeneral') {
      const indexedComponents: Record<SliceId, CarGeneral> = {};
      for (const row of data) {
        const carGeneral: CarGeneral = {
          brand: row[1],
          type: row[2],
          doors: parseInt(row[3]),
          energyConsumption: parseFloat(row[4]),
          units: { energy: row[5], _hash: '' },
          serviceIntervals: row[6].split(',').map((s) => parseInt(s.trim())),
          isElectric: row[7].toLowerCase() === 'true',
          _hash: '',
        };
        indexedComponents[row[0]] = carGeneral;
      }
      const insertData = { carGeneralLayer: indexedComponents };
      return insertData;
    }
  }
}
