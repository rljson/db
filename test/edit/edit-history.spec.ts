// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip, hsh, rmhsh } from '@rljson/hash';
import { IoMem } from '@rljson/io';
import { Json } from '@rljson/json';
import {
  Cake,
  createCakeTableCfg,
  createEditHistoryTableCfg,
  createEditTableCfg,
  createLayerTableCfg,
  createMultiEditTableCfg,
  createSliceIdsTableCfg,
  Edit,
  EditAction,
  Layer,
  Route,
  SliceIds,
  TableCfg,
} from '@rljson/rljson';

import { beforeEach, describe, expect, it } from 'vitest';

import { Db } from '../../src/db';
import {
  exampleEditActionColumnSelection,
  exampleEditActionSetValue,
} from '../../src/edit/edit-action';
import { MultiEditManager } from '../../src/edit/multi-edit-manager';
import { staticExample } from '../../src/example-static/example-static';

const toEdit = (action: EditAction): Edit =>
  hip<Edit>({ name: action.name, action, _hash: '' });

// Edits and returns the new head
const editAndGetHead = async (
  manager: MultiEditManager,
  action: EditAction,
  cakeRef?: string,
): Promise<string> => {
  await manager.edit(toEdit(action), cakeRef);
  return manager.head!.editHistoryRef;
};

// Walks to an edit history state and returns the rows of its view
const rowsAt = async (manager: MultiEditManager, ref: string) =>
  (await manager.editHistoryRef(ref)).join.rows;

describe('Edit history', () => {
  describe('with the static example', () => {
    const cakeKey = 'carCake';
    const cakeRef = staticExample().carCake._data[2]._hash as string;
    let db: Db;
    let manager: MultiEditManager;

    const setServiceIntervals = (value: number[]): EditAction =>
      ({
        ...exampleEditActionSetValue(),
        data: { ...exampleEditActionSetValue().data, value },
      }) as EditAction;

    // Column 2 is serviceIntervals
    const serviceIntervals = (rows: any[][]) => rows.map((r) => r[2]);
    const all = (rows: any[][], value: any) =>
      serviceIntervals(rows).every(
        (v) => JSON.stringify(v) === JSON.stringify(value),
      );

    beforeEach(async () => {
      const io = new IoMem();
      await io.init();
      await io.isReady();
      db = new Db(io);
      for (const tableCfg of staticExample().tableCfgs._data) {
        await db.core.createTableWithInsertHistory(tableCfg);
      }
      await db.core.createTable(createMultiEditTableCfg(cakeKey));
      await db.core.createTable(createEditTableCfg(cakeKey));
      await db.core.createTable(createEditHistoryTableCfg(cakeKey));
      await db.core.import(staticExample());
      manager = new MultiEditManager(cakeKey, db);
    });

    it('back, forward, back again and editing after going back', async () => {
      const select = await editAndGetHead(
        manager,
        exampleEditActionColumnSelection() as EditAction,
        cakeRef,
      );
      const original = serviceIntervals(await rowsAt(manager, select));

      // Edit forward
      await manager.editHistoryRef(select);
      const setA = await editAndGetHead(manager, setServiceIntervals([1]));
      const setB = await editAndGetHead(manager, setServiceIntervals([2]));

      // Back
      expect(serviceIntervals(await rowsAt(manager, select))).toEqual(
        original,
      );
      expect(all(await rowsAt(manager, setA), [1])).toBe(true);

      // Forward
      const rowsB = serviceIntervals(await rowsAt(manager, setB));
      expect(rowsB.every((v) => v.at(-1) === 2)).toBe(true);

      // Back again
      expect(all(await rowsAt(manager, setA), [1])).toBe(true);
      expect(serviceIntervals(await rowsAt(manager, select))).toEqual(
        original,
      );

      // Edit after going back
      await manager.editHistoryRef(select);
      const setC = await editAndGetHead(manager, setServiceIntervals([3]));
      expect(all(await rowsAt(manager, setC), [3])).toBe(true);
      expect(serviceIntervals(await rowsAt(manager, select))).toEqual(
        original,
      );
      expect(all(await rowsAt(manager, setA), [1])).toBe(true);
      expect(serviceIntervals(await rowsAt(manager, setB))).toEqual(rowsB);

      // Nothing is published for the selection only
      const selectProc = await manager.editHistoryRef(select);
      expect(selectProc.join.insert()).toEqual([]);
    });

    it('a fresh manager rebuilds each state independently', async () => {
      const select = await editAndGetHead(
        manager,
        exampleEditActionColumnSelection() as EditAction,
        cakeRef,
      );
      const original = serviceIntervals(await rowsAt(manager, select));
      const setA = await editAndGetHead(manager, setServiceIntervals([1]));
      const setB = await editAndGetHead(manager, setServiceIntervals([2]));

      const fresh = new MultiEditManager(cakeKey, db);
      const rowsB = serviceIntervals(await rowsAt(fresh, setB));
      expect(rowsB.every((v) => v.at(-1) === 2)).toBe(true);
      expect(all(await rowsAt(fresh, setA), [1])).toBe(true);
      expect(serviceIntervals(await rowsAt(fresh, select))).toEqual(original);
      expect(serviceIntervals(await rowsAt(fresh, setB))).toEqual(rowsB);
    });
  });

  describe('with a cake and layers without name suffixes', () => {
    let db: Db;
    let catalogRef: string;
    let manager: MultiEditManager;

    const componentsCfg = (key: string): TableCfg =>
      hip<TableCfg>({
        key,
        type: 'components',
        columns: [
          { key: '_hash', type: 'string', titleLong: 'Hash', titleShort: 'H' },
          {
            key: 'value',
            type: 'jsonValue',
            titleLong: 'Value',
            titleShort: 'V',
          },
        ],
        isHead: false,
        isRoot: false,
        isShared: true,
      } as TableCfg);

    const sliceIds = hsh<SliceIds>({ add: ['a', 'b', 'c'] } as SliceIds);
    const prices = ['1', '2', '3'].map((v) => hsh<Json>({ value: v }));

    const priceLayer = hsh<Layer>({
      add: { a: prices[0]._hash, b: prices[1]._hash, c: prices[2]._hash },
      sliceIdsTable: 'items',
      sliceIdsTableRow: sliceIds._hash,
      componentsTable: 'prices',
    } as Layer);

    const catalog = hsh<Cake>({
      sliceIdsTable: 'items',
      sliceIdsRow: sliceIds._hash,
      layers: { carPrices: priceLayer._hash },
    } as Cake);

    const route = 'catalogs/carPrices/prices/value';

    const select = {
      name: 'Select',
      type: 'selection',
      data: {
        columns: [
          {
            key: 'price',
            route,
            alias: 'price',
            titleShort: 'Price',
            titleLong: 'Price',
            type: 'string',
          },
        ],
      },
      _hash: '',
    } as any as EditAction;

    const setPrice = (value: string): EditAction =>
      ({
        name: 'Set',
        type: 'setValue',
        data: { route, value },
        _hash: '',
      }) as any;

    const sortPrices = (order: 'asc' | 'desc'): EditAction =>
      ({
        name: 'Sort',
        type: 'sort',
        data: { [route]: order },
        _hash: '',
      }) as any;

    const filterPrice = (value: string): EditAction =>
      ({
        name: 'Filter',
        type: 'filter',
        data: {
          columnFilters: [
            {
              type: 'string',
              column: route,
              operator: 'equals',
              search: value,
              _hash: '',
            },
          ],
          operator: 'and',
          _hash: '',
        },
        _hash: '',
      }) as any;

    const prices_ = (rows: any[][]) => rows.map((r) => r[0][0]);

    beforeEach(async () => {
      const io = new IoMem();
      await io.init();
      await io.isReady();
      db = new Db(io);

      for (const cfg of [
        hip<TableCfg>(createSliceIdsTableCfg('items')),
        componentsCfg('prices'),
        hip<TableCfg>(createLayerTableCfg('carPrices')),
        hip<TableCfg>(createCakeTableCfg('catalogs')),
      ]) {
        await db.core.createTableWithInsertHistory(cfg);
      }
      await db.core.createTable(createMultiEditTableCfg('catalogs'));
      await db.core.createTable(createEditTableCfg('catalogs'));
      await db.core.createTable(createEditHistoryTableCfg('catalogs'));

      await db.core.import({
        items: { _type: 'sliceIds', _data: [sliceIds] },
        prices: { _type: 'components', _data: prices },
        carPrices: { _type: 'layers', _data: [priceLayer] },
      } as any);

      const [inserted] = await db.insert(Route.fromFlat('catalogs'), {
        catalogs: { _type: 'cakes', _data: [rmhsh(catalog)] },
      });
      catalogRef = (inserted as any).catalogsRef;
      manager = new MultiEditManager('catalogs', db);
    });

    it('keeps sort, filter and set value states apart', async () => {
      const s = await editAndGetHead(manager, select, catalogRef);
      const desc = await editAndGetHead(manager, sortPrices('desc'));
      const filtered = await editAndGetHead(manager, filterPrice('2'));
      const set = await editAndGetHead(manager, setPrice('9'));

      // Back
      expect(prices_(await rowsAt(manager, s))).toEqual(['1', '2', '3']);
      expect(prices_(await rowsAt(manager, desc))).toEqual(['3', '2', '1']);
      expect(prices_(await rowsAt(manager, filtered))).toEqual(['2']);

      // Forward and back again
      expect(prices_(await rowsAt(manager, set))).toEqual(['9']);
      expect(prices_(await rowsAt(manager, filtered))).toEqual(['2']);
      expect(prices_(await rowsAt(manager, s))).toEqual(['1', '2', '3']);

      // Edit after going back
      const asc = await editAndGetHead(manager, sortPrices('asc'));
      expect(prices_(await rowsAt(manager, asc))).toEqual(['1', '2', '3']);
      expect(prices_(await rowsAt(manager, desc))).toEqual(['3', '2', '1']);
      expect(prices_(await rowsAt(manager, set))).toEqual(['9']);
      await manager.editHistoryRef(s);
      const set5 = await editAndGetHead(manager, setPrice('5'));
      expect(prices_(await rowsAt(manager, set5))).toEqual(['5', '5', '5']);
      expect(prices_(await rowsAt(manager, set))).toEqual(['9']);
      expect(prices_(await rowsAt(manager, s))).toEqual(['1', '2', '3']);

      // Publishing an old state publishes only its own edits
      await manager.editHistoryRef(set);
      const { cakeRef } = await manager.publish();
      const { cell } = await db.get(
        Route.fromFlat(`catalogs@${cakeRef}/carPrices/prices/value`),
        {},
      );
      expect(cell.map((c) => c.value).sort()).toEqual(['1', '3', '9']);
    });
  });
});
