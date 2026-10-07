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
  Layer,
  Route,
  SliceIds,
  TableCfg,
} from '@rljson/rljson';

import { beforeEach, describe, expect, it } from 'vitest';

import { Db } from '../src/db';
import {
  exampleEditActionColumnSelection,
  exampleEditActionRowFilter,
  exampleEditActionSetValue,
} from '../src/edit/edit-action';
import { MultiEditManager } from '../src/edit/multi-edit-manager';
import { staticExample } from '../src/example-static/example-static';

// Reads a cake row
const readCake = async (db: Db, table: string, ref: string) => {
  const { [table]: cakes } = await db.core.readRow(table, ref);
  return cakes._data[0] as Cake;
};

// Reads all components of a layer of a cake, keyed by sliceId
const readItems = async (
  db: Db,
  cakeKey: string,
  cakeRef: string,
  layerKey: string,
  componentsKey: string,
): Promise<Record<string, Json>> => {
  const { cell } = await db.get(
    Route.fromFlat(`${cakeKey}@${cakeRef}/${layerKey}/${componentsKey}`),
    {},
  );
  const items: Record<string, Json> = {};
  for (const c of cell) {
    const path = c.path[0];
    const sliceId = path[path.indexOf('add') + 1] as string;
    items[sliceId] = rmhsh(c.row as Json);
  }
  return items;
};

describe('Complete catalogs', () => {
  describe('with the static example', () => {
    const cakeKey = 'carCake';
    const cakeRef = staticExample().carCake._data[2]._hash as string;
    let db: Db;

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
    });

    it('publish keeps untouched layers and items', async () => {
      const before = await readCake(db, cakeKey, cakeRef);
      const generalBefore = await readItems(
        db,
        cakeKey,
        cakeRef,
        'carGeneralLayer',
        'carGeneral',
      );
      const technicalBefore = await readItems(
        db,
        cakeKey,
        cakeRef,
        'carTechnicalLayer',
        'carTechnical',
      );

      // Edit the service intervals of the filtered cars only
      const manager = new MultiEditManager(cakeKey, db);
      await manager.init();
      await manager.edit(
        hip<Edit>({
          name: 'Select',
          action: exampleEditActionColumnSelection(),
          _hash: '',
        }),
        cakeRef,
      );
      await manager.edit(
        hip<Edit>({
          name: 'Filter',
          action: exampleEditActionRowFilter(),
          _hash: '',
        }),
      );
      await manager.edit(
        hip<Edit>({
          name: 'Set',
          action: exampleEditActionSetValue(),
          _hash: '',
        }),
      );
      const { cakeRef: newRef } = await manager.publish();

      // All layers and the slice ids are kept
      const after = await readCake(db, cakeKey, newRef);
      expect(Object.keys(rmhsh(after.layers)).sort()).toEqual(
        Object.keys(rmhsh(before.layers)).sort(),
      );
      expect(after.layers.carTechnicalLayer).toBe(
        before.layers.carTechnicalLayer,
      );
      expect(after.sliceIdsTable).toBe(before.sliceIdsTable);
      expect(after.sliceIdsRow).toBe(before.sliceIdsRow);

      // Untouched layer is unchanged
      const technicalAfter = await readItems(
        db,
        cakeKey,
        newRef,
        'carTechnicalLayer',
        'carTechnical',
      );
      expect(technicalAfter).toEqual(technicalBefore);

      // All items of the edited layer are kept, only filtered ones changed
      const generalAfter = await readItems(
        db,
        cakeKey,
        newRef,
        'carGeneralLayer',
        'carGeneral',
      );
      expect(Object.keys(generalAfter).sort()).toEqual(
        Object.keys(generalBefore).sort(),
      );
      const newIntervals = [15000, 30000, 45000, 60000];
      const changed = Object.keys(generalAfter).filter(
        (id) =>
          JSON.stringify(generalAfter[id].serviceIntervals) ===
          JSON.stringify(newIntervals),
      );
      expect(changed.length).toBeGreaterThan(0);
      expect(changed.length).toBeLessThan(Object.keys(generalBefore).length);
      for (const id of Object.keys(generalBefore)) {
        if (changed.includes(id)) {
          expect({
            ...generalAfter[id],
            serviceIntervals: generalBefore[id].serviceIntervals,
          }).toEqual(generalBefore[id]);
        } else {
          expect(generalAfter[id]).toEqual(generalBefore[id]);
        }
      }
    });

    it('insert on a route with a ref keeps untouched layers and items', async () => {
      const generalBefore = await readItems(
        db,
        cakeKey,
        cakeRef,
        'carGeneralLayer',
        'carGeneral',
      );
      const vin1 = { ...generalBefore['VIN1'], brand: 'Opel' };

      const [inserted] = await db.insert(
        Route.fromFlat(`${cakeKey}@${cakeRef}/carGeneralLayer/carGeneral`),
        {
          [cakeKey]: {
            _type: 'cakes',
            _data: [
              {
                layers: {
                  carGeneralLayer: {
                    _type: 'layers',
                    _data: [
                      {
                        add: {
                          VIN1: {
                            carGeneral: { _type: 'components', _data: [vin1] },
                          },
                        },
                      },
                    ],
                  },
                },
              },
            ],
          },
        },
      );
      const newRef = (inserted as any)[cakeKey + 'Ref'] as string;

      const before = await readCake(db, cakeKey, cakeRef);
      const after = await readCake(db, cakeKey, newRef);
      expect(after.sliceIdsTable).toBe(before.sliceIdsTable);
      expect(after.sliceIdsRow).toBe(before.sliceIdsRow);
      expect(after.layers.carColorLayer).toBe(before.layers.carColorLayer);

      const { carGeneralLayer: layers } = await db.core.readRow(
        'carGeneralLayer',
        after.layers.carGeneralLayer,
      );
      const layer = layers._data[0] as Layer;
      expect(layer.base).toBe(before.layers.carGeneralLayer);
      expect(layer.componentsTable).toBe('carGeneral');
      expect(layer.sliceIdsTable).toBe('carSliceId');

      const generalAfter = await readItems(
        db,
        cakeKey,
        newRef,
        'carGeneralLayer',
        'carGeneral',
      );
      expect(generalAfter).toEqual({ ...generalBefore, VIN1: vin1 });
    });
  });

  describe('with a cake and layers without name suffixes', () => {
    let db: Db;
    let catalogRef: string;

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
    const names = ['A', 'B', 'C'].map((v) => hsh<Json>({ value: v }));

    const layer = (componentsTable: string, comps: Json[]) =>
      hsh<Layer>({
        add: { a: comps[0]._hash, b: comps[1]._hash, c: comps[2]._hash },
        sliceIdsTable: 'items',
        sliceIdsTableRow: sliceIds._hash,
        componentsTable,
      } as Layer);

    const priceLayer = layer('prices', prices);
    const nameLayer = layer('names', names);
    const catalog = hsh<Cake>({
      sliceIdsTable: 'items',
      sliceIdsRow: sliceIds._hash,
      layers: { carPrices: priceLayer._hash, carNames: nameLayer._hash },
    } as Cake);

    beforeEach(async () => {
      const io = new IoMem();
      await io.init();
      await io.isReady();
      db = new Db(io);

      for (const cfg of [
        hip<TableCfg>(createSliceIdsTableCfg('items')),
        componentsCfg('prices'),
        componentsCfg('names'),
        hip<TableCfg>(createLayerTableCfg('carPrices')),
        hip<TableCfg>(createLayerTableCfg('carNames')),
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
        names: { _type: 'components', _data: names },
        carPrices: { _type: 'layers', _data: [priceLayer] },
        carNames: { _type: 'layers', _data: [nameLayer] },
      } as any);

      const [inserted] = await db.insert(Route.fromFlat('catalogs'), {
        catalogs: { _type: 'cakes', _data: [rmhsh(catalog)] },
      });
      catalogRef = (inserted as any).catalogsRef;
      expect(catalogRef).toBe(catalog._hash);
    });

    const priceTree = (value: string) => ({
      catalogs: {
        _type: 'cakes',
        _data: [
          {
            layers: {
              carPrices: {
                _type: 'layers',
                _data: [
                  {
                    add: {
                      b: { prices: { _type: 'components', _data: [{ value }] } },
                    },
                  },
                ],
              },
            },
          },
        ],
      },
    });

    const expectComplete = async (ref: string, price: string) => {
      expect(
        await readItems(db, 'catalogs', ref, 'carPrices', 'prices'),
      ).toEqual({ a: { value: '1' }, b: { value: price }, c: { value: '3' } });
      expect(
        await readItems(db, 'catalogs', ref, 'carNames', 'names'),
      ).toEqual({ a: { value: 'A' }, b: { value: 'B' }, c: { value: 'C' } });
      const cake = await readCake(db, 'catalogs', ref);
      expect(cake.sliceIdsTable).toBe('items');
      expect(cake.sliceIdsRow).toBe(sliceIds._hash);
    };

    it('insert with a ref keeps untouched layers and items', async () => {
      const [inserted] = await db.insert(
        Route.fromFlat(`catalogs@${catalogRef}/carPrices/prices`),
        priceTree('20'),
      );
      await expectComplete((inserted as any).catalogsRef, '20');
    });

    it('insert with a timeId keeps untouched layers and items', async () => {
      const [first] = await db.insert(
        Route.fromFlat(`catalogs@${catalogRef}/carPrices/prices`),
        priceTree('20'),
      );
      const [second] = await db.insert(
        Route.fromFlat(`catalogs@${first.timeId}/carPrices/prices`),
        priceTree('30'),
      );
      await expectComplete((second as any).catalogsRef, '30');
    });

    it('insert without a known previous cake writes the given layers', async () => {
      const tree = priceTree('20') as any;
      tree.catalogs._data[0]._hash = 'unknownCakeHash1234567';
      const [inserted] = await db.insert(
        Route.fromFlat(`catalogs@0000000000000:abcd/carPrices/prices`),
        tree,
      );
      const cake = await readCake(db, 'catalogs', (inserted as any).catalogsRef);
      expect(Object.keys(rmhsh(cake.layers))).toEqual(['carPrices']);
    });

    it('publish keeps untouched layers and items', async () => {
      const manager = new MultiEditManager('catalogs', db);
      await manager.init();
      await manager.edit(
        hip<Edit>({
          name: 'Select',
          action: {
            name: 'Select',
            type: 'selection',
            data: {
              columns: [
                {
                  key: 'price',
                  route: 'catalogs/carPrices/prices/value',
                  alias: 'price',
                  titleShort: 'Price',
                  titleLong: 'Price',
                  type: 'string',
                },
              ],
            },
            _hash: '',
          } as any,
          _hash: '',
        }),
        catalogRef,
      );
      await manager.edit(
        hip<Edit>({
          name: 'Set',
          action: {
            name: 'Set',
            type: 'setValue',
            data: { route: 'catalogs/carPrices/prices/value', value: '9' },
            _hash: '',
          } as any,
          _hash: '',
        }),
      );
      const { cakeRef } = await manager.publish();

      // All items get the same price, the new layer holds all of them
      const cake = await readCake(db, 'catalogs', cakeRef);
      const { carPrices: priceLayers } = await db.core.readRow(
        'carPrices',
        cake.layers.carPrices,
      );
      const newPriceLayer = priceLayers._data[0] as Layer;
      expect(newPriceLayer.base).toBe(priceLayer._hash);
      expect(Object.keys(rmhsh(newPriceLayer.add)).sort()).toEqual([
        'a',
        'b',
        'c',
      ]);
      expect(
        Object.values(
          await readItems(db, 'catalogs', cakeRef, 'carPrices', 'prices'),
        ),
      ).toEqual([{ value: '9' }]);

      // The untouched layer is kept
      expect(cake.layers.carNames).toBe(nameLayer._hash);
      expect(
        await readItems(db, 'catalogs', cakeRef, 'carNames', 'names'),
      ).toEqual({ a: { value: 'A' }, b: { value: 'B' }, c: { value: 'C' } });
    });
  });
});
