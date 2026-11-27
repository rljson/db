// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { IoMem } from '@rljson/io';
import { Json } from '@rljson/json';
import { Route } from '@rljson/rljson';

import { beforeEach, describe, expect, it, vi } from 'vitest';

import {
  Db,
  DbEvents,
  DbHookRefusedError,
  DbInsertEvent,
  MockHook,
} from '../src/index.ts';
import {
  CarGeneral,
  staticExample,
} from '../src/example-static/example-static.ts';
import { inject } from '../src/tools/inject.ts';
import { isolate } from '../src/tools/isolate.ts';

describe('Db with events', () => {
  let io: IoMem;
  let db: Db;
  const route = Route.fromFlat('/carGeneral');

  const createDb = async (db: Db) => {
    for (const tableCfg of staticExample().tableCfgs._data) {
      await db.core.createTableWithInsertHistory(tableCfg);
    }
    await db.core.import(staticExample());
  };

  beforeEach(async () => {
    io = new IoMem();
    await io.init();
    await io.isReady();
    db = new Db(io);
    await createDb(db);
  });

  const brands = async (db: Db): Promise<string[]> => {
    const { rljson } = await db.get(route, {});
    return (rljson.carGeneral._data as Json[])
      .map((row) => row.brand as string)
      .sort();
  };

  const insertPorscheMacan = async (db: Db) => {
    const { tree, cell } = await db.get(route, {});
    const path = cell.flatMap((c) => c.path)[0];
    const component = {
      brand: 'Porsche',
      type: 'Macan Electric',
      doors: 4,
      energyConsumption: 2.1,
      units: { energy: 'kWh/100km', _hash: '' },
      serviceIntervals: [20000, 40000, 60000],
      isElectric: true,
      meta: { pressText: 'An electric SUV.', _hash: '' },
      _type: 'components',
      _hash: '',
    } as CarGeneral;
    const insert = isolate(tree, path);
    inject(insert, path, component);
    return db.insert(route, insert);
  };

  describe('constructor', () => {
    it('creates an event model and an empty context by default', () => {
      expect(db.events).toBeInstanceOf(DbEvents);
      expect(db.context).toEqual({});
      expect(db.notify.events).toBe(db.events);
    });

    it('takes an event model and a context', async () => {
      const events = DbEvents.example();
      const hook = MockHook.example(events);
      const other = new Db(io, { events, context: { user: 'u' } });
      expect(other.events).toBe(events);

      await other.get(route, {});
      expect(hook.reads.length).toBeGreaterThan(0);
      expect(hook.reads.every((e) => e.context.user === 'u')).toBe(true);
    });
  });

  describe('reads', () => {
    it('return only what the read hooks leave', async () => {
      const all = await brands(db);
      expect(all).toContain('BMW');

      db.events.beforeRead(() => ({ filter: (row) => row.brand !== 'BMW' }), {
        tables: ['carGeneral'],
      });

      const filtered = await brands(db);
      expect(filtered).not.toContain('BMW');
      expect(filtered).toEqual(all.filter((b) => b !== 'BMW'));
    });

    it('are refused by a read hook', async () => {
      db.events.beforeRead(() => ({ refuse: 'hidden' }), {
        tables: ['carGeneral'],
      });
      await expect(db.get(route, {})).rejects.toBeInstanceOf(
        DbHookRefusedError,
      );
    });

    it('drop cached results when a read hook is added or removed', async () => {
      const all = await brands(db);
      const remove = db.events.beforeRead(() => ({
        filter: (row) => row.brand !== 'BMW',
      }));
      expect(await brands(db)).not.toContain('BMW');
      remove();
      expect(await brands(db)).toEqual(all);
    });
  });

  describe('writes', () => {
    it('are refused by a write hook, and nothing is written', async () => {
      const before = await io.rowCount('carGeneral');
      MockHook.example(db.events, { refuseWrites: 'read only' });

      await expect(insertPorscheMacan(db)).rejects.toThrow(
        'Hook "mockHook" refused write on carGeneral: read only',
      );
      expect(await io.rowCount('carGeneral')).toBe(before);
    });

    it('start the insert observers of the event model', async () => {
      const inserts: DbInsertEvent[] = [];
      const registered = vi.fn();
      db.events.afterInsert((e) => {
        inserts.push(e);
      });
      db.registerObserver(route, registered);

      const [row] = await insertPorscheMacan(db);
      expect(inserts.map((e) => e.route.flat)).toEqual(['/carGeneral']);
      expect(inserts[0].insertHistoryRow).toEqual(row);
      expect(registered).toHaveBeenCalledWith(row);
    });
  });

  describe('ioFor(context)', () => {
    it('returns an io on the same store with another context', async () => {
      const hook = MockHook.example(db.events, {
        filterReads: (row) => row.brand !== 'BMW',
      });
      const peerIo = db.ioFor({ peer: 'p1' });

      const { carGeneral } = await peerIo.readRows({
        table: 'carGeneral',
        where: {},
      });
      expect((carGeneral._data as Json[]).some((r) => r.brand === 'BMW')).toBe(
        false,
      );
      expect(hook.reads.at(-1)!.context).toEqual({ peer: 'p1' });
    });
  });
});
