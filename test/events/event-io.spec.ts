// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hsh } from '@rljson/hash';
import { Io, IoMem } from '@rljson/io';

import { beforeEach, describe, expect, it, vi } from 'vitest';

import {
  DbEvents,
  DbHookRefusedError,
  DbReadEvent,
  DbWriteEvent,
  EventIo,
} from '../../src/index.ts';

import {
  carsCfg,
  carsData,
  carsIo,
  onlyAlice,
  ownersOf,
} from './test-store.ts';

describe('EventIo', () => {
  let events: DbEvents;
  let io: IoMem;
  let eventIo: EventIo;

  beforeEach(async () => {
    events = DbEvents.example();
    io = await carsIo();
    eventIo = events.io(io, { user: 'u' }) as EventIo;
  });

  describe('without hooks', () => {
    it('passes everything to the wrapped io', async () => {
      expect(eventIo).toBeInstanceOf(EventIo);
      expect(eventIo.io).toBe(io);
      expect(eventIo.isOpen).toBe(io.isOpen);
      expect(eventIo.context).toEqual({ user: 'u' });
      expect(eventIo.depth).toBe(0);

      await eventIo.isReady();
      expect(await eventIo.tableExists('cars')).toBe(true);
      expect(await eventIo.contentType({ table: 'cars' })).toBe('components');
      expect(await eventIo.rawTableCfgs()).toEqual(await io.rawTableCfgs());
      expect(await eventIo.dump()).toEqual(await io.dump());
      expect(await eventIo.dumpTable({ table: 'cars' })).toEqual(
        await io.dumpTable({ table: 'cars' }),
      );
      expect(await eventIo.rowCount('cars')).toBe(2);
      expect(
        ownersOf(await eventIo.readRows({ table: 'cars', where: {} })),
      ).toEqual(['alice', 'bob']);

      await eventIo.write({ data: carsData('carol') });
      expect(await io.rowCount('cars')).toBe(3);

      await eventIo.close();
      expect(io.isOpen).toBe(false);
    });

    it('initializes the wrapped io', async () => {
      const fresh = new IoMem();
      const wrapped = events.io(fresh);
      await wrapped.init();
      expect(fresh.isOpen).toBe(true);
    });

    it('offers readRowsByHashes only when the wrapped io has it', async () => {
      const withoutBatch = { ...io, readRowsByHashes: undefined } as Io;
      expect(events.io(withoutBatch).readRowsByHashes).toBeUndefined();

      const hash = (await io.readRows({ table: 'cars', where: {} })).cars
        ._data[0]._hash as string;
      const result = await eventIo.readRowsByHashes!({
        table: 'cars',
        hashes: [hash],
      });
      expect(result.cars._data.length).toBe(1);
    });
  });

  describe('reads', () => {
    it('hand the hooks what is read, by whom and the follow-up io', async () => {
      const seen: DbReadEvent[] = [];
      events.beforeRead((e) => {
        seen.push(e);
      });

      await eventIo.readRows({ table: 'cars', where: { owner: 'bob' } });
      await eventIo.readRowsByHashes!({ table: 'cars', hashes: ['h'] });
      await eventIo.dump();
      await eventIo.dumpTable({ table: 'cars' });
      await eventIo.rowCount('cars');

      expect(
        seen.map(({ operation, table, where, hashes, context, depth }) => ({
          operation,
          table,
          where,
          hashes,
          context,
          depth,
        })),
      ).toEqual([
        {
          operation: 'readRows',
          table: 'cars',
          where: { owner: 'bob' },
          hashes: undefined,
          context: { user: 'u' },
          depth: 0,
        },
        {
          operation: 'readRowsByHashes',
          table: 'cars',
          where: undefined,
          hashes: ['h'],
          context: { user: 'u' },
          depth: 0,
        },
        {
          operation: 'dump',
          table: undefined,
          where: undefined,
          hashes: undefined,
          context: { user: 'u' },
          depth: 0,
        },
        {
          operation: 'dumpTable',
          table: 'cars',
          where: undefined,
          hashes: undefined,
          context: { user: 'u' },
          depth: 0,
        },
        {
          operation: 'rowCount',
          table: 'cars',
          where: undefined,
          hashes: undefined,
          context: { user: 'u' },
          depth: 0,
        },
      ]);

      const followUp = seen[0].io as EventIo;
      expect(followUp.depth).toBe(1);
      expect(followUp.context).toEqual({ user: 'u' });
      expect(followUp.io).toBe(io);
    });

    it('throw when a hook refuses, without reading', async () => {
      const readRows = vi.spyOn(io, 'readRows');
      events.beforeRead(() => ({ refuse: 'secret' }), { name: 'guard' });
      await expect(
        eventIo.readRows({ table: 'cars', where: {} }),
      ).rejects.toBeInstanceOf(DbHookRefusedError);
      await expect(eventIo.rowCount('cars')).rejects.toThrow(
        'Hook "guard" refused rowCount on cars: secret',
      );
      expect(readRows).not.toHaveBeenCalled();
    });

    it('return only the rows the filters keep, with updated hashes', async () => {
      events.beforeRead(() => ({ filter: onlyAlice }));

      const rows = await eventIo.readRows({ table: 'cars', where: {} });
      expect(ownersOf(rows)).toEqual(['alice']);
      expect(rows.cars._hash).toBe(hsh(rows.cars as any)._hash);

      const dump = await eventIo.dump();
      expect(ownersOf(dump)).toEqual(['alice']);
      expect(dump._hash).toBe(hsh(dump as any)._hash);
      expect(dump.tableCfgs).toEqual((await io.dump()).tableCfgs);

      expect(ownersOf(await eventIo.dumpTable({ table: 'cars' }))).toEqual([
        'alice',
      ]);
      expect(await eventIo.rowCount('cars')).toBe(1);
    });

    it('return the result unchanged when the filter keeps every row', async () => {
      events.beforeRead(() => ({ filter: () => true }));
      expect(await eventIo.dump()).toEqual(await io.dump());
    });

    it('count the rows directly when no hook filters', async () => {
      events.beforeRead(() => undefined);
      expect(await eventIo.rowCount('cars')).toBe(2);
    });

    it('keep tables without hash unhashed', async () => {
      events.beforeRead(() => ({ filter: onlyAlice }));
      const unhashed = {
        ...io,
        readRows: async () => ({
          cars: {
            _type: 'components',
            _data: [{ owner: 'alice' }, { owner: 'bob' }],
          },
        }),
      } as unknown as Io;
      const result = await events
        .io(unhashed)
        .readRows({ table: 'cars', where: {} });
      expect(result).toEqual({
        cars: { _type: 'components', _data: [{ owner: 'alice' }] },
      });
    });
  });

  describe('writes', () => {
    it('hand the hooks what is written, then start the observers', async () => {
      const seen: DbWriteEvent[] = [];
      const observed: DbWriteEvent[] = [];
      events.beforeWrite((e) => {
        seen.push(e);
      });
      events.afterWrite((e) => {
        observed.push(e);
      });

      const data = carsData('carol');
      await eventIo.write({ data });

      expect(seen.length).toBe(1);
      expect(seen[0]).toMatchObject({
        kind: 'write',
        operation: 'write',
        tables: ['cars'],
        data,
        context: { user: 'u' },
        depth: 0,
      });
      expect(observed.length).toBe(1);
      expect(observed[0].data).toBe(data);
    });

    it('throw when a hook refuses, without writing', async () => {
      events.beforeWrite(() => ({ refuse: 'read only' }));
      await expect(
        eventIo.write({ data: carsData('carol') }),
      ).rejects.toBeInstanceOf(DbHookRefusedError);
      expect(await io.rowCount('cars')).toBe(2);
    });

    it('write only the rows the filters keep', async () => {
      const observed: DbWriteEvent[] = [];
      events.beforeWrite(() => ({ filter: onlyAlice }));
      events.afterWrite((e) => {
        observed.push(e);
      });

      await eventIo.write({ data: carsData('alice', 'carol') });
      const rows = await io.readRows({ table: 'cars', where: {} });
      expect(ownersOf(rows)).toEqual(['alice', 'bob']);
      expect(ownersOf(observed[0].data!)).toEqual(['alice']);
    });

    it('write without hooks and observers straight through', async () => {
      const write = vi.spyOn(io, 'write');
      const data = carsData('carol');
      await eventIo.write({ data });
      expect(write).toHaveBeenCalledWith({ data });
    });

    it('run the hooks for creating or extending a table', async () => {
      const seen: DbWriteEvent[] = [];
      const observed: DbWriteEvent[] = [];
      const tableCfg = carsCfg('trucks');
      events.beforeWrite((e) => {
        seen.push(e);
      });
      events.afterWrite((e) => {
        observed.push(e);
      });

      await eventIo.createOrExtendTable({ tableCfg });
      expect(seen[0]).toMatchObject({
        operation: 'createOrExtendTable',
        tables: ['trucks'],
        tableCfg,
      });
      expect(observed.length).toBe(1);
      expect(await io.tableExists('trucks')).toBe(true);
    });

    it('refuse creating a table', async () => {
      events.beforeWrite(() => ({ refuse: 'no tables' }));
      await expect(
        eventIo.createOrExtendTable({
          tableCfg: carsCfg('trucks'),
        }),
      ).rejects.toThrow('no tables');
      expect(await io.tableExists('trucks')).toBe(false);
    });

    it('start the observers of a created table without hooks', async () => {
      const observed: DbWriteEvent[] = [];
      events.afterWrite((e) => {
        observed.push(e);
      });
      await eventIo.createOrExtendTable({ tableCfg: carsCfg('trucks') });
      expect(observed.map((e) => e.tables)).toEqual([['trucks']]);
    });

    it('create a table without hooks straight through', async () => {
      await eventIo.createOrExtendTable({
        tableCfg: carsCfg('trucks'),
      });
      expect(await io.tableExists('trucks')).toBe(true);
    });
  });
});
