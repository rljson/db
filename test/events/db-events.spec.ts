// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { IoMem } from '@rljson/io';
import { Route } from '@rljson/rljson';

import { beforeEach, describe, expect, it, vi } from 'vitest';

import {
  DbEvents,
  DbHookLoopError,
  DbHookRefusedError,
  DbInsertEvent,
  DbReadEvent,
  DbWriteEvent,
} from '../../src/index.ts';

import { carsData, carsIo, onlyAlice, ownersOf } from './test-store.ts';

describe('DbEvents', () => {
  let events: DbEvents;
  let io: IoMem;

  beforeEach(async () => {
    events = DbEvents.example();
    io = await carsIo();
  });

  const read = (depth = 0): DbReadEvent => ({
    kind: 'read',
    operation: 'readRows',
    table: 'cars',
    where: {},
    context: {},
    depth,
    io,
  });

  const write = (depth = 0): DbWriteEvent => ({
    kind: 'write',
    operation: 'write',
    tables: ['cars'],
    data: carsData('carol'),
    context: {},
    depth,
    io,
  });

  describe('example()', () => {
    it('returns an event model without hooks', () => {
      expect(events.hasReadHooks).toBe(false);
      expect(events.hasWriteHooks).toBe(false);
      expect(events.hasWriteObservers).toBe(false);
      expect(events.maxDepth).toBe(8);
    });
  });

  describe('beforeRead(hook)', () => {
    it('lets a read pass when no hook decides', async () => {
      events.beforeRead(() => undefined);
      events.beforeRead(() => ({ allow: true }));
      expect(await events.runBeforeRead(read())).toBeUndefined();
    });

    it('throws when a hook refuses', async () => {
      events.beforeRead(() => ({ refuse: 'no' }), { name: 'guard' });
      await expect(events.runBeforeRead(read())).rejects.toThrow(
        new DbHookRefusedError('guard', 'no', read()),
      );
      await expect(events.runBeforeRead(read())).rejects.toThrow(
        'Hook "guard" refused readRows on cars: no',
      );
    });

    it('names an unnamed hook after its function or anonymous', async () => {
      events.beforeRead(function namedHook() {
        return { refuse: 'a' };
      });
      await expect(events.runBeforeRead(read())).rejects.toThrow(
        'Hook "namedHook"',
      );

      const other = DbEvents.example();
      other.beforeRead((() => () => ({ refuse: 'b' }))());
      await expect(other.runBeforeRead(read())).rejects.toThrow(
        'Hook "anonymous"',
      );
    });

    it('returns the filter of a single filtering hook', async () => {
      events.beforeRead(() => ({ filter: onlyAlice }));
      expect(await events.runBeforeRead(read())).toBe(onlyAlice);
    });

    it('combines the filters of several hooks', async () => {
      events.beforeRead(() => ({ filter: onlyAlice }));
      events.beforeRead(() => ({ filter: (row) => row.brand === 'brand0' }));
      const filter = (await events.runBeforeRead(read()))!;
      expect(filter({ owner: 'alice', brand: 'brand0' }, 'cars')).toBe(true);
      expect(filter({ owner: 'alice', brand: 'brand1' }, 'cars')).toBe(false);
      expect(filter({ owner: 'bob', brand: 'brand0' }, 'cars')).toBe(false);
    });

    it('runs only the hooks of the tables read', async () => {
      const hook = vi.fn();
      events.beforeRead(hook, { tables: ['other'] });
      await events.runBeforeRead(read());
      expect(hook).not.toHaveBeenCalled();

      events.beforeRead(hook, { tables: ['cars'] });
      await events.runBeforeRead(read());
      expect(hook).toHaveBeenCalledTimes(1);
    });

    it('limits the filter of a table-bound hook to its tables', async () => {
      events.beforeRead(() => ({ filter: () => false }), { tables: ['cars'] });
      const filter = (await events.runBeforeRead(read()))!;
      expect(filter({ owner: 'alice' }, 'cars')).toBe(false);
      expect(filter({ owner: 'alice' }, 'other')).toBe(true);
    });

    it('runs table-bound hooks for a dump of all tables', async () => {
      const hook = vi.fn();
      events.beforeRead(hook, { tables: ['other'] });
      await events.runBeforeRead({
        ...read(),
        operation: 'dump',
        table: undefined,
      });
      expect(hook).toHaveBeenCalledTimes(1);
    });

    it('removes the hook with the returned function', async () => {
      const hook = vi.fn();
      const remove = events.beforeRead(hook);
      expect(events.hasReadHooks).toBe(true);
      remove();
      remove();
      expect(events.hasReadHooks).toBe(false);
      await events.runBeforeRead(read());
      expect(hook).not.toHaveBeenCalled();
    });
  });

  describe('beforeWrite(hook)', () => {
    it('throws when a hook refuses', async () => {
      events.beforeWrite(() => ({ refuse: 'read only' }), { name: 'ro' });
      await expect(events.runBeforeWrite(write())).rejects.toThrow(
        'Hook "ro" refused write on cars: read only',
      );
    });

    it('returns the filter of the hooks', async () => {
      events.beforeWrite(() => ({ filter: onlyAlice }));
      expect(await events.runBeforeWrite(write())).toBe(onlyAlice);
      expect(events.hasWriteHooks).toBe(true);
    });

    it('names the operation in a refusal without tables', async () => {
      events.beforeWrite(() => ({ refuse: 'x' }), { name: 'h' });
      await expect(
        events.runBeforeWrite({ ...write(), tables: [] }),
      ).rejects.toThrow('Hook "h" refused write: x');
    });
  });

  describe('order', () => {
    it('runs higher priorities first, then in the order of registration', async () => {
      const calls: string[] = [];
      const hook = (name: string) => () => {
        calls.push(name);
      };
      events.beforeRead(hook('a'));
      events.beforeRead(hook('b'), { priority: 10 });
      events.beforeRead(hook('c'));
      events.beforeRead(hook('d'), { priority: -1 });
      events.beforeRead(hook('e'), { priority: 10 });
      await events.runBeforeRead(read());
      expect(calls).toEqual(['b', 'e', 'a', 'c', 'd']);
    });

    it('stops at the first refusal', async () => {
      const later = vi.fn();
      events.beforeRead(() => ({ refuse: 'first' }), { priority: 1 });
      events.beforeRead(later);
      await expect(events.runBeforeRead(read())).rejects.toThrow('first');
      expect(later).not.toHaveBeenCalled();
    });

    it('awaits async hooks one after the other', async () => {
      const calls: string[] = [];
      events.beforeRead(async () => {
        await new Promise((r) => setTimeout(r, 5));
        calls.push('slow');
      });
      events.beforeRead(() => {
        calls.push('fast');
      });
      await events.runBeforeRead(read());
      expect(calls).toEqual(['slow', 'fast']);
    });

    it('keeps the hooks of a run when a hook removes another', async () => {
      const second = vi.fn();
      let removeSecond = () => {};
      events.beforeRead(() => removeSecond());
      removeSecond = events.beforeRead(second);
      await events.runBeforeRead(read());
      expect(second).toHaveBeenCalledTimes(1);
      await events.runBeforeRead(read());
      expect(second).toHaveBeenCalledTimes(1);
    });
  });

  describe('loop protection', () => {
    it('throws when an event is deeper than maxDepth', async () => {
      const small = DbEvents.example({ maxDepth: 2 });
      await expect(small.runBeforeRead(read(2))).resolves.toBeUndefined();
      await expect(small.runBeforeRead(read(3))).rejects.toThrow(
        new DbHookLoopError(3, 2),
      );
      await expect(small.runBeforeWrite(write(3))).rejects.toThrow(
        'Hooks triggered each other 3 levels deep; at most 2 are allowed.',
      );
    });

    it('ends hooks that trigger each other through their io', async () => {
      const wrapped = events.io(io);
      let calls = 0;
      events.beforeWrite(async (event) => {
        calls++;
        await event.io.write({ data: carsData('loop') });
      });

      await expect(
        wrapped.write({ data: carsData('carol') }),
      ).rejects.toBeInstanceOf(DbHookLoopError);
      expect(calls).toBe(events.maxDepth + 1);
    });
  });

  describe('afterWrite(observer)', () => {
    it('starts the observers of the written tables', async () => {
      const all = vi.fn();
      const cars = vi.fn();
      const other = vi.fn();
      events.afterWrite(all);
      events.afterWrite(cars, { tables: ['cars'] });
      events.afterWrite(other, { tables: ['other'] });
      expect(events.hasWriteObservers).toBe(true);

      events.emitAfterWrite(write());
      expect(all).toHaveBeenCalledWith(write());
      expect(cars).toHaveBeenCalledTimes(1);
      expect(other).not.toHaveBeenCalled();
    });

    it('reports errors of observers without throwing', async () => {
      const onObserverError = vi.fn();
      const reporting = DbEvents.example({ onObserverError });
      reporting.afterWrite(async () => {
        throw new Error('boom');
      });

      reporting.emitAfterWrite(write());
      await vi.waitFor(() => expect(onObserverError).toHaveBeenCalled());
      expect(onObserverError.mock.calls[0][0]).toBe(
        'Error notifying observers for write on cars:',
      );
    });

    it('does nothing without observers', () => {
      expect(() => events.emitAfterWrite(write())).not.toThrow();
    });
  });

  describe('afterInsert(observer)', () => {
    const route = Route.fromFlat('/cars');
    const otherRoute = Route.fromFlat('/other');
    const row = { timeId: 't', route: '/cars', carsRef: 'r' } as any;

    it('starts the observers of the route and those of all routes', () => {
      const ofRoute = vi.fn();
      const ofAll = vi.fn();
      const ofOther = vi.fn();
      events.afterInsert(ofRoute, { route });
      events.afterInsert(ofAll);
      events.afterInsert(ofOther, { route: otherRoute });

      expect(events.insertObservers(route)).toEqual([ofRoute, ofAll]);

      events.emitInsert(route, row);
      const event: DbInsertEvent = {
        kind: 'insert',
        route,
        insertHistoryRow: row,
      };
      expect(ofRoute).toHaveBeenCalledWith(event);
      expect(ofAll).toHaveBeenCalledWith(event);
      expect(ofOther).not.toHaveBeenCalled();
    });

    it('reports errors with the route', async () => {
      const onObserverError = vi.fn();
      const reporting = DbEvents.example({ onObserverError });
      reporting.afterInsert(() => Promise.reject(new Error('boom')));
      reporting.emitInsert(route, row);
      await vi.waitFor(() =>
        expect(onObserverError).toHaveBeenCalledWith(
          'Error notifying callbacks for route /cars:',
          new Error('boom'),
        ),
      );
    });
  });

  describe('onReadHooksChanged(listener)', () => {
    it('is called when a read hook is added or removed', () => {
      const listener = vi.fn();
      const stop = events.onReadHooksChanged(listener);

      const remove = events.beforeRead(() => undefined);
      expect(listener).toHaveBeenCalledTimes(1);

      events.beforeWrite(() => undefined);
      events.afterWrite(() => undefined);
      expect(listener).toHaveBeenCalledTimes(1);

      remove();
      expect(listener).toHaveBeenCalledTimes(2);

      stop();
      events.beforeRead(() => undefined);
      expect(listener).toHaveBeenCalledTimes(2);
    });
  });

  describe('io(io)', () => {
    it('wraps an io whose reads pass the hooks', async () => {
      events.beforeRead(() => ({ filter: onlyAlice }));
      const wrapped = events.io(io, { user: 'u' });
      const result = await wrapped.readRows({ table: 'cars', where: {} });
      expect(ownersOf(result)).toEqual(['alice']);
    });
  });
});
