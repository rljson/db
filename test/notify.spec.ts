// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Route } from '@rljson/rljson';

import { describe, expect, it, vi } from 'vitest';

import { DbEvents, Notify } from '../src/index.ts';

describe('Notify', () => {
  const route = Route.fromFlat('/cars');
  const row = { timeId: 't', route: '/cars', carsRef: 'r' } as any;

  it('creates an event model of its own by default', () => {
    expect(new Notify().events).toBeInstanceOf(DbEvents);
  });

  it('registers its callbacks as insert observers of the event model', () => {
    const events = DbEvents.example();
    const notify = new Notify(events);
    const callback = vi.fn();
    const inserted = vi.fn();
    notify.register(route, callback);
    events.afterInsert(inserted);

    notify.notify(route, row);
    expect(callback).toHaveBeenCalledWith(row);
    expect(inserted).toHaveBeenCalledTimes(1);

    events.emitInsert(route, row);
    expect(callback).toHaveBeenCalledTimes(2);
  });

  it('calls the callbacks in the order of registration', () => {
    const notify = new Notify();
    const calls: string[] = [];
    notify.register(route, async () => calls.push('a'));
    notify.register(route, async () => calls.push('b'));
    notify.notify(route, row);
    expect(calls).toEqual(['a', 'b']);
  });

  it('unregisters every registration of a callback, and only that one', () => {
    const notify = new Notify();
    const a = vi.fn();
    const b = vi.fn();
    notify.register(route, a);
    notify.register(route, b);
    notify.register(route, a);

    notify.unregister(route, a);
    expect(notify.getCallBacksForRoute(route)).toEqual([b]);

    notify.notify(route, row);
    expect(a).not.toHaveBeenCalled();
    expect(b).toHaveBeenCalledTimes(1);

    notify.unregister(route, b);
    expect(notify.callbacks.get('/cars')).toEqual([]);
  });

  it('unregisters all callbacks of a route', () => {
    const notify = new Notify();
    const a = vi.fn();
    notify.register(route, a);
    notify.unregisterAll(route);
    notify.unregisterAll(route);
    notify.notify(route, row);
    expect(a).not.toHaveBeenCalled();
    expect(notify.callbacks.size).toBe(0);
  });

  it('logs failing callbacks without throwing', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {});
    const notify = new Notify();
    notify.register(route, async () => {
      throw new Error('boom');
    });
    notify.notify(route, row);
    await vi.waitFor(() =>
      expect(error).toHaveBeenCalledWith(
        'Error notifying callbacks for route /cars:',
        new Error('boom'),
      ),
    );
    error.mockRestore();
  });
});
