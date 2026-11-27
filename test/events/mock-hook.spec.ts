// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { IoMem } from '@rljson/io';

import { beforeEach, describe, expect, it } from 'vitest';

import { DbEvents, MockHook } from '../../src/index.ts';

import { carsData, carsIo, onlyAlice, ownersOf } from './test-store.ts';

describe('MockHook', () => {
  let events: DbEvents;
  let io: IoMem;

  beforeEach(async () => {
    events = DbEvents.example();
    io = await carsIo();
  });

  it('records reads and writes and allows them by default', async () => {
    const hook = MockHook.example(events);
    const wrapped = events.io(io, { user: 'u' });

    await wrapped.readRows({ table: 'cars', where: {} });
    await wrapped.write({ data: carsData('carol') });

    expect(hook.reads.map((e) => e.operation)).toEqual(['readRows']);
    expect(hook.writes.map((e) => e.tables)).toEqual([['cars']]);
    expect(hook.writes[0].context).toEqual({ user: 'u' });
    expect(await io.rowCount('cars')).toBe(3);
  });

  it('refuses reads and writes', async () => {
    MockHook.example(events, { refuseReads: 'r', refuseWrites: 'w' });
    const wrapped = events.io(io);

    await expect(wrapped.dump()).rejects.toThrow(
      'Hook "mockHook" refused dump: r',
    );
    await expect(wrapped.write({ data: carsData('x') })).rejects.toThrow(
      'Hook "mockHook" refused write on cars: w',
    );
  });

  it('filters reads and writes', async () => {
    MockHook.example(events, {
      filterReads: onlyAlice,
      filterWrites: onlyAlice,
    });
    const wrapped = events.io(io);

    await wrapped.write({ data: carsData('alice', 'carol') });
    expect(await io.rowCount('cars')).toBe(2);
    expect(
      ownersOf(await wrapped.readRows({ table: 'cars', where: {} })),
    ).toEqual(['alice']);
  });

  it('can be detached and attached again', async () => {
    const hook = new MockHook({ refuseReads: 'no' }).attach(events);
    expect(hook.options).toEqual({ refuseReads: 'no' });
    hook.detach();
    expect(events.hasReadHooks).toBe(false);
    expect(events.hasWriteHooks).toBe(false);

    hook.attach(events);
    hook.attach(events);
    await expect(events.io(io).dump()).rejects.toThrow('no');
    hook.detach();
    await expect(events.io(io).dump()).resolves.toBeDefined();
  });
});
