// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { IoMem, SocketMock } from '@rljson/io';
import { Route, syncEvents } from '@rljson/rljson';

import { describe, expect, it, vi } from 'vitest';

import { Connector, stateBeaconEvent } from '../../src/connector/connector';
import { Db } from '../../src/db';

// ONE-446. The state beacon is only useful because the connector ignores it:
// a hub announcement that reached the apply path (the bootstrap heartbeat)
// was measured net-harmful. This pins both halves of that contract.
describe('stateBeaconEvent', () => {
  it('is the route with :state', () => {
    expect(stateBeaconEvent('/sharedTree')).toBe('/sharedTree:state');
  });

  it('is none of the events the connector acts on', () => {
    const events = Object.values(syncEvents('/sharedTree'));
    expect(events).not.toContain(stateBeaconEvent('/sharedTree'));
  });

  it('never reaches a connector listener', async () => {
    const io = new IoMem();
    await io.init();
    const route = Route.fromFlat('/sharedTree');
    const socket = new SocketMock();
    const connector = new Connector(new Db(io), route, socket);
    const heard = vi.fn(async () => {});
    connector.listen(heard);

    socket.emit(stateBeaconEvent(route.flat), { o: 'hub', r: 'hub-state' });

    expect(heard).not.toHaveBeenCalled();
    connector.tearDown();
  });
});
