// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { IoMem, SocketMock } from '@rljson/io';
import { RefStamp, Route, syncEvents } from '@rljson/rljson';

import { beforeEach, describe, expect, it, vi } from 'vitest';

import { Connector, RefArrivalInfo } from '../../src/connector/connector';
import { Db } from '../../src/db';

const route = Route.fromFlat('/fsTree');
const events = syncEvents(route.flat);
const stamp: RefStamp = { domain: 'office', epoch: 2, hub: 'node-1', n: 7 };

describe('Connector — the hub stamp', () => {
  let socket: SocketMock;
  let connector: Connector;

  beforeEach(async () => {
    const io = new IoMem();
    await io.init();
    await io.isReady();
    socket = new SocketMock();
    connector = new Connector(new Db(io), route, socket);
  });

  /** The payloads the connector sent on the ref event. */
  const sentPayloads = (): Array<Record<string, unknown>> => {
    const emit = vi.mocked(socket.emit);
    return emit.mock.calls
      .filter(([event]) => event === events.ref)
      .map(([, payload]) => payload as Record<string, unknown>);
  };

  /** What the listener was told about each ref it received. */
  const arrivals = (): Map<string, RefArrivalInfo | undefined> => {
    const seen = new Map<string, RefArrivalInfo | undefined>();
    connector.listen(async (ref, _predecessors, info) => {
      seen.set(ref, info);
    });
    return seen;
  };

  describe('send()', () => {
    it('carries a stamp the caller passes', () => {
      vi.spyOn(socket, 'emit');
      connector.send('ref1', { stamp });
      expect(sentPayloads()[0].stamp).toEqual(stamp);
    });

    it('sends no stamp without one, and none for a malformed one', () => {
      vi.spyOn(socket, 'emit');
      connector.send('ref1');
      connector.send('ref2', {
        stamp: { domain: 'office' } as unknown as RefStamp,
      });
      expect(sentPayloads().map((p) => 'stamp' in p)).toEqual([false, false]);
    });
  });

  describe('a received ref', () => {
    it('reports the stamp of an announcement to the listener', () => {
      const seen = arrivals();
      socket.emit(events.ref, { o: 'peer', r: 'ref1', stamp });
      expect(seen.get('ref1')?.stamp).toEqual(stamp);
    });

    it('reports the stamp of a bootstrap', () => {
      const seen = arrivals();
      socket.emit(events.bootstrap, { o: 'peer', r: 'ref1', stamp });
      expect(seen.get('ref1')?.stamp).toEqual(stamp);
    });

    it('reports no stamp when there is none, or a malformed one', () => {
      const seen = arrivals();
      socket.emit(events.ref, { o: 'peer', r: 'ref1' });
      socket.emit(events.ref, {
        o: 'peer',
        r: 'ref2',
        stamp: { ...stamp, n: -1 },
      });
      expect(seen.get('ref1')).toEqual({ isNewestFromSender: true });
      expect(seen.get('ref2')?.stamp).toBeUndefined();
    });
  });

  describe('onStamp()', () => {
    it('hears the stamp the hub gave a sent ref', () => {
      const heard: Array<[string, RefStamp]> = [];
      connector.onStamp((ref, s) => heard.push([ref, s]));
      socket.emit(events.stamp, { r: 'ref1', stamp });
      expect(heard).toEqual([['ref1', stamp]]);
    });

    it('drops a malformed notice', () => {
      const heard: string[] = [];
      connector.onStamp((ref) => heard.push(ref));
      socket.emit(events.stamp, null);
      socket.emit(events.stamp, { stamp });
      socket.emit(events.stamp, { r: 'ref1', stamp: { domain: 'x' } });
      expect(heard).toEqual([]);
    });

    it('stops calling a callback that was removed', () => {
      const heard: string[] = [];
      const off = connector.onStamp((ref) => heard.push(ref));
      off();
      socket.emit(events.stamp, { r: 'ref1', stamp });
      expect(heard).toEqual([]);
    });

    it('is deaf after tearDown and hears again once listening is re-armed', () => {
      const heard: string[] = [];
      connector.onStamp((ref) => heard.push(ref));

      connector.tearDown();
      socket.emit(events.stamp, { r: 'ref1', stamp });
      expect(heard).toEqual([]);

      connector.listen(async () => {});
      socket.emit(events.stamp, { r: 'ref2', stamp });
      expect(heard).toEqual(['ref2']);
    });
  });
});
