// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';
import { IoMem, Socket, SocketMock } from '@rljson/io';
import {
  AckPayload,
  ConnectorPayload,
  createEditHistoryTableCfg,
  createEditTableCfg,
  createMultiEditTableCfg,
  Edit,
  EditHistory,
  GapFillResponse,
  isClientId,
  MultiEdit,
  Route,
  SyncConfig,
  syncEvents,
  timeId,
} from '@rljson/rljson';

import { beforeEach, describe, expect, it, vi } from 'vitest';

import { Connector } from '../../src/connector/connector';
import { Db } from '../../src/db';
import { exampleEditActionColumnSelection } from '../../src/edit/edit-action';
import { staticExample } from '../../src/example-static/example-static';

describe('Connector sync protocol', () => {
  let db: Db;
  let socket: Socket;

  let editHistory: EditHistory;

  const cakeKey = 'carCake';
  const cakeRef = staticExample().carCake._data[2]._hash as string;
  const route = Route.fromFlat(`${cakeKey}EditHistory`);
  const events = syncEvents(route.flat);

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

    socket = new SocketMock();

    const editActionColumnSelection = exampleEditActionColumnSelection();
    const edit = hip<Edit>({
      name: 'testEdit',
      action: editActionColumnSelection,
      _hash: '',
    });

    const { [cakeKey + 'EditsRef']: editRef } = (
      await db.addEdit(cakeKey, edit)
    )[0] as any;

    const multiEdit = hip<MultiEdit>({
      previous: null,
      edit: editRef!,
      _hash: '',
    });

    const { [cakeKey + 'MultiEditsRef']: multiEditRef } = (
      await db.addMultiEdit(cakeKey, multiEdit)
    )[0] as any;

    editHistory = hip<EditHistory>({
      timeId: timeId(),
      dataRef: cakeRef,
      multiEditRef: multiEditRef!,
      previous: [],
      _hash: '',
    });
  });

  // =========================================================================
  // Backward compatibility (no SyncConfig)
  // =========================================================================

  describe('backward compatibility (no SyncConfig)', () => {
    it('should work identically when no SyncConfig is provided', () => {
      const connector = new Connector(db, route, socket);

      expect(connector.syncConfig).toBeUndefined();
      expect(connector.clientIdentity).toBeUndefined();
      expect(connector.seq).toBe(0);

      const callback = vi.fn();
      socket.on(events.ref, callback);

      connector.send(editHistory._hash);

      expect(callback).toHaveBeenCalledTimes(1);
      const payload = callback.mock.calls[0][0] as ConnectorPayload;
      expect(payload.o).toBe(connector.origin);
      expect(payload.r).toBe(editHistory._hash);
      expect(payload.c).toBeUndefined();
      expect(payload.t).toBeUndefined();
      expect(payload.seq).toBeUndefined();
      expect(payload.p).toBeUndefined();

      connector.teardown();
    });
  });

  // =========================================================================
  // Client identity
  // =========================================================================

  describe('includeClientIdentity', () => {
    it('should auto-generate clientId when includeClientIdentity is true', () => {
      const config: SyncConfig = { includeClientIdentity: true };
      const connector = new Connector(db, route, socket, config);

      expect(connector.clientIdentity).toBeDefined();
      expect(isClientId(connector.clientIdentity!)).toBe(true);

      connector.teardown();
    });

    it('should use provided clientIdentity over auto-generated', () => {
      const config: SyncConfig = { includeClientIdentity: true };
      const myId = 'client_MyCustomId123';
      const connector = new Connector(db, route, socket, config, myId);

      expect(connector.clientIdentity).toBe(myId);

      connector.teardown();
    });

    it('should attach c and t to payload when enabled', () => {
      const config: SyncConfig = { includeClientIdentity: true };
      const connector = new Connector(db, route, socket, config);

      const callback = vi.fn();
      socket.on(events.ref, callback);

      const before = Date.now();
      connector.send(editHistory._hash);
      const after = Date.now();

      const payload = callback.mock.calls[0][0] as ConnectorPayload;
      expect(payload.c).toBe(connector.clientIdentity);
      expect(payload.t).toBeGreaterThanOrEqual(before);
      expect(payload.t).toBeLessThanOrEqual(after);

      connector.teardown();
    });

    it('should not attach c/t when includeClientIdentity is false', () => {
      const config: SyncConfig = { includeClientIdentity: false };
      const connector = new Connector(db, route, socket, config);

      const callback = vi.fn();
      socket.on(events.ref, callback);

      connector.send(editHistory._hash);

      const payload = callback.mock.calls[0][0] as ConnectorPayload;
      expect(payload.c).toBeUndefined();
      expect(payload.t).toBeUndefined();

      connector.teardown();
    });
  });

  // =========================================================================
  // Causal ordering
  // =========================================================================

  describe('causalOrdering', () => {
    it('should attach seq to payload when causalOrdering is enabled', () => {
      const config: SyncConfig = { causalOrdering: true };
      const connector = new Connector(db, route, socket, config);

      const callback = vi.fn();
      socket.on(events.ref, callback);

      connector.send(editHistory._hash);

      const payload = callback.mock.calls[0][0] as ConnectorPayload;
      expect(payload.seq).toBe(1);
      expect(connector.seq).toBe(1);

      connector.teardown();
    });

    it('should increment seq with each send', () => {
      const config: SyncConfig = { causalOrdering: true };
      const connector = new Connector(db, route, socket, config);

      const callback = vi.fn();
      socket.on(events.ref, callback);

      connector.send('ref1');
      connector.send('ref2');
      connector.send('ref3');

      expect(callback).toHaveBeenCalledTimes(3);
      expect((callback.mock.calls[0][0] as ConnectorPayload).seq).toBe(1);
      expect((callback.mock.calls[1][0] as ConnectorPayload).seq).toBe(2);
      expect((callback.mock.calls[2][0] as ConnectorPayload).seq).toBe(3);
      expect(connector.seq).toBe(3);

      connector.teardown();
    });

    it('should attach predecessors when set via setPredecessors', () => {
      const config: SyncConfig = { causalOrdering: true };
      const connector = new Connector(db, route, socket, config);

      const callback = vi.fn();
      socket.on(events.ref, callback);

      connector.setPredecessors(['1700000000000:AbCd']);
      connector.send(editHistory._hash);

      const payload = callback.mock.calls[0][0] as ConnectorPayload;
      expect(payload.p).toEqual(['1700000000000:AbCd']);

      connector.teardown();
    });

    it('should not attach p when predecessors is empty', () => {
      const config: SyncConfig = { causalOrdering: true };
      const connector = new Connector(db, route, socket, config);

      const callback = vi.fn();
      socket.on(events.ref, callback);

      connector.send(editHistory._hash);

      const payload = callback.mock.calls[0][0] as ConnectorPayload;
      expect(payload.p).toBeUndefined();

      connector.teardown();
    });

    it('should detect gap and emit gapFillReq', () => {
      const config: SyncConfig = {
        causalOrdering: true,
        includeClientIdentity: true,
      };
      const connector = new Connector(db, route, socket, config);

      const gapCallback = vi.fn();
      socket.on(events.gapFillReq, gapCallback);

      // Simulate receiving seq 1 from a peer
      const peerId = 'client_PeerClient1';
      socket.emit(events.ref, {
        o: 'other-origin',
        r: 'ref1',
        c: peerId,
        seq: 1,
      } as ConnectorPayload);

      expect(gapCallback).not.toHaveBeenCalled();

      // Simulate receiving seq 5 (gap: 2, 3, 4 missing)
      socket.emit(events.ref, {
        o: 'other-origin',
        r: 'ref5',
        c: peerId,
        seq: 5,
      } as ConnectorPayload);

      expect(gapCallback).toHaveBeenCalledTimes(1);
      expect(gapCallback).toHaveBeenCalledWith({
        route: route.flat,
        afterSeq: 1,
      });

      connector.teardown();
    });

    it('should not detect gap for sequential messages', () => {
      const config: SyncConfig = {
        causalOrdering: true,
        includeClientIdentity: true,
      };
      const connector = new Connector(db, route, socket, config);

      const gapCallback = vi.fn();
      socket.on(events.gapFillReq, gapCallback);

      const peerId = 'client_PeerClient1';
      socket.emit(events.ref, {
        o: 'other-origin',
        r: 'ref1',
        c: peerId,
        seq: 1,
      } as ConnectorPayload);

      socket.emit(events.ref, {
        o: 'other-origin-2',
        r: 'ref2',
        c: peerId,
        seq: 2,
      } as ConnectorPayload);

      expect(gapCallback).not.toHaveBeenCalled();

      connector.teardown();
    });

    it('should process gap-fill response refs', () => {
      const config: SyncConfig = { causalOrdering: true };
      const connector = new Connector(db, route, socket, config);

      const notifyCallback = vi.fn();
      (connector as any)._callbacks.push(notifyCallback);

      // Simulate receiving gap-fill response
      const gapFillRes: GapFillResponse = {
        route: route.flat,
        refs: [
          { o: 'other-origin', r: 'ref2', seq: 2 },
          { o: 'other-origin', r: 'ref3', seq: 3 },
        ],
      };

      socket.emit(events.gapFillRes, gapFillRes);

      expect(notifyCallback).toHaveBeenCalledTimes(2);
      expect(notifyCallback).toHaveBeenCalledWith('ref2');
      expect(notifyCallback).toHaveBeenCalledWith('ref3');

      connector.teardown();
    });

    it('should not re-process already received refs from gap-fill', () => {
      const config: SyncConfig = { causalOrdering: true };
      const connector = new Connector(db, route, socket, config);

      const notifyCallback = vi.fn();
      (connector as any)._callbacks.push(notifyCallback);

      // First receive ref2 normally
      socket.emit(events.ref, {
        o: 'other-origin',
        r: 'ref2',
      } as ConnectorPayload);

      expect(notifyCallback).toHaveBeenCalledTimes(1);

      // Then receive gap-fill containing ref2 again
      const gapFillRes: GapFillResponse = {
        route: route.flat,
        refs: [
          { o: 'other-origin', r: 'ref2', seq: 2 },
          { o: 'other-origin', r: 'ref3', seq: 3 },
        ],
      };

      socket.emit(events.gapFillRes, gapFillRes);

      // ref2 should not trigger again, only ref3
      expect(notifyCallback).toHaveBeenCalledTimes(2);
      expect(notifyCallback).toHaveBeenLastCalledWith('ref3');

      connector.teardown();
    });
  });

  // =========================================================================
  // Acknowledgment
  // =========================================================================

  describe('requireAck', () => {
    it('should emit ackClient when receiving a ref with requireAck', () => {
      const config: SyncConfig = { requireAck: true };
      const connector = new Connector(db, route, socket, config);

      const ackCallback = vi.fn();
      socket.on(events.ackClient, ackCallback);

      // Simulate receiving a ref from another connector
      socket.emit(events.ref, {
        o: 'other-origin',
        r: 'ref1',
      } as ConnectorPayload);

      expect(ackCallback).toHaveBeenCalledTimes(1);
      expect(ackCallback).toHaveBeenCalledWith({ r: 'ref1' });

      connector.teardown();
    });

    it('should not emit ackClient when requireAck is false', () => {
      const config: SyncConfig = { requireAck: false };
      const connector = new Connector(db, route, socket, config);

      const ackCallback = vi.fn();
      socket.on(events.ackClient, ackCallback);

      socket.emit(events.ref, {
        o: 'other-origin',
        r: 'ref1',
      } as ConnectorPayload);

      expect(ackCallback).not.toHaveBeenCalled();

      connector.teardown();
    });

    it('sendWithAck should use default timeout when ackTimeoutMs is not set', async () => {
      const config: SyncConfig = { requireAck: true };
      const connector = new Connector(db, route, socket, config);

      const ack: AckPayload = {
        r: editHistory._hash,
        ok: true,
        receivedBy: 1,
        totalClients: 1,
      };

      // Should use default 10_000ms timeout, resolve quickly
      const ackPromise = connector.sendWithAck(editHistory._hash);
      setTimeout(() => {
        socket.emit(events.ack, ack);
      }, 10);

      const result = await ackPromise;
      expect(result.r).toBe(editHistory._hash);

      connector.teardown();
    });

    it('sendWithAck should resolve when ACK is received', async () => {
      const config: SyncConfig = { requireAck: true, ackTimeoutMs: 1000 };
      const connector = new Connector(db, route, socket, config);

      const ack: AckPayload = {
        r: editHistory._hash,
        ok: true,
        receivedBy: 2,
        totalClients: 2,
      };

      // Send and wait for ack — simulate server responding after 10ms
      const ackPromise = connector.sendWithAck(editHistory._hash);
      setTimeout(() => {
        socket.emit(events.ack, ack);
      }, 10);

      const result = await ackPromise;
      expect(result).toEqual(ack);

      connector.teardown();
    });

    it('sendWithAck should reject on timeout', async () => {
      const config: SyncConfig = { requireAck: true, ackTimeoutMs: 50 };
      const connector = new Connector(db, route, socket, config);

      // Send without any ACK coming back
      await expect(connector.sendWithAck(editHistory._hash)).rejects.toThrow(
        'ACK timeout',
      );

      connector.teardown();
    });

    it('sendWithAck should ignore ACK for different ref', async () => {
      const config: SyncConfig = { requireAck: true, ackTimeoutMs: 100 };
      const connector = new Connector(db, route, socket, config);

      const ackPromise = connector.sendWithAck(editHistory._hash);

      // Send ACK for a different ref — should not resolve
      setTimeout(() => {
        socket.emit(events.ack, {
          r: 'different-ref',
          ok: true,
        } as AckPayload);
      }, 10);

      // Then send correct ACK
      setTimeout(() => {
        socket.emit(events.ack, {
          r: editHistory._hash,
          ok: true,
          receivedBy: 1,
          totalClients: 1,
        } as AckPayload);
      }, 30);

      const result = await ackPromise;
      expect(result.r).toBe(editHistory._hash);

      connector.teardown();
    });
  });

  // =========================================================================
  // Combined config
  // =========================================================================

  describe('all features enabled', () => {
    it('should attach all fields when all flags are true', () => {
      const config: SyncConfig = {
        causalOrdering: true,
        requireAck: true,
        ackTimeoutMs: 5_000,
        includeClientIdentity: true,
      };
      const connector = new Connector(db, route, socket, config);

      const callback = vi.fn();
      socket.on(events.ref, callback);

      connector.setPredecessors(['1700000000000:AbCd']);
      connector.send(editHistory._hash);

      const payload = callback.mock.calls[0][0] as ConnectorPayload;
      expect(payload.o).toBe(connector.origin);
      expect(payload.r).toBe(editHistory._hash);
      expect(payload.c).toBe(connector.clientIdentity);
      expect(payload.t).toBeDefined();
      expect(payload.seq).toBe(1);
      expect(payload.p).toEqual(['1700000000000:AbCd']);

      connector.teardown();
    });
  });

  // =========================================================================
  // Events getter
  // =========================================================================

  describe('events getter', () => {
    it('should return typed event names based on route', () => {
      const connector = new Connector(db, route, socket);

      expect(connector.events.ref).toBe(route.flat);
      expect(connector.events.ack).toBe(`${route.flat}:ack`);
      expect(connector.events.ackClient).toBe(`${route.flat}:ack:client`);
      expect(connector.events.gapFillReq).toBe(`${route.flat}:gapfill:req`);
      expect(connector.events.gapFillRes).toBe(`${route.flat}:gapfill:res`);

      connector.teardown();
    });
  });

  // =========================================================================
  // Teardown with sync features
  // =========================================================================

  describe('teardown with sync features', () => {
    it('should clean up all event listeners on teardown', () => {
      const config: SyncConfig = {
        causalOrdering: true,
        requireAck: true,
        includeClientIdentity: true,
      };
      const connector = new Connector(db, route, socket, config);

      connector.teardown();

      expect(connector.isListening).toBe(false);

      // Verify no callbacks fire after teardown
      const callback = vi.fn();
      (connector as any)._callbacks.push(callback);

      socket.emit(events.ref, {
        o: 'other-origin',
        r: 'ref1',
      } as ConnectorPayload);

      // The callback should not be called because the listener was removed
      expect(callback).not.toHaveBeenCalled();
    });
  });

  // =========================================================================
  // Bounded dedup sets (two-generation eviction)
  // =========================================================================

  describe('bounded dedup sets', () => {
    it('should evict old sent refs when maxDedupSetSize is reached', () => {
      const config: SyncConfig = { maxDedupSetSize: 3 };
      const connector = new Connector(db, route, socket, config);

      const callback = vi.fn();
      socket.on(events.ref, callback);

      // Send 3 refs — fills the current generation
      connector.send('a');
      connector.send('b');
      connector.send('c');
      expect(callback).toHaveBeenCalledTimes(3);

      // Sending 'a' again should be deduped (it's in previous generation)
      connector.send('a');
      expect(callback).toHaveBeenCalledTimes(3);

      // Send 3 more — fills current generation again, evicts previous
      connector.send('d');
      connector.send('e');
      connector.send('f');
      expect(callback).toHaveBeenCalledTimes(6);

      // 'a','b','c' are now fully evicted — 'a' can be sent again
      connector.send('a');
      expect(callback).toHaveBeenCalledTimes(7);

      connector.teardown();
    });

    it('should evict old received refs when maxDedupSetSize is reached', () => {
      const config: SyncConfig = { maxDedupSetSize: 3 };
      const connector = new Connector(db, route, socket, config);

      const notifyCallback = vi.fn();
      (connector as any)._callbacks.push(notifyCallback);

      // Receive 3 refs — fills current generation
      for (const r of ['x', 'y', 'z']) {
        socket.emit(events.ref, {
          o: 'other-origin',
          r,
        } as ConnectorPayload);
      }
      expect(notifyCallback).toHaveBeenCalledTimes(3);

      // Receiving 'x' again should be deduped (in previous generation)
      socket.emit(events.ref, {
        o: 'other-origin',
        r: 'x',
      } as ConnectorPayload);
      expect(notifyCallback).toHaveBeenCalledTimes(3);

      // Receive 3 more — evicts previous generation
      for (const r of ['p', 'q', 'r']) {
        socket.emit(events.ref, {
          o: 'other-origin',
          r,
        } as ConnectorPayload);
      }
      expect(notifyCallback).toHaveBeenCalledTimes(6);

      // 'x' is now fully evicted — can be received again
      socket.emit(events.ref, {
        o: 'other-origin',
        r: 'x',
      } as ConnectorPayload);
      expect(notifyCallback).toHaveBeenCalledTimes(7);

      connector.teardown();
    });

    it('should default to 10_000 when maxDedupSetSize is not set', () => {
      const connector = new Connector(db, route, socket);
      expect((connector as any)._maxDedup).toBe(10_000);
      connector.teardown();
    });
  });

  // =========================================================================
  // onIncomingRef (safe callback via _processIncoming)
  // =========================================================================

  describe('onIncomingRef', () => {
    it('should receive deduplicated incoming refs', () => {
      const connector = new Connector(db, route, socket);

      const callback = vi.fn();
      connector.onIncomingRef(callback);

      // Receive a ref
      socket.emit(events.ref, {
        o: 'other-origin',
        r: 'ref1',
      } as ConnectorPayload);

      expect(callback).toHaveBeenCalledTimes(1);
      expect(callback).toHaveBeenCalledWith('ref1');

      // Receiving the same ref again — should be deduped
      socket.emit(events.ref, {
        o: 'other-origin',
        r: 'ref1',
      } as ConnectorPayload);

      expect(callback).toHaveBeenCalledTimes(1);

      connector.teardown();
    });

    it('should filter self-origin refs', () => {
      const connector = new Connector(db, route, socket);

      const callback = vi.fn();
      connector.onIncomingRef(callback);

      // Emit with connector's own origin — should be filtered
      socket.emit(events.ref, {
        o: connector.origin,
        r: 'ref1',
      } as ConnectorPayload);

      expect(callback).not.toHaveBeenCalled();

      connector.teardown();
    });

    it('should support multiple onIncomingRef callbacks', () => {
      const connector = new Connector(db, route, socket);

      const cb1 = vi.fn();
      const cb2 = vi.fn();
      connector.onIncomingRef(cb1);
      connector.onIncomingRef(cb2);

      socket.emit(events.ref, {
        o: 'other-origin',
        r: 'ref1',
      } as ConnectorPayload);

      expect(cb1).toHaveBeenCalledTimes(1);
      expect(cb2).toHaveBeenCalledTimes(1);

      connector.teardown();
    });

    it('should trigger gap detection when used with causalOrdering', () => {
      const config: SyncConfig = {
        causalOrdering: true,
        includeClientIdentity: true,
      };
      const connector = new Connector(db, route, socket, config);

      const refCallback = vi.fn();
      connector.onIncomingRef(refCallback);

      const gapCallback = vi.fn();
      socket.on(events.gapFillReq, gapCallback);

      const peerId = 'client_Peer1';

      // Receive seq 1
      socket.emit(events.ref, {
        o: 'other-origin',
        r: 'ref1',
        c: peerId,
        seq: 1,
      } as ConnectorPayload);

      // Receive seq 5 — gap
      socket.emit(events.ref, {
        o: 'other-origin',
        r: 'ref5',
        c: peerId,
        seq: 5,
      } as ConnectorPayload);

      expect(refCallback).toHaveBeenCalledTimes(2);
      expect(gapCallback).toHaveBeenCalledTimes(1);

      connector.teardown();
    });
  });

  // =========================================================================
  // sendWithAck with synchronous ACK
  // =========================================================================

  describe('sendWithAck listener ordering', () => {
    it('should resolve when ACK arrives synchronously during send', async () => {
      const config: SyncConfig = { requireAck: true, ackTimeoutMs: 1000 };
      const connector = new Connector(db, route, socket, config);

      // Set up a listener that immediately responds with ACK when ref is sent
      socket.on(events.ref, (payload: ConnectorPayload) => {
        socket.emit(events.ack, {
          r: payload.r,
          ok: true,
          receivedBy: 1,
          totalClients: 1,
        } as AckPayload);
      });

      const result = await connector.sendWithAck(editHistory._hash);
      expect(result.r).toBe(editHistory._hash);
      expect(result.ok).toBe(true);

      connector.teardown();
    });
  });
});
