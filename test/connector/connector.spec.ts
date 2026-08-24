// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';
import { IoMem, Socket, SocketMock } from '@rljson/io';
import {
  createEditHistoryTableCfg,
  createEditTableCfg,
  createMultiEditTableCfg,
  Edit,
  EditHistory,
  MultiEdit,
  Route,
  timeId,
} from '@rljson/rljson';

import { beforeEach, describe, expect, it, vi } from 'vitest';

import { Connector, ConnectorPayload } from '../../src/connector/connector';
import { Db } from '../../src/db';
import { exampleEditActionColumnSelection } from '../../src/edit/edit-action';
import { staticExample } from '../../src/example-static/example-static';

// Ref-callback assertions check the REF rather than the whole call signature.
// The callback gained a third argument (arrival info) and would otherwise need
// every assertion rewritten each time it grows.
describe('Connector', () => {
  let db: Db;

  let editHistory: EditHistory;
  let edit: Edit;
  let multiEdit: MultiEdit;

  const cakeKey = 'carCake';
  const cakeRef = staticExample().carCake._data[2]._hash as string;
  const route = Route.fromFlat(`${cakeKey}EditHistory`);

  let socket: Socket;
  let connector: Connector;

  beforeEach(async () => {
    //Init io
    const io = new IoMem();
    await io.init();
    await io.isReady();

    //Init Core
    db = new Db(io);

    //Create Tables for TableCfgs in carsExample
    for (const tableCfg of staticExample().tableCfgs._data) {
      await db.core.createTableWithInsertHistory(tableCfg);
    }

    //Create Tables for Edit TableCfgs
    await db.core.createTable(createMultiEditTableCfg(cakeKey));
    await db.core.createTable(createEditTableCfg(cakeKey));
    await db.core.createTable(createEditHistoryTableCfg(cakeKey));

    //Import Data
    await db.core.import(staticExample());

    //Instantiate Socket
    socket = new SocketMock();

    //Instantiate Connector
    connector = new Connector(db, route, socket);

    const editActionColumnSelection = exampleEditActionColumnSelection();
    edit = hip<Edit>({
      name: 'Select: brand, type, serviceIntervals, isElectric, height, width, length, engine, repairedByWorkshop',
      action: editActionColumnSelection,
      _hash: '',
    });

    const { [cakeKey + 'EditsRef']: editRef } = (
      await db.addEdit(cakeKey, edit)
    )[0] as any;

    multiEdit = hip<MultiEdit>({
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

  describe('listen', () => {
    it('should initialize listening state', () => {
      expect(connector.isListening).toBe(true);
    });
    it('should listen for new Socket events', async () => {
      const callback = vi.fn();
      const origin = timeId();

      //Start listening
      connector.listen(callback);

      //Emit new EditHistory
      const payload = {
        r: editHistory._hash,
        o: origin,
      } as ConnectorPayload;
      socket.emit(route.flat, payload);

      expect(callback).toHaveBeenCalled();
      expect(callback.mock.calls.at(-1)?.[0]).toBe(payload.r);
    });

    it('should replay missed ref that arrived before listen()', async () => {
      const callback = vi.fn();
      const origin = timeId();

      // Emit BEFORE any callback is registered — simulates bootstrap race
      const payload = {
        r: editHistory._hash,
        o: origin,
      } as ConnectorPayload;
      socket.emit(route.flat, payload);

      // Callback not yet registered — should not have been called
      expect(callback).not.toHaveBeenCalled();

      // Now register the callback — missed ref should be replayed
      connector.listen(callback);

      // Allow microtask to settle (replay uses Promise.resolve)
      await new Promise((r) => setTimeout(r, 0));

      expect(callback).toHaveBeenCalledTimes(1);
      expect(callback.mock.calls.at(-1)?.[0]).toBe(editHistory._hash);
    });

    it('re-opens a missed ref that a later arrival replaced, so a return to its state is deliverable', async () => {
      const origin = timeId();

      // Two refs arrive before any listener exists. The slot holds one, so
      // the second replaces the first — correct, the newer ref describes the
      // newer state. But BOTH were marked received on arrival, and only the
      // survivor is ever delivered.
      const replaced = 'replacedBootstrapRef';
      const survivor = editHistory._hash;
      socket.emit(route.flat, { r: replaced, o: origin } as ConnectorPayload);
      socket.emit(route.flat, { r: survivor, o: origin } as ConnectorPayload);

      const callback = vi.fn();
      connector.listen(callback);
      await new Promise((r) => setTimeout(r, 0));

      // Only the survivor is replayed — the replaced ref is not resurrected.
      expect(callback).toHaveBeenCalledTimes(1);
      expect(callback.mock.calls.at(-1)?.[0]).toBe(survivor);

      // But the replaced ref must no longer count as "already received".
      // Refs are content hashes: a peer returning to that exact state
      // re-derives that exact ref, and it has to reach the listener. Left
      // marked, the state is unreachable for the life of the connector.
      callback.mockClear();
      socket.emit(route.flat, { r: replaced, o: origin } as ConnectorPayload);
      await new Promise((r) => setTimeout(r, 0));
      expect(callback.mock.calls.at(-1)?.[0]).toBe(replaced);

      // The survivor still describes the current state, so re-advertising it
      // is an echo and stays suppressed.
      callback.mockClear();
      socket.emit(route.flat, { r: survivor, o: origin } as ConnectorPayload);
      await new Promise((r) => setTimeout(r, 0));
      expect(callback).not.toHaveBeenCalled();
    });

    it('should not replay missed ref on second listen()', async () => {
      const origin = timeId();

      // Emit before any callback
      const payload = {
        r: editHistory._hash,
        o: origin,
      } as ConnectorPayload;
      socket.emit(route.flat, payload);

      // First listen() consumes the missed ref
      const cb1 = vi.fn();
      connector.listen(cb1);
      await new Promise((r) => setTimeout(r, 0));
      expect(cb1).toHaveBeenCalledTimes(1);

      // Second listen() should NOT replay (already consumed)
      const cb2 = vi.fn();
      connector.listen(cb2);
      await new Promise((r) => setTimeout(r, 0));
      expect(cb2).not.toHaveBeenCalled();
    });

    it('should replay missed ref even after send()', async () => {
      const origin = timeId();

      // Bootstrap arrives before any callback
      const payload = {
        r: 'old-bootstrap-ref',
        o: origin,
      } as ConnectorPayload;
      socket.emit(route.flat, payload);

      // syncToDb sends fresher local state — but must NOT discard
      // the bootstrap. listen() still needs it so syncFromDb can
      // restore the server's tree.
      connector.send('fresh-local-ref');

      // listen() SHOULD replay the bootstrap ref
      const cb = vi.fn();
      connector.listen(cb);
      await new Promise((r) => setTimeout(r, 0));
      expect(cb).toHaveBeenCalledTimes(1);
      expect(cb.mock.calls.at(-1)?.[0]).toBe('old-bootstrap-ref');
    });
  });

  describe('send', () => {
    it('should send new Socket events', async () => {
      const callback = vi.fn();

      //Listen for emitted event
      socket.on(route.flat, callback);

      connector.send(editHistory._hash);

      expect(callback).toHaveBeenCalled();
      expect(callback).toHaveBeenCalledWith({
        o: connector.origin,
        r: editHistory._hash,
      });
    });

    it('should not send already sent refs', async () => {
      const callback = vi.fn();

      //Listen for emitted event
      socket.on(route.flat, callback);

      connector.send(editHistory._hash);
      connector.send(editHistory._hash); //Send again

      expect(callback).toHaveBeenCalledTimes(1);
      expect(callback).toHaveBeenCalledWith({
        o: connector.origin,
        r: editHistory._hash,
      });
    });

    it('should not send received refs', async () => {
      const callback = vi.fn();

      //Listen for emitted event
      socket.on(route.flat, callback);

      //Simulate receiving the ref
      (connector as any)._receivedRefsCurrent.add(editHistory._hash);

      connector.send(editHistory._hash);

      expect(callback).not.toHaveBeenCalled();
    });
  });

  describe('invalidateSent', () => {
    it('lets a folder that returns to an earlier state broadcast it again', async () => {
      // Content-addressed state means A → B → A re-derives A's exact ref, and
      // send() drops it as a duplicate. A file created and then deleted inside
      // one session is exactly that shape; the deletion reached no peer.
      const sent: string[] = [];
      socket.on(route.flat, (p: ConnectorPayload) => sent.push(p.r as string));

      connector.send('state-a');
      expect(sent).toEqual(['state-a']);

      connector.send('state-b');
      expect(sent).toEqual(['state-a', 'state-b']);

      // Returning to state-a is swallowed: send() has seen this ref before.
      connector.send('state-a');
      expect(sent).toEqual(['state-a', 'state-b']);

      // Moving off a ref makes a later return to it real news again.
      connector.invalidateSent('state-a');
      connector.send('state-a');
      expect(sent).toEqual(['state-a', 'state-b', 'state-a']);
    });

    it('also clears the received set, since send() consults both', () => {
      const callback = vi.fn();
      connector.listen(callback);
      const origin = timeId();
      socket.emit(route.flat, {
        r: editHistory._hash,
        o: origin,
      } as ConnectorPayload);
      expect(callback).toHaveBeenCalledTimes(1);

      const sent: string[] = [];
      socket.on(route.flat, (p: ConnectorPayload) => sent.push(p.r as string));
      // A ref this connector RECEIVED is equally undeliverable via send(),
      // so clearing only the sent set would leave the same hole.
      connector.send(editHistory._hash);
      expect(sent).toEqual([]);

      connector.invalidateSent(editHistory._hash);
      connector.send(editHistory._hash);
      expect(sent).toEqual([editHistory._hash]);
    });

    it('is a no-op for a ref that was never sent', () => {
      expect(() => connector.invalidateSent('never-sent-ref')).not.toThrow();
    });
  });

  describe('invalidateReceived', () => {
    it('allows a received ref to be re-delivered (heartbeat recovery)', async () => {
      const callback = vi.fn();
      const origin = timeId();
      connector.listen(callback);

      const payload = {
        r: editHistory._hash,
        o: origin,
      } as ConnectorPayload;

      // First delivery — callback fires, ref enters the received-dedup set.
      socket.emit(route.flat, payload);
      expect(callback).toHaveBeenCalledTimes(1);

      // A re-advertisement (e.g. the server's bootstrap heartbeat re-sending
      // the latest ref) is suppressed as a duplicate — this is what makes a
      // failed apply permanent.
      socket.emit(route.flat, payload);
      expect(callback).toHaveBeenCalledTimes(1);

      // After invalidating, the same ref is delivered again so the heartbeat
      // can re-trigger the apply once the transient condition clears.
      connector.invalidateReceived(editHistory._hash);
      socket.emit(route.flat, payload);
      expect(callback).toHaveBeenCalledTimes(2);
    });

    it('is a no-op for a ref that was never received', () => {
      // Must not throw and must not affect later delivery of a fresh ref.
      expect(() => connector.invalidateReceived('never-seen-ref')).not.toThrow();

      const callback = vi.fn();
      connector.listen(callback);
      socket.emit(route.flat, {
        r: editHistory._hash,
        o: timeId(),
      } as ConnectorPayload);
      expect(callback).toHaveBeenCalledTimes(1);
    });
  });

  describe('registerDbObserver', () => {
    it('should register Db observer for new EditHistory additions', async () => {
      const callback = vi.fn();

      socket.on(route.flat, callback);

      await db.addEditHistory(cakeKey, editHistory);

      expect(callback).toHaveBeenCalled();
      expect(callback).toHaveBeenCalledWith({
        o: connector.origin,
        r: editHistory._hash,
      });
    });
  });

  describe('tearDown', () => {
    it('should tearDown connector and stop listening', () => {
      connector.tearDown();

      expect(connector.isListening).toBe(false);
    });
  });
});
