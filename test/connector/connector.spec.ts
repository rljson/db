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
      expect(callback).toHaveBeenCalledWith(payload.r);
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
      (connector as any)._receivedRefs.add(editHistory._hash);

      connector.send(editHistory._hash);

      expect(callback).not.toHaveBeenCalled();
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

  describe('teardown', () => {
    it('should teardown connector and stop listening', () => {
      connector.teardown();

      expect(connector.isListening).toBe(false);
    });
  });
});
