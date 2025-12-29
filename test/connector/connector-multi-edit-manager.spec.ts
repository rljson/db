// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.
// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';
import { IoMem, Socket, SocketMock } from '@rljson/io';
import { Json } from '@rljson/json';
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
import {
  exampleEditActionColumnSelection,
  exampleEditActionSetValue,
} from '../../src/edit/edit-action';
import { MultiEditManager } from '../../src/edit/multi-edit-manager';
import { staticExample } from '../../src/example-static/example-static';

interface ClientSetup {
  db: Db;
  socket: Socket;
  connector: Connector;
  multiEditManager: MultiEditManager;
  editHistoryRefs: (string | undefined)[];
}

const cakeKey = 'carCake';
const cakeRef = staticExample().carCake._data[2]._hash as string;
const route = Route.fromFlat(`${cakeKey}EditHistory`);

const generateClientSetup = async (): Promise<ClientSetup> => {
  //Init io
  const io = new IoMem();
  await io.init();
  await io.isReady();

  //Init Core
  const db = new Db(io);

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

  //ColumnSelection Edit
  const columnSelectionEdit = hip<Edit>({
    name: 'Select: brand, type, serviceIntervals, isElectric, height, width, length, engine, repairedByWorkshop',
    action: exampleEditActionColumnSelection(),
    _hash: '',
  });
  await db.addEdit(cakeKey, columnSelectionEdit);

  const { [cakeKey + 'EditsRef']: columnSelectionEditRef } = (
    await db.addEdit(cakeKey, columnSelectionEdit)
  )[0] as any;

  const columnSelectionMultiEdit: MultiEdit = hip<MultiEdit>({
    previous: null,
    edit: columnSelectionEditRef!,
    _hash: '',
  });

  const { [cakeKey + 'MultiEditsRef']: columnSelectionMultiEditRef } = (
    await db.addMultiEdit(cakeKey, columnSelectionMultiEdit)
  )[0] as any;

  const { [cakeKey + 'EditHistoryRef']: columnSelectionEditHistoryRef } = (
    await db.addEditHistory(cakeKey, {
      _hash: '',
      dataRef: cakeRef,
      multiEditRef: columnSelectionMultiEditRef,
      timeId: '1765170151918:USbV',
      previous: [],
    } as EditHistory)
  )[0] as any;

  //SetValue Edit
  const setValueEdit = hip<Edit>({
    name: 'Set: Service Intervals to [15000, 30000, 45000, 60000]',
    action: exampleEditActionSetValue(),
    _hash: '',
  });
  await db.addEdit(cakeKey, setValueEdit);

  const { [cakeKey + 'EditsRef']: setValueEditRef } = (
    await db.addEdit(cakeKey, setValueEdit)
  )[0] as any;

  const setValueMultiEdit: MultiEdit = hip<MultiEdit>({
    previous: columnSelectionMultiEditRef!,
    edit: setValueEditRef!,
    _hash: '',
  });

  const { [cakeKey + 'MultiEditsRef']: setValueMultiEditRef } = (
    await db.addMultiEdit(cakeKey, setValueMultiEdit)
  )[0] as any;

  const { [cakeKey + 'EditHistoryRef']: setValueEditHistoryRef } = (
    await db.addEditHistory(cakeKey, {
      _hash: '',
      dataRef: cakeRef,
      multiEditRef: setValueMultiEditRef,
      timeId: '1765170151938:c4I6',
      previous: [columnSelectionEditHistoryRef!],
    } as EditHistory)
  )[0] as any;

  const editHistoryRefs = [
    columnSelectionEditHistoryRef,
    setValueEditHistoryRef,
  ];

  //Instantiate Socket
  const socket = new SocketMock();

  //Connect Connector and MultiEditManager
  const connector = new Connector(db, route, socket);
  const multiEditManager = new MultiEditManager(cakeKey, db);
  multiEditManager.init();
  connector.listen(async (editHistoryRef: string) => {
    await multiEditManager.editHistoryRef(editHistoryRef);
  });

  return {
    db,
    socket,
    connector,
    multiEditManager,
    editHistoryRefs,
  };
};

const wire = (route: Route, clientSetups: ClientSetup[]) => {
  // attach a stable wire id to each socket so we can mark forwarded messages
  const sockets = clientSetups.map((cs, idx) => {
    const s = cs.socket as any;
    s.__wireId = `wire_${idx}_${Math.random().toString(36).slice(2)}`;
    return s;
  });

  for (const socketA of sockets) {
    socketA.on(route.flat, (payload: ConnectorPayload) => {
      const p = payload as any;

      // If payload already has an origin, it was forwarded by the wire and should not be re-forwarded.
      if (p && p.__origin) {
        return;
      }

      for (const socketB of sockets) {
        if (socketA !== socketB) {
          // clone and mark the forwarded payload with the origin to prevent loops
          const forwarded = Object.assign({}, payload, {
            __origin: (socketA as any).__wireId,
          });
          socketB.emit(route.flat, forwarded);
        }
      }
    });
  }
};

describe('Connector/MultiEditManager interoperability', () => {
  let a: ClientSetup;
  let b: ClientSetup;

  beforeEach(async () => {
    a = await generateClientSetup();
    b = await generateClientSetup();
    wire(route, [a, b]);
  });

  describe('interoperability', () => {
    it('Send EditHistoryRefs over wired Sockets', async () => {
      const callback = vi.fn();

      b.connector.listen(callback);

      const { cell: editHistories } = await a.db.get(route, {});
      for (const editHistory of editHistories) {
        const editHistoryRef = (editHistory.row as Json)._hash as string;
        a.connector.send(editHistoryRef);
      }
      expect(callback).toHaveBeenCalledTimes(editHistories.length);
      expect(callback).toHaveBeenCalledWith(
        (editHistories[0].row as Json)._hash as string,
      );
      expect(callback).toHaveBeenCalledWith(
        (editHistories[1].row as Json)._hash as string,
      );
    });
    it('MultiEditManager processes received EditHistoryRefs', async () => {
      const callback = vi.fn();

      //Listen to head changes on b's MultiEditManager
      b.multiEditManager.listenToHeadChanges(callback);

      //Simulate Db write on a --> triggers Connector send
      a.db.notify.notify(route, {
        route: route.flat,
        [cakeKey + 'EditHistoryRef']: a.editHistoryRefs[1] as string,
        timeId: timeId(),
      });

      // Wait for b's MultiEditManager to process the EditHistories
      await vi.waitUntil(
        () => {
          return (
            !!b.multiEditManager.head &&
            !!b.multiEditManager.head.editHistoryRef
          );
        },
        { timeout: 2000, interval: 100 },
      );

      // Verify that the callback was called with the new head editHistoryRef
      expect(callback).toHaveBeenCalled();
      expect(callback).toHaveBeenCalledWith(
        b.multiEditManager.head?.editHistoryRef,
      );

      // Verify MultiEditProcessor state on b's MultiEditManager
      expect(b.multiEditManager.head).toBeDefined();
      expect(b.multiEditManager.head?.editHistoryRef).toBeDefined();

      // Check that both processors are present (columnSelection and setValue)
      expect(
        b.multiEditManager.processors.has(a.editHistoryRefs[1] as string),
      ).toBe(true);
      expect(b.multiEditManager.processors.size).toBe(2);

      const firstProc = b.multiEditManager.processors.get(
        a.editHistoryRefs[0] as string,
      );
      expect(firstProc).toBeDefined();
      expect(firstProc?.edits.length).toBe(1); // ColumnSelection Edit

      // Verify final data state
      const secondProc = b.multiEditManager.processors.get(
        a.editHistoryRefs[1] as string,
      );
      expect(secondProc).toBeDefined();
      expect(secondProc?.edits.length).toBe(2); // ColumnSelection + SetValue Edit

      // Get Join Result
      const join = secondProc?.join;
      expect(join).toBeDefined();

      // Check Result
      expect(join?.rows.length).toBe(12);
      expect(
        join?.rows
          .map((r) => r[2])
          .every(
            (si) =>
              JSON.stringify(si) ==
              JSON.stringify([15000, 30000, 45000, 60000]),
          ),
      ).toBe(true);
    });
  });
});
