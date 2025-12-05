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
  Route,
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

  //SetValue Edit
  const setValueEdit = hip<Edit>({
    name: 'Set: Service Intervals to [15000, 30000, 45000, 60000]',
    action: exampleEditActionSetValue(),
    _hash: '',
  });
  await db.addEdit(cakeKey, setValueEdit);

  //Instantiate MultiEditManager
  const helpingMEM = new MultiEditManager(cakeKey, db);
  helpingMEM.init();
  await helpingMEM.edit(columnSelectionEdit, cakeRef);
  await helpingMEM.edit(setValueEdit);

  //Instantiate Socket
  const socket = new SocketMock();

  //Connect Connector and MultiEditManager
  const connector = new Connector(db, route, socket);
  const multiEditManager = new MultiEditManager(cakeKey, db);
  multiEditManager.init();
  connector.listen((editHistoryRef: string) =>
    multiEditManager.editHistoryRef(editHistoryRef),
  );

  return {
    db,
    socket,
    connector,
    multiEditManager,
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

      b.multiEditManager.listenToHeadChanges(callback);

      const { cell: editHistories } = await a.db.get(route, {});
      for (const editHistory of editHistories) {
        const editHistoryRef = (editHistory.row as Json)._hash as string;
        a.connector.send(editHistoryRef);
      }

      // Wait for b's MultiEditManager to process the EditHistories
      await vi.waitUntil(
        () => {
          return !!b.multiEditManager.head;
        },
        { timeout: 2000, interval: 100 },
      );

      expect(b.multiEditManager.head).toBeDefined();
      expect(b.multiEditManager.head?.editHistoryRef).toBeDefined();
    });
  });
});
