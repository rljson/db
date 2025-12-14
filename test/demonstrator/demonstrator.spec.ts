// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip, rmhsh } from '@rljson/hash';
import {
  Io,
  IoMem,
  IoMulti,
  IoMultiIo,
  IoPeer,
  PeerSocketMock,
  Socket,
  SocketMock,
} from '@rljson/io';
import { Edit, Route, TableCfg } from '@rljson/rljson';

import { readFileSync, writeFileSync } from 'node:fs';
import { afterAll, beforeAll, describe, it } from 'vitest';

import { Connector, ConnectorPayload } from '../../src/connector/connector';
import { Db } from '../../src/db';
import {
  EditActionColumnSelection,
  EditActionRowFilter,
  EditActionSetValue,
} from '../../src/edit/edit-action';
import { MultiEditManager } from '../../src/edit/multi-edit-manager';
import { RowFilter } from '../../src/join/filter/row-filter';
import {
  ColumnInfo,
  ColumnSelection,
} from '../../src/join/selection/column-selection';
import { SetValue } from '../../src/join/set-value/set-value';

interface Client {
  db: Db;
  socket: Socket;
  connector: Connector;
  multiEditManager: MultiEditManager;
}

const client = async (
  cakeKey: string,
  io: Io,
  cache: Map<string, any>,
): Promise<Client> => {
  //Create Client Db with MultiIo and Cache
  const clientDb = new Db(io);
  clientDb.setCache(cache);

  //Instantiate Socket
  const socket = new SocketMock();

  //Create MultiEditManager - Manages MultiEdits over Connector/Socket
  const connector = new Connector(
    clientDb,
    Route.fromFlat(cakeKey + 'EditHistory'),
    socket,
  );
  const multiEditManager = new MultiEditManager(cakeKey, clientDb);
  multiEditManager.init();

  connector.listen(async (editHistoryRef: string) => {
    await multiEditManager.editHistoryRef(editHistoryRef);
  });

  return {
    db: clientDb,
    socket: socket,
    connector: connector,
    multiEditManager: multiEditManager,
  };
};

const wire = (route: Route, clients: Client[]) => {
  const sockets = clients.map((cs, idx) => {
    const s = cs.socket as any;
    s.__wireId = `wire_${idx}_${Math.random().toString(36).slice(2)}`;
    return s;
  });

  for (const socketA of sockets) {
    socketA.on(route.flat, async (payload: ConnectorPayload) => {
      const p = payload as any;

      if (p && p.__origin) {
        return;
      }

      for (const socketB of sockets) {
        if (socketA !== socketB) {
          const forwarded = Object.assign({}, payload, {
            __origin: (socketA as any).__wireId,
          });
          // Ensure emit completes before continuing
          await socketB.emit(route.flat, forwarded);
        }
      }
    });
  }
};

describe('Demonstrator', () => {
  let catalogRljson: any;
  let catalogTableCfgs: TableCfg[];
  let cache: Map<string, any>;
  let io: IoMem;
  let db: Db;

  beforeAll(async () => {
    const catalogFs = readFileSync(
      'src/example-converted/catalog.rljson.json',
      'utf-8',
    );
    catalogRljson = JSON.parse(catalogFs);
    catalogTableCfgs = { ...catalogRljson.tableCfgs }._data as TableCfg[];

    delete catalogRljson.tableCfgs;

    //Init io
    io = new IoMem();
    await io.init();
    await io.isReady();

    //Init Core
    db = new Db(io);

    //Initialize Catalog
    for (const tableCfg of catalogTableCfgs) {
      await db.core.createTable(tableCfg);
    }

    //Import Data
    await db.core.import(rmhsh(catalogRljson));

    //Load Cache if existing
    try {
      const cacheFs = readFileSync(
        __dirname + '/demonstrator-cache.json',
        'utf-8',
      );
      const cacheObj: [string, any][] = JSON.parse(cacheFs);

      cache = new Map<string, any>(cacheObj);
      db.setCache(cache);

      console.log('Demonstrator Cache loaded.');
    } catch (e) {
      console.log('No Demonstrator Cache found.', e);
    }
  });

  afterAll(async () => {
    const cache = JSON.stringify(Array.from(db.cache.entries()));
    writeFileSync(__dirname + '/demonstrator-cache.json', cache, 'utf-8');
  });

  describe('Modeling', async () => {
    // Modellierung von Katalogen als Cakes
    //   Laysers (Schichten) -> Z.B. Preise, Titel, Grafische Daten, Kaufmännische Daten
    //   Slices (Stücke) -> Z.B. ein Artikel,
    //   SliceId -> Artikel-Type
    it('Structures Data models horizontally into layers', () => {
      //........................................................................
      //SliceId articleType
      //Primary Keys of Articles
      //........................................................................
      const articleType = catalogRljson.articleSliceId;
      console.log('Article Types:', articleType);

      //........................................................................
      //Component Article Text
      //Plain List of Article Texts
      //........................................................................
      const articleTexts = catalogRljson.articleArticleText;
      console.log('Article Texts:', articleTexts);

      //........................................................................
      //Layer Article Text
      //Relation Article Type -> Article Text
      //........................................................................
      const articleTextLayer = catalogRljson.articleArticleTextLayer;
      console.log('Article Text Layer:', articleTextLayer);

      //........................................................................
      //Cake Article
      //Combines all Layers to complete Articles
      //........................................................................
      const articleCake = catalogRljson.articleCake;
      console.log('Article Cake:', articleCake);
    });
  });
  describe('Requests', async () => {
    //  Abfragen:
    //    Komplette Schichten eines Katalogs können rausgezogen werden
    //    Einzelne Artikel können rausgezogen werden
    //    Abruf von kombinierten Ansichten aus mehreren Schichten
    it('Get complete Layers of Catalog', async () => {
      const articleTextRoute = Route.fromFlat(
        '/articleCake/articleArticleTextLayer/articleArticleText/articleText',
      );
      const articleTexts = await db.get(articleTextRoute, {});

      // Get only plain data
      const articlePlainTexts = articleTexts.cell.flatMap((c) => c.value);
      console.log('Article Texts Data', articlePlainTexts);

      // Get complete rljson structure
      console.log('Article Texts Rljson', articleTexts.rljson);

      // Get tree structure
      console.log('Article Texts Tree', articleTexts.tree);
    });
    it('Get specific Article Text from single Article Type', async () => {
      const singleArticleType = 'HILIGHTH16IX100';
      const singleArticleTextRoute = Route.fromFlat(
        `/articleCake(${singleArticleType})/articleArticleTextLayer/articleArticleText/articleText`,
      );
      const singleArticle = await db.get(singleArticleTextRoute, {});

      // Get only plain data
      const articlePlainTexts = singleArticle.cell.flatMap((c) => c.value);
      console.log('Specific Article Text Data', articlePlainTexts);

      // Get specific route to article text
      const articleRoute = singleArticle.cell.flatMap((c) => c.route.flat);
      console.log('Specific Article Text Route', articleRoute);

      // Get complete rljson structure
      console.log('Specific Article Text Rljson', singleArticle.rljson);

      // Get tree structure
      console.log('Specific Article Text Tree', singleArticle.tree);
    });
    it('Get combined Join from multiple Layers', async () => {
      // Get Cake Reference
      const cakeKey = '/articleCake';
      const cakeRef = (
        (await db.get(Route.fromFlat(cakeKey), {})).cell[0].row as any
      )._hash;

      // Define Columns to select
      const columnInfos: ColumnInfo[] = [
        {
          key: 'text',
          alias: 'text',
          route:
            'articleCake/articleArticleTextLayer/articleArticleText/articleText',
          titleLong: 'Article Text describing main aspects',
          titleShort: 'Article Text',
          type: 'string',
        },
        {
          key: 'depth',
          alias: 'depth',
          route:
            'articleCake/articleBasicShapeLayer/articleBasicShape/articleBasicShapeDepth/d',
          titleLong: 'Article Basic Shape Depth in cm',
          titleShort: 'Article Basic Shape Depth',
          type: 'number',
        },
        {
          key: 'length',
          alias: 'length',
          route:
            'articleCake/articleBasicShapeLayer/articleBasicShape/articleBasicShapeLength/l',
          titleLong: 'Article Basic Shape Length in cm',
          titleShort: 'Article Basic Shape Length',
          type: 'number',
        },
        {
          key: 'width',
          alias: 'width',
          route:
            'articleCake/articleBasicShapeLayer/articleBasicShape/articleBasicShapeWidth/w',
          titleLong: 'Article Basic Shape Width in cm',
          titleShort: 'Article Basic Shape Width',
          type: 'number',
        },
      ];
      const selection = new ColumnSelection(columnInfos);

      // Execute Join
      const join = await db.join(selection, 'articleCake', cakeRef);
      const rows = join.rows.map((r) => r.flat());

      console.log('Join Result Rows:', rows);
    }, 30000);
  });
  describe('Editing on joined Data', async () => {
    //  Bearbeitung von Daten:
    //    Massenbearbeitung auf Basis von Ansichten
    it('Modify mass data on combined Join from multiple Layers', async () => {
      // Get Cake Reference
      const cakeKey = '/articleCake';
      const cakeRef = (
        (await db.get(Route.fromFlat(cakeKey), {})).cell[0].row as any
      )._hash;

      // Define Columns to select
      const columnInfos: ColumnInfo[] = [
        {
          key: 'text',
          alias: 'text',
          route:
            'articleCake/articleArticleTextLayer/articleArticleText/articleText',
          titleLong: 'Article Text describing main aspects',
          titleShort: 'Article Text',
          type: 'string',
        },
        {
          key: 'depth',
          alias: 'depth',
          route:
            'articleCake/articleBasicShapeLayer/articleBasicShape/articleBasicShapeDepth/d',
          titleLong: 'Article Basic Shape Depth in cm',
          titleShort: 'Article Basic Shape Depth',
          type: 'number',
        },
        {
          key: 'length',
          alias: 'length',
          route:
            'articleCake/articleBasicShapeLayer/articleBasicShape/articleBasicShapeLength/l',
          titleLong: 'Article Basic Shape Length in cm',
          titleShort: 'Article Basic Shape Length',
          type: 'number',
        },
        {
          key: 'width',
          alias: 'width',
          route:
            'articleCake/articleBasicShapeLayer/articleBasicShape/articleBasicShapeWidth/w',
          titleLong: 'Article Basic Shape Width in cm',
          titleShort: 'Article Basic Shape Width',
          type: 'number',
        },
      ];
      const selection = new ColumnSelection(columnInfos);

      // Execute Join
      const join = await db.join(selection, 'articleCake', cakeRef);

      // Modify Data
      const setValue: SetValue = {
        route:
          'articleCake/articleBasicShapeLayer/articleBasicShape/articleBasicShapeWidth/w',
        value: 999,
      };

      join.setValue(setValue);
      const rows = join.rows.map((r) => r.flat());

      console.log('Join Modified Result Rows:', rows);
    });
  });
  describe('EditHistory', async () => {
    // Führung einer Änderungshierarchie
    //   Liste von non-destruktiven Massenbearbeitungsoperationen
    //   Jeder Stand des Katalogs kann geholt werden
    it('Define a List of MultiEdits and let it non-destructively modify Data', async () => {
      // Get Cake Reference
      const cakeKey = 'articleCake';
      const cakeRef = (
        (await db.get(Route.fromFlat(cakeKey), {})).cell[0].row as any
      )._hash;

      // Define MultiEdit Manager
      const multiEditManager = new MultiEditManager(cakeKey, db);

      //....................................................................
      // Define Edit 0: Select Columns
      //........................................................
      const selectionEdit: Edit = hip<Edit>({
        name: 'Select Columns: Article Text, Depth, Length, Width',
        action: {
          type: 'selection',
          data: {
            columns: [
              {
                key: 'text',
                alias: 'text',
                route:
                  'articleCake/articleArticleTextLayer/articleArticleText/articleText',
                titleLong: 'Article Text describing main aspects',
                titleShort: 'Article Text',
                type: 'string',
              },
              {
                key: 'depth',
                alias: 'depth',
                route:
                  'articleCake/articleBasicShapeLayer/articleBasicShape/articleBasicShapeDepth/d',
                titleLong: 'Article Basic Shape Depth in cm',
                titleShort: 'Article Basic Shape Depth',
                type: 'number',
              },
              {
                key: 'length',
                alias: 'length',
                route:
                  'articleCake/articleBasicShapeLayer/articleBasicShape/articleBasicShapeLength/l',
                titleLong: 'Article Basic Shape Length in cm',
                titleShort: 'Article Basic Shape Length',
                type: 'number',
              },
              {
                key: 'width',
                alias: 'width',
                route:
                  'articleCake/articleBasicShapeLayer/articleBasicShape/articleBasicShapeWidth/w',
                titleLong: 'Article Basic Shape Width in cm',
                titleShort: 'Article Basic Shape Width',
                type: 'number',
              },
            ] as ColumnInfo[],
          },
        } as EditActionColumnSelection,
        _hash: '',
      });

      //...............................................................
      // Define Edit 2: Filter to Article Text containing "HILIGHT"
      //...............................................................
      const filterEdit: Edit = hip<Edit>({
        name: 'Filter to Article Text containing HILIGHT',
        action: {
          name: 'Filter to Article Text containing HILIGHT',
          type: 'filter',
          data: {
            columnFilters: [
              {
                column:
                  'articleCake/articleArticleTextLayer/articleArticleText/articleText',
                operator: 'contains',
                type: 'string',
                search: 'CARLO NOBILI',
                _hash: '',
              },
            ],
            operator: 'and',
            _hash: '',
          } as RowFilter,
        } as EditActionRowFilter,
        _hash: '',
      });

      //........................................
      // Define Edit 3: Set Width to 111
      //........................................
      const setValueEdit: Edit = hip<Edit>({
        name: 'Set Width to 111',
        action: {
          name: 'Set Width to 111',
          type: 'setValue',
          data: {
            route:
              'articleCake/articleBasicShapeLayer/articleBasicShape/articleBasicShapeWidth/w',
            value: 111,
          } as SetValue,
        } as EditActionSetValue,
        _hash: '',
      });

      //........................................
      // Execute Edits
      //........................................

      // Apply Selection Edit
      await multiEditManager.edit(selectionEdit, cakeRef);

      const selectedRows = multiEditManager.join.rows;
      console.log(
        'Selected Rows:',
        selectedRows.map((r) => r.flat()),
      );

      // Apply Filter Edit
      await multiEditManager.edit(filterEdit);

      const filteredRows = multiEditManager.join.rows;
      console.log(
        'Filtered Rows:',
        filteredRows.map((r) => r.flat()),
      );

      // Apply Set Value Edit
      await multiEditManager.edit(setValueEdit);

      const modifiedRows = multiEditManager.join.rows;
      console.log(
        'Modified Rows:',
        modifiedRows.map((r) => r.flat()),
      );

      // Get Edit History
      const editHistory = (await db.getEditHistories(cakeKey, {}))[0];
      console.log('Edit History:', editHistory);

      // Load previous Edit History into new MultiEditManager
      const newMultiEditManager = new MultiEditManager(cakeKey, db);
      await newMultiEditManager.editHistoryRef(editHistory._hash);

      const latestRows = newMultiEditManager.join.rows;
      console.log(
        'Latest EditHistory Rows:',
        latestRows.map((r) => r.flat()),
      );
    });
  });
  describe('High Availability', async () => {
    // Hochverfügbarkeit von Daten
    //   Angabe von mehrere Herkunftsquellen (Speicher, lokale Platte, Cloud, benachbarte Rechner)
    //   Priorisierte Abfrage von Daten -> Speicher, Platte, Benachbarte Rechner, Cloud
    //   Automatisches Caching von Daten in lokalen Datenspeichern
    //   Offlinefähigkeit bei vorherigem Download der Daten
    //   Reduktion der Cloud-Last
    let localIo: Io;
    let remoteIo: Io;
    let multiIo: Io;
    let multiDb: Db;

    beforeAll(async () => {
      //.....................................................
      // Local Io as Cache
      // In-Memory Io acting as local Cache
      //.....................................................
      localIo = new IoMem();
      await localIo.init();
      await localIo.isReady();

      const localDb = new Db(localIo);
      //Initialize Tables only
      for (const tableCfg of catalogTableCfgs) {
        await localDb.core.createTable(tableCfg);
      }

      //.....................................................
      // Peer Socket Mock as Remote Io
      // Socket based Io to another Peer holding its own Io
      //.....................................................
      remoteIo = new IoPeer(new PeerSocketMock(io));
      await remoteIo.init();
      await remoteIo.isReady();

      //.....................................................
      // MultiIo with Local Io as Cache and Remote Io as Source
      //.....................................................
      const ios: IoMultiIo[] = [
        {
          io: localIo,
          read: true,
          write: true,
          dump: true,
          priority: 1,
        },
        {
          io: remoteIo,
          read: true,
          write: false,
          dump: true,
          priority: 2,
        },
      ];

      multiIo = new IoMulti(ios);
      multiDb = new Db(multiIo);
    });

    it('Access Data over MultiIo with local Cache and remote Source', async () => {
      // Initial Access - Data is fetched from local Io
      const {
        ['articleArticleText']: { _data: localArticleTexts },
      } = await localIo.dumpTable({
        table: 'articleArticleText',
      });
      console.log(
        'Local MultiIo Article Texts Data before access:',
        localArticleTexts,
      );

      // Initial Access - Data is fetched from remote Io
      const {
        ['articleArticleText']: { _data: remoteArticleTexts },
      } = await remoteIo.dumpTable({
        table: 'articleArticleText',
      });
      console.log(
        'Remote MultiIo Article Texts Data before access:',
        remoteArticleTexts,
      );

      // ......................................................
      // Access Data over MultiIo - Data is fetched from remote Io and cached locally
      // ......................................................
      const articleTextsThroughDb = await multiDb.get(
        Route.fromFlat(
          '/articleCake/articleArticleTextLayer/articleArticleText/articleText',
        ),
        {},
      );
      const articlePlainTexts = articleTextsThroughDb.cell.flatMap(
        (c) => c.value,
      );
      console.log('MultiIo Article Texts Data through Db:', articlePlainTexts);

      // ......................................................
      // Subsequent Access - Data is now fetched from local Io
      // AND is cached in local Db
      // ......................................................
      const {
        ['articleArticleText']: { _data: newLocalArticleTexts },
      } = await localIo.dumpTable({
        table: 'articleArticleText',
      });
      console.log(
        'Local MultiIo Article Texts Data after access:',
        newLocalArticleTexts,
      );
    });
  });
  describe('Collaboration', async () => {
    const cakeKey = 'articleCake';
    let cakeRef: string;

    let a: Client, b: Client;

    beforeAll(async () => {
      cakeRef = ((await db.get(Route.fromFlat(cakeKey), {})).cell[0].row as any)
        ._hash;

      a = await client(cakeKey, io, cache || new Map<string, any>());
      b = await client(cakeKey, io, cache || new Map<string, any>());

      wire(Route.fromFlat('/articleCakeEditHistory'), [a, b]);
    });

    it('Define Edits on Client A and wired Client B will receive and execute it automatically', async () => {
      // Client A makes an selection Edit
      const selectionEditA: Edit = hip<Edit>({
        name: 'Client A selects Columns: Article Text, Depth, Length, Width',
        action: {
          type: 'selection',
          data: {
            columns: [
              {
                key: 'text',
                alias: 'text',
                route:
                  'articleCake/articleArticleTextLayer/articleArticleText/articleText',
                titleLong: 'Article Text describing main aspects',
                titleShort: 'Article Text',
                type: 'string',
              },
              {
                key: 'depth',
                alias: 'depth',
                route:
                  'articleCake/articleBasicShapeLayer/articleBasicShape/articleBasicShapeDepth/d',
                titleLong: 'Article Basic Shape Depth in cm',
                titleShort: 'Article Basic Shape Depth',
                type: 'number',
              },
              {
                key: 'length',
                alias: 'length',
                route:
                  'articleCake/articleBasicShapeLayer/articleBasicShape/articleBasicShapeLength/l',
                titleLong: 'Article Basic Shape Length in cm',
                titleShort: 'Article Basic Shape Length',
                type: 'number',
              },
              {
                key: 'width',
                alias: 'width',
                route:
                  'articleCake/articleBasicShapeLayer/articleBasicShape/articleBasicShapeWidth/w',
                titleLong: 'Article Basic Shape Width in cm',
                titleShort: 'Article Basic Shape Width',
                type: 'number',
              },
            ] as ColumnInfo[],
          },
        } as EditActionColumnSelection,
        _hash: '',
      });
      await a.multiEditManager.edit(selectionEditA, cakeRef);

      // Give time for the message to propagate through the wire
      await new Promise((resolve) => setTimeout(resolve, 100));

      // Client A makes an filter Edit
      const filterEditA: Edit = hip<Edit>({
        name: 'Client A filters to Article Text containing CARLO NOBILI',
        action: {
          name: 'Filter to Article Text containing CARLO NOBILI',
          type: 'filter',
          data: {
            columnFilters: [
              {
                column:
                  'articleCake/articleArticleTextLayer/articleArticleText/articleText',
                operator: 'contains',
                type: 'string',
                search: 'CARLO NOBILI',
                _hash: '',
              },
            ],
            operator: 'and',
            _hash: '',
          } as RowFilter,
        } as EditActionRowFilter,
        _hash: '',
      });
      await a.multiEditManager.edit(filterEditA);

      await new Promise((resolve) => setTimeout(resolve, 100));

      // Client A makes an setValue Edit
      const setValueEditA: Edit = hip<Edit>({
        name: 'Client A sets Width to 222',
        action: {
          name: 'Set Width to 222',
          type: 'setValue',
          data: {
            route:
              'articleCake/articleBasicShapeLayer/articleBasicShape/articleBasicShapeWidth/w',
            value: 222,
          } as SetValue,
        } as EditActionSetValue,
        _hash: '',
      });
      await a.multiEditManager.edit(setValueEditA);

      await new Promise((resolve) => setTimeout(resolve, 100));

      const modifiedRowsB = b.multiEditManager.join.rows;
      console.log(
        'Client B Modified Rows after Client A Edits:',
        modifiedRowsB.map((r) => r.flat()),
      );
      debugger;
    });
  });

  describe('High Performance Caching', async () => {
    let cachedDb: Db;

    beforeAll(async () => {
      cachedDb = new Db(io);
    });

    it('Second get will be really fast', async () => {
      const requestRoute = Route.fromFlat(
        '/articleCake/articleArticleTextLayer/articleArticleText/articleText',
      );

      const startFirst = Date.now();
      const firstGet = await cachedDb.get(requestRoute, {});
      const endFirst = Date.now();

      const startSecond = Date.now();
      const secondGet = await cachedDb.get(requestRoute, {});
      const endSecond = Date.now();

      console.log(`First Get took ${endFirst - startFirst} ms`);
      console.log(`Second Get took ${endSecond - startSecond} ms`);

      // Verify both gets return the same data
      const firstData = firstGet.cell.flatMap((c) => c.value);
      const secondData = secondGet.cell.flatMap((c) => c.value);

      console.log(
        'First and Second Get return same data:',
        JSON.stringify(firstData) === JSON.stringify(secondData),
      );
    });
  });

  describe('Publishing', async () => {
    // Publishing von Änderungen
    //   Änderungsprotokoll wird in die Daten geschrieben
    //   Neue Artikel, Schicht, Serien, Katalog und Hersteller Revisionen werden angelegt
    //   Änderungen können differentiell erfolgen: D.h. nur die geänderten Slices werden gespeichert. Vorstand wird geerbt
    it('Publishing Data from Join and lookup on resulting changes', async () => {
      // Get Cake Reference
      const cakeKey = '/articleCake';
      const cakeRef = (
        (await db.get(Route.fromFlat(cakeKey), {})).cell[0].row as any
      )._hash;

      // Define Columns to select
      const columnInfos: ColumnInfo[] = [
        {
          key: 'text',
          alias: 'text',
          route:
            'articleCake/articleArticleTextLayer/articleArticleText/articleText',
          titleLong: 'Article Text describing main aspects',
          titleShort: 'Article Text',
          type: 'string',
        },
        {
          key: 'depth',
          alias: 'depth',
          route:
            'articleCake/articleBasicShapeLayer/articleBasicShape/articleBasicShapeDepth/d',
          titleLong: 'Article Basic Shape Depth in cm',
          titleShort: 'Article Basic Shape Depth',
          type: 'number',
        },
        {
          key: 'length',
          alias: 'length',
          route:
            'articleCake/articleBasicShapeLayer/articleBasicShape/articleBasicShapeLength/l',
          titleLong: 'Article Basic Shape Length in cm',
          titleShort: 'Article Basic Shape Length',
          type: 'number',
        },
        {
          key: 'width',
          alias: 'width',
          route:
            'articleCake/articleBasicShapeLayer/articleBasicShape/articleBasicShapeWidth/w',
          titleLong: 'Article Basic Shape Width in cm',
          titleShort: 'Article Basic Shape Width',
          type: 'number',
        },
      ];
      const selection = new ColumnSelection(columnInfos);

      // Execute Join
      const join = await db.join(selection, 'articleCake', cakeRef);

      // Filter Data
      const filter: RowFilter = {
        columnFilters: [
          {
            column:
              'articleCake/articleBasicShapeLayer/articleBasicShape/articleBasicShapeWidth/w',
            operator: 'greaterThan',
            type: 'number',
            search: 500,
            _hash: '',
          },
        ],
        operator: 'and',
        _hash: '',
      };

      // Modify Data
      const setValue: SetValue = {
        route:
          'articleCake/articleArticleTextLayer/articleArticleText/articleText',
        value: 'Ich bin echt breit.',
      };

      join.filter(filter).setValue(setValue);
      const rows = join.rows.map((r) => r.flat());

      console.log('Join Modified Result Rows:', rows);

      // Insert Join --> Publish Data
      const { route, tree } = join.insert()[0];
      await db.insert(route, tree);

      //Lookup Article Text Insert History
      const articleTextInserts = await io.dumpTable({
        table: 'articleArticleTextInsertHistory',
      });
      console.log('articleArticleTextInsertHistory:', articleTextInserts);

      //Lookup Article Text Layer Insert History
      const articleTextLayerInserts = await io.dumpTable({
        table: 'articleArticleTextLayerInsertHistory',
      });
      console.log(
        'articleArticleTextLayerInsertHistory:',
        articleTextLayerInserts,
      );

      //Lookup Article Cake Insert History
      const articleCakeInserts = await io.dumpTable({
        table: 'articleCakeInsertHistory',
      });
      console.log('articleCakeInsertHistory:', articleCakeInserts);
    });
  });
});
