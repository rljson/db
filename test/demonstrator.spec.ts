// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { rmhsh } from '@rljson/hash';
import { IoMem } from '@rljson/io';
import { Route, TableCfg } from '@rljson/rljson';

import { readFileSync } from 'node:fs';
import { beforeAll, describe, it } from 'vitest';

import { Db } from '../src/db';
import { RowFilter } from '../src/join/filter/row-filter';
import {
  ColumnInfo,
  ColumnSelection,
} from '../src/join/selection/column-selection';
import { SetValue } from '../src/join/set-value/set-value';

describe('Demonstrator', () => {
  let catalogRljson: any;
  let io: IoMem;
  let db: Db;

  beforeAll(async () => {
    const catalogFs = readFileSync(
      'src/example-converted/catalog-reduced.rljson.json',
      'utf-8',
    );
    catalogRljson = JSON.parse(catalogFs);

    const catalogTableCfgs = { ...catalogRljson.tableCfgs }._data as TableCfg[];

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
