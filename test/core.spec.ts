// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip, hsh, rmhsh } from '@rljson/hash';
import { JsonArray, JsonValue } from '@rljson/json';
import {
  Example,
  exampleRljson,
  exampleTableCfgTable,
  Rljson,
  TableCfg,
  TablesCfgTable,
  TableType,
} from '@rljson/rljson';

import { traverse } from 'object-traversal';
import { beforeEach, describe, expect, it } from 'vitest';

import { Core } from '../src/core';

describe('Core', () => {
  let core: Core;
  let data: Rljson;
  let dataTable: TableType;
  let tableCfg: TableCfg;

  beforeEach(async () => {
    core = await Core.example();
    tableCfg = exampleTableCfgTable()._data[0];
    await core.createTable(tableCfg);

    data = exampleRljson();
    await core.import(data);

    dataTable = hsh(data.table) as TableType;
    traverse(dataTable, ({ parent, key, value }) => {
      if (key === 'null' && !value) {
        delete parent![key];
      }
    });
  });

  describe('createTable(name, type)', () => {
    it('creates a table', async () => {
      const tables = await core.tables();
      expect(Object.keys(tables)).toEqual([
        '_hash',
        'tableCfgs',
        'revisions',
        'table',
      ]);
    });
  });

  describe('dump()', () => {
    it('returns the complete db content as Rljson', async () => {
      const dump = await core.dump();
      const dumpTable = dump.table;
      expect(dumpTable).toEqual(dataTable);
    });
  });

  describe('dumpTable()', () => {
    it('returns the complete table', async () => {
      const dump = await core.dumpTable('table');
      expect(dump.table).toEqual(dataTable);
    });

    it('throws when the table does not exist', async () => {
      let message: string = '';
      try {
        await core.dumpTable('non-existing-table');
      } catch (error) {
        message = error.message;
      }

      expect(message).toBe('Table "non-existing-table" not found');
    });
  });

  describe('import(data)', () => {
    it('throws when the data is not valid', async () => {
      let message: string = '';
      const broken = Example.broken.base.brokenTableKey() as Rljson;
      try {
        await core.import(broken);
      } catch (error: any) {
        message = error.message;
      }

      expect(message).toBe(error);
    });

    it('writes the data into the IO', async () => {
      // Was tested in beforeEach
    });
  });

  describe('tables()', () => {
    it('returns the list of tables', async () => {
      const tables = await core.tables();
      expect(Object.keys(tables)).toEqual([
        '_hash',
        'tableCfgs',
        'revisions',
        'table',
      ]);
    });
  });

  describe('hasTable(table)', () => {
    it('returns true if the table exists', async () => {
      const result = await core.hasTable('table');
      expect(result).toBe(true);

      const result2 = await core.hasTable('non-existing-table');
      expect(result2).toBe(false);
    });
  });

  describe('readRow(table, rowHash)', () => {
    it('returns a specific row from a database table', async () => {
      const dump = await core.dumpTable('table');
      const rowExpected = (dump.table as TableType)._data[0];
      const rowHash = rowExpected._hash as string;

      const result = await core.readRow('table', rowHash);
      expect((result.table as any)._data[0]).toEqual(rowExpected);
    });
  });

  describe('readRows(table, where)', () => {
    beforeEach(async () => {
      const binaryTableCfgs = hip<TablesCfgTable>({
        _hash: '',
        _type: 'tableCfgs',
        _data: [
          {
            version: 0,
            _hash: '',
            key: 'table',
            type: 'components',
            isHead: false,
            isRoot: false,
            isShared: true,
            columns: [
              {
                key: '_hash',
                type: 'string',
              },
              {
                key: 'a',
                type: 'boolean',
              },
              {
                key: 'b',
                type: 'boolean',
              },
            ],
          },
        ],
      });
      core = await Core.example();
      tableCfg = binaryTableCfgs._data[0];
      await core.createTable(tableCfg);

      data = Example.ok.binary();
      await core.import(data);
    });

    const readRows = async (where: {
      [column: string]: JsonValue;
    }): Promise<JsonArray> => {
      const result = rmhsh(await core.readRows('table', where));
      return (result.table as any)._data;
    };

    it('returns rows from a database table', async () => {
      expect(await readRows({ a: false })).toEqual([
        { a: false, b: true },
        { a: false, b: false },
      ]);

      expect(await readRows({ a: true })).toEqual([
        { a: true, b: true },
        { a: true, b: false },
      ]);

      expect(await readRows({ a: false, b: false })).toEqual([
        { a: false, b: false },
      ]);
    });
  });
});

// .............................................................................
const error = `The imported rljson data is not valid:
{
  "base": {
    "hasErrors": true,
    "tableKeysNotLowerCamelCase": {
      "error": "Table names must be lower camel case",
      "invalidTableKeys": [
        "brok$en"
      ]
    }
  }
}`;
