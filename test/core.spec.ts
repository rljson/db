// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip, hsh, rmhsh } from '@rljson/hash';
import { IoMem } from '@rljson/io';
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
  let io: IoMem;
  let core: Core;
  let data: Rljson;
  let dataTable: TableType;
  let tableCfg: TableCfg;

  beforeEach(async () => {
    io = await IoMem.example();

    core = new Core(io);
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

  describe('createInsertHistory(table)', () => {
    it('creates an insertHistory table for a given table', async () => {
      await core.createInsertHistory(tableCfg);
      const tables = await core.tables();
      expect(Object.keys(tables)).toEqual([
        '_hash',
        'tableCfgs',
        'revisions',
        'table',
        'tableInsertHistory',
      ]);
    });
  });

  describe('createTableWithInsertHistory(table)', () => {
    it('creates a table and an insertHistory for the table', async () => {
      const newTableCfg: TableCfg = {
        version: 0,
        key: 'newTable',
        type: 'components',
        isHead: false,
        isRoot: false,
        isShared: true,
        columns: [
          {
            titleLong: 'Hash',
            titleShort: 'Hash',
            key: '_hash',
            type: 'string',
          },
          {
            titleLong: 'C',
            titleShort: 'C',
            key: 'c',
            type: 'boolean',
          },
        ],
      };

      await core.createTableWithInsertHistory(newTableCfg);
      const tables = await core.tables();
      expect(Object.keys(tables)).toEqual([
        '_hash',
        'tableCfgs',
        'revisions',
        'table',
        'newTable',
        'newTableInsertHistory',
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
      } catch (error: any) {
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

  describe('contentType(table)', () => {
    it('returns the content type of a table', async () => {
      const contentType = await core.contentType('table');
      expect(contentType).toBe('components');
    });
  });

  describe('tableCfg(table)', () => {
    it('returns the TableCfg of a table', async () => {
      // Test normal case
      const tableCfgResult = await core.tableCfg('table');
      expect(rmhsh(tableCfgResult)).toEqual(rmhsh(tableCfg));

      const tableCfgWithoutRefResult = await core.tableCfg('table');
      expect(rmhsh(tableCfgWithoutRefResult)).toEqual(rmhsh(tableCfg));
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

  describe('tableCfg(table)', () => {
    it('finds tables created by another instance sharing the io', async () => {
      // Warm the config cache
      const warm = await core.tableCfg('table');
      expect(warm).toBeDefined();

      // Create a new table through a SECOND Core sharing the same io —
      // the first Core's cache does not see the creation
      const core2 = new Core(io);
      const externalCfg: TableCfg = {
        version: 0,
        key: 'externalTable',
        type: 'components',
        isHead: false,
        isRoot: false,
        isShared: true,
        columns: [
          { titleLong: 'Hash', titleShort: 'Hash', key: '_hash', type: 'string' },
          { titleLong: 'X', titleShort: 'X', key: 'x', type: 'string' },
        ],
      };
      await core2.createTable(externalCfg);

      // The first Core refetches on miss and finds the new table
      const found = await core.tableCfg('externalTable');
      expect(found).toBeDefined();
      expect(found.key).toBe('externalTable');

      // A truly missing table stays undefined after the refetch
      const missing = await core.tableCfg('doesNotExistAnywhere');
      expect(missing).toBeUndefined();
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

    it('serves repeated reads of the same row from the row cache', async () => {
      const dump = await core.dumpTable('table');
      const rowExpected = (dump.table as TableType)._data[0];
      const rowHash = rowExpected._hash as string;

      const first = await core.readRow('table', rowHash);
      const second = await core.readRow('table', rowHash);
      expect(second).toBe(first);
    });

    it('coalesces concurrent reads of the same row', async () => {
      const dump = await core.dumpTable('table');
      const rowExpected = (dump.table as TableType)._data[0];
      const rowHash = rowExpected._hash as string;

      const [first, second] = await Promise.all([
        core.readRow('table', rowHash),
        core.readRow('table', rowHash),
      ]);
      expect(second).toBe(first);
    });

    it('does not cache misses and evicts oldest rows when full', async () => {
      // A missing row is not cached
      const miss1 = await core.readRow('table', 'MISSING-HASH');
      const miss2 = await core.readRow('table', 'MISSING-HASH');
      expect((miss1.table as TableType)._data.length).toBe(0);
      expect(miss2).not.toBe(miss1);

      // Eviction keeps the cache bounded
      await core.import({
        table: {
          _type: 'components',
          _data: [{ int: 42, string: 'evictionRow', _hash: '' }],
        },
      } as unknown as Rljson);
      const dump = await core.dumpTable('table');
      const rows = (dump.table as TableType)._data;
      expect(rows.length).toBeGreaterThan(1);

      (core as any)._maxRowCacheEntries = 1;
      (core as any)._rowCache.clear();

      await core.readRow('table', rows[0]._hash as string);
      await core.readRow('table', rows[1]._hash as string);
      expect((core as any)._rowCache.size).toBe(1);

      (core as any)._maxRowCacheEntries = 10000;
    });
  });

  describe('readRowsByHashes(table, hashes)', () => {
    it('reads rows in one batch, caches them and skips missing/duplicates', async () => {
      const dump = await core.dumpTable('table');
      const row = (dump.table as TableType)._data[0];
      const hash = row._hash as string;

      const result = await core.readRowsByHashes('table', [
        hash,
        hash,
        'MISSING',
      ]);
      expect(result.size).toBe(1);
      expect(result.get(hash)).toEqual(row);

      // Second call is served from the batch row cache
      const again = await core.readRowsByHashes('table', [hash]);
      expect(again.get(hash)).toBe(result.get(hash));
    });

    it('falls back to per-hash reads when the io lacks batch support', async () => {
      const ioWithoutBatch = Object.create(io);
      ioWithoutBatch.readRowsByHashes = undefined;
      const coreNoBatch = new Core(ioWithoutBatch);

      const dump = await coreNoBatch.dumpTable('table');
      const row = (dump.table as TableType)._data[0];
      const hash = row._hash as string;

      const result = await coreNoBatch.readRowsByHashes('table', [
        hash,
        'MISSING',
      ]);
      expect(result.size).toBe(1);
      expect(result.get(hash)).toEqual(row);
    });

    it('evicts oldest batch rows when the cache is full', async () => {
      await core.import({
        table: {
          _type: 'components',
          _data: [{ int: 77, string: 'batchEvictionRow', _hash: '' }],
        },
      } as unknown as Rljson);

      const dump = await core.dumpTable('table');
      const rows = (dump.table as TableType)._data;
      expect(rows.length).toBeGreaterThan(1);

      (core as any)._maxBatchRowCacheEntries = 1;
      (core as any)._batchRowCache.clear();

      await core.readRowsByHashes('table', [rows[0]._hash as string]);
      await core.readRowsByHashes('table', [rows[1]._hash as string]);
      expect((core as any)._batchRowCache.size).toBe(1);

      (core as any)._maxBatchRowCacheEntries = 10000;
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
                titleLong: 'Hash',
                titleShort: 'Hash',
                key: '_hash',
                type: 'string',
              },
              {
                titleLong: 'A',
                titleShort: 'A',
                key: 'a',
                type: 'boolean',
              },
              {
                titleLong: 'B',
                titleShort: 'B',
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
