// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hsh } from '@rljson/hash';
import { Hashed } from '@rljson/json';
import {
  exampleRljson,
  exampleRljsonWithErrors,
  Rljson,
  TableType,
} from '@rljson/rljson';

import { beforeEach, describe, expect, it } from 'vitest';

import { Core } from '../src/core';

describe('Core', () => {
  let core: Core;
  let data: Rljson;
  let dataH: Hashed<Rljson>;

  beforeEach(async () => {
    core = await Core.example();
    core.createTable('table', 'properties');
    data = exampleRljson();
    dataH = hsh(data);
    await core.import(data);
  });

  describe('createTable(name, type)', () => {
    it('creates a table', async () => {
      const tables = await core.tables();
      expect(tables).toEqual(['table']);
    });
  });

  describe('dump()', () => {
    it('returns the complete db content as Rljson', async () => {
      const dump = await core.dump();
      expect(dump).toEqual(dataH);
    });
  });

  describe('dumpTable()', () => {
    it('returns the complete table', async () => {
      const dump = await core.dumpTable('table');
      expect(dump.table).toEqual((dataH as any).table);
    });

    it('throws when the table does not exist', async () => {
      let message: string = '';
      try {
        await core.dumpTable('non-existing-table');
      } catch (error) {
        message = error.message;
      }

      expect(message).toBe('Table non-existing-table not found');
    });
  });

  describe('import(data)', () => {
    it('throws when the data is not valid', async () => {
      let message: string = '';
      try {
        await core.import(exampleRljsonWithErrors());
      } catch (error) {
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
      expect(tables).toEqual(['table']);
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
});

// .............................................................................
const error = `The imported rljson data is not valid:
{
  "tableNamesAreLowerCamelCase": {
    "error": "Table names must be lower camel case",
    "invalidTableNames": [
      "brok$en"
    ]
  },
  "hasErrors": true
}`;
