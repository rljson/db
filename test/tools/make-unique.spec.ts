// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Rljson } from '@rljson/rljson';

import { describe, expect, it } from 'vitest';

import {
  makeUnique,
  makeUniqueArrayByHash,
} from '../../src/tools/make-unique';

describe('makeUniqueArrayByHash', () => {
  it('removes duplicate items by hash, keeping the first occurrence', () => {
    const first = { _hash: 'a', value: 1 };
    const duplicate = { _hash: 'a', value: 1 };
    const other = { _hash: 'b', value: 2 };

    const result = makeUniqueArrayByHash([first, duplicate, other] as any);
    expect(result).toEqual([first, other]);
    expect(result[0]).toBe(first);
  });
});

describe('makeUnique', () => {
  it('removes duplicate rows from all _data arrays', () => {
    const rljson = {
      table: {
        _type: 'components',
        _data: [
          { _hash: 'a', value: 1 },
          { _hash: 'a', value: 1 },
          { _hash: 'b', value: 2 },
        ],
      },
    } as unknown as Rljson;

    const result = makeUnique(rljson);
    expect(result.table._data.map((r: any) => r._hash)).toEqual(['a', 'b']);
  });
});
