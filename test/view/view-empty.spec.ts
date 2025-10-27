// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { beforeAll, describe, expect, it } from 'vitest';

import { ViewEmpty } from '../../src/edit/view/view-empty';

describe.skip('ViewEmpty', () => {
  let viewEmpty: ViewEmpty;

  beforeAll(() => {
    viewEmpty = new ViewEmpty();
  });

  it('example', () => {
    expect(ViewEmpty.example).toBeDefined();
  });

  describe('rows', () => {
    it('returns an empty array', () => {
      expect(viewEmpty.rows).toEqual([]);
    });
  });

  describe('rowCount', () => {
    it('returns 0', () => {
      expect(viewEmpty.rowCount).toBe(0);
    });
  });

  describe('columnCount', () => {
    it('returns 0', () => {
      expect(viewEmpty.columnCount).toBe(0);
    });
  });

  describe('_hash', () => {
    it('returns an empty string', () => {
      expect(viewEmpty._hash).toBe('blnGkibzshLf9iIxFoUWhj');
    });
  });
});
