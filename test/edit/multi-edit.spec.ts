// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hsh } from '@rljson/hash';

import { describe, expect, it } from 'vitest';

import {
  exampleEditSetAllEndingWithOTo1234,
  exampleEditSetAllStartingWithOToTrue,
  exampleEditSetAllTrueToDone,
  initialEdit,
} from '../../src/edit/edit/edit';
import {
  emptyMultiEdit,
  exampleMultiEdit0,
  exampleMultiEdit1,
  exampleMultiEdit2,
  exampleMultiEditDb,
} from '../../src/edit/edit/multi-edit';

describe('MultiEdit', () => {
  const multiEdit0 = exampleMultiEdit0();
  const multiEdit1 = exampleMultiEdit1();
  const multiEdit2 = exampleMultiEdit2();

  describe('emptyMultiEdit', () => {
    it('has right data', () => {
      const empty = emptyMultiEdit();
      expect(empty).toEqual({
        _hash: empty._hash,
        edit: initialEdit._hash,
        previous: '',
      });
    });
  });

  describe('exampleMultiEdit0', () => {
    it('has right hashes', () => {
      hsh(multiEdit0);
    });

    it('has no previous', () => {
      expect(multiEdit0.previous).toBe('');
    });

    it('sets width of article types starting with UE to 1111', () => {
      expect(multiEdit0.edit).toBe(exampleEditSetAllEndingWithOTo1234()._hash);
    });
  });

  describe('exampleMultiEdit1', () => {
    it('has right hashes', () => {
      hsh(multiEdit1);
    });

    it('follows multi edit 0', () => {
      expect(multiEdit1.previous).toBe(multiEdit0._hash);
    });

    it('sets ok for all articles with a width < 1000 to true', () => {
      expect(multiEdit1.edit).toBe(
        exampleEditSetAllStartingWithOToTrue()._hash,
      );
    });
  });

  describe('exampleMultiEdit2', () => {
    it('has right hashes', () => {
      hsh(multiEdit2);
    });

    it('follows multi edit 1', () => {
      expect(multiEdit2.previous).toBe(multiEdit1._hash);
    });

    it('sets ok for all articles with a width < 1000 to true', () => {
      expect(multiEdit2.edit).toBe(exampleEditSetAllTrueToDone()._hash);
    });
  });

  describe('exampleMultiEditDb', () => {
    it('provides a database of example multi edits', () => {
      const db = exampleMultiEditDb();
      expect(db).toEqual({
        _hash: db._hash,
        [multiEdit0._hash]: multiEdit0,
        [multiEdit1._hash]: multiEdit1,
        [multiEdit2._hash]: multiEdit2,
      });
    });
  });
});
