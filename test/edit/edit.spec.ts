// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip, hsh } from '@rljson/hash';

import { describe, expect, it } from 'vitest';

import {
  exampleEdit,
  exampleEditDb,
  exampleEditSetAllEndingWithOTo1234,
  exampleEditSetAllStartingWithOToTrue,
  exampleEditSetAllTrueToDone,
  initialEdit,
} from '../../src/edit/edit';
import { exampleEditAction } from '../../src/edit/edit-action';
import { exampleRowFilter } from '../../src/join/filter/row-filter';

describe('Edit', () => {
  it('exampleEdit', () => {
    const edit = exampleEdit();

    hsh(edit);

    expect(edit).toEqual({
      name: 'Example Step',
      filter: exampleRowFilter(),
      actions: [exampleEditAction()],
      _hash: edit._hash,
    });
  });

  it('exampleEditSetAllTrueToDone', () => {
    const edit = hip(exampleEditSetAllTrueToDone());
    expect(edit).toEqual(exampleEditSetAllTrueToDone());
  });

  it('exampleEditSetAllStartingWithOToTrue', () => {
    const edit = hip(exampleEditSetAllStartingWithOToTrue());
    expect(edit).toEqual(exampleEditSetAllStartingWithOToTrue());
  });

  it('exampleEditSetAllEndingWithOTo1234', () => {
    const edit = hip(exampleEditSetAllEndingWithOTo1234());
    expect(edit).toEqual(exampleEditSetAllEndingWithOTo1234());
  });

  it('exampleEditDb', () => {
    expect(exampleEditDb()).toEqual({
      [exampleEditSetAllTrueToDone()._hash]: exampleEditSetAllTrueToDone(),
      [exampleEditSetAllStartingWithOToTrue()._hash]:
        exampleEditSetAllStartingWithOToTrue(),
      [exampleEditSetAllEndingWithOTo1234()._hash]:
        exampleEditSetAllEndingWithOTo1234(),
    });
  });

  it('emptyEdit', () => {
    expect(initialEdit).toEqual({
      _hash: 'tECxfcI5st91r1s3fXhzqO',
      actions: [],
      filter: {
        _hash: '25AByTYsD1UbZNLHnUowUy',
        columnFilters: [],
        operator: 'and',
      },
      name: '',
    });
  });
});
