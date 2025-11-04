// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { beforeEach, describe, expect, it } from 'vitest';

import { MultiEditResolved } from '../../src/edit/multi-edit-resolved';

describe('MultiEditResolved', () => {
  let current: MultiEditResolved;

  beforeEach(() => {
    current = MultiEditResolved.example;
  });

  describe('fromData, example', () => {
    it('creates a resolved multi edit using the database', () => {
      expect(current).toBeDefined();
    });
  });

  it('hash', () => {
    // expect(current._hash).toBe('QISB0HvV1j_pqLRCBu5Il7');
  });

  describe('edit', () => {
    it('returns the edit', () => {
      const edit = current.edit;
      expect(edit.name).toBe('Set all true to done');
    });
  });

  describe('previous', () => {
    it('returns the previous multi edit or undefined if not existing', () => {
      const p0 = current;
      const p1 = p0.previous;
      const p2 = p1?.previous;
      const p3 = p2?.previous;

      // Defined?
      expect(p0).toBeDefined();
      expect(p1).toBeDefined();
      expect(p2).toBeDefined();
      expect(p3).toBeUndefined();

      // Edit name?
      expect(p0?.edit.name).toBe('Set all true to done');
      expect(p1?.edit.name).toBe('Set all smaller 2 to to true');
      expect(p2?.edit.name).toBe('Set all ending with o to 1234');
    });
  });

  it('example', () => {
    const e = MultiEditResolved.example;
    const e2 = MultiEditResolved.exampleStep0and1and2;
    const e1 = MultiEditResolved.exampleStep0and1;
    const e0 = MultiEditResolved.exampleStep0;

    expect(e2).toEqual(e);
    expect(e1).toEqual(e2.previous);
    expect(e0).toEqual(e1.previous);
  });
});
