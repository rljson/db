// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { beforeEach, describe, expect, it } from 'vitest';

import { Db } from '../src/db';

describe('Db', () => {
  let db: Db;

  beforeEach(async () => {
    db = await Db.example();
  });

  describe('core', () => {
    it('should be defined', () => {
      expect(db.core).toBeDefined();
    });
  });

  describe('edit', () => {
    it('basic edit', () => {
      expect(db.core).toBeDefined();
    });
  });
});
