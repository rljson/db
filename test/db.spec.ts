// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { beforeEach, describe, expect, it } from 'vitest';

import { RljsonDb } from '../src/db';

describe('Db', () => {
  let db: RljsonDb;

  beforeEach(async () => {
    db = await RljsonDb.example();
  });

  describe('core', () => {
    it('should be defined', () => {
      expect(db.core).toBeDefined();
    });
  });
});
