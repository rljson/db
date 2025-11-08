// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Db } from '../db.ts';

import { MultiEdit } from './multi-edit.ts';

export class MultiEditProcessor {
  private _multiEdit: MultiEdit;
  private _cakeKey: string;
  private _cakeRef: string;

  private _db: Db;

  constructor(multiEdit: MultiEdit, cakeKey: string, cakeRef: string, db: Db) {
    this._multiEdit = multiEdit;
    this._cakeKey = cakeKey;
    this._cakeRef = cakeRef;
    this._db = db;
  }
}
