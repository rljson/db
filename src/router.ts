// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Db } from './db.ts';

export class Router {
  // ...........................................................................
  constructor(private readonly _db: Db) {}

  // ...........................................................................
  /**
   * Runs an edit on the database
   * @param edit - The edit to run
   * @throws when the table does not exist
   * @throws when the edit is invalid
   */
  // async route(edit: Edit<any>): Promise<EditProtocolRow<any>> {
  //   //return this._core.run(edit);
  // }
}
