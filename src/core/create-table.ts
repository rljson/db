// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Io } from '@rljson/io';
import { ColumnsConfig } from '@rljson/table';

import { Db } from '../db.ts';

/**
 * Manages creating tables
 */
export class CreateTable {
  constructor(
    private readonly _db: Db,
    private readonly _io: Io,
    private readonly _name: string,
    private readonly _columnsConfig: ColumnsConfig,
  ) {}

  /**
   * Creates the table and resolves the promise when done
   */
  create(): Promise<void> {
    return this._create();
  }

  // ######################
  // Private
  // ######################

  private async _create(): Promise<void> {
    await this._checkExistingTable();
  }

  private async _checkExistingTable(): Promise<void> {
    if (!(await this._db.core.hasTable(this._name))) {
      return;
    }

    const columnConfig = this._io.columnConfig(this._name);
  }
}
