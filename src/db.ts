// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Io, IoMem } from '@rljson/io';

import { Core } from './core.ts';

/**
 * Access Rljson data
 */
export class Db {
  /**
   * Constructor
   * @param _io - The Io instance used to read and write data
   */
  constructor(private readonly _io: Io) {
    this.core = new Core(this._io);
  }

  /**
   * Core functionalities like importing data, setting and getting tables
   */
  readonly core: Core;

  /**
   * Example
   * @returns A new Db instance for test purposes
   */
  static example = async () => {
    const io = new IoMem();
    return new Db(io);
  };
}
