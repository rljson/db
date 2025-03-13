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
export class RljsonDb {
  /**
   * Constructor
   * @param io - The Io instance used to read and write data
   */
  constructor(private readonly io: Io) {}

  /**
   * Core functionalities like importing data, setting and getting tables
   */
  readonly core = new Core(this.io);

  /**
   * Example
   * @returns A new RljsonDb instance for test purposes
   */
  static example = async () => {
    const io = new IoMem();
    return new RljsonDb(io);
  };
}
