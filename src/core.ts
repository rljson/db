// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Io, IoMem } from '@rljson/io';
import { ContentType, Rljson } from '@rljson/rljson';
import { validate } from '@rljson/validate';

/** Implements core functionalities like importing data, setting tables  */
export class Core {
  // ...........................................................................
  constructor(private readonly _io: Io) {}

  static example = async () => {
    return new Core(IoMem.example());
  };

  // ...........................................................................
  /**
   * Creates a table
   * @param name The name of the table.
   * @param type The type of the table.
   */
  async createTable(name: string, type: ContentType): Promise<void> {
    return this._io.createTable({ name, type });
  }

  // ...........................................................................
  /**
   * Returns a dump of the database
   */
  dump(): Promise<Rljson> {
    return this._io.dump();
  }

  /**
   * Returns a dump of a table.
   * @returns a dump of a table.
   * @throws when table name does not exist
   */
  dumpTable(name: string): Promise<Rljson> {
    return this._io.dumpTable({ name });
  }

  // ...........................................................................
  /**
   * Imports data into the memory.
   * @throws {Error} If the data is invalid.
   */
  async import(data: Rljson): Promise<void> {
    // Throw an error if the data is invalid
    const result = validate(data);
    if (result.hasErrors) {
      throw new Error(
        'The imported rljson data is not valid:\n' +
          JSON.stringify(result, null, 2),
      );
    }

    // Write data
    await this._io.write({ data });
  }

  // ...........................................................................
  async tables(): Promise<string[]> {
    return this._io.tables();
  }

  // ...........................................................................
  async hasTable(table: string): Promise<boolean> {
    const tables = await this._io.tables();
    return tables.includes(table);
  }
}
