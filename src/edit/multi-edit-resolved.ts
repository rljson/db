// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hsh } from '@rljson/hash';

import { Edit, exampleEditDb, initialEdit } from './edit.ts';
import {
  emptyMultiEdit,
  exampleMultiEdit2,
  exampleMultiEditDb,
  MultiEdit,
} from './multi-edit.ts';

/**
 * An item in an MultiEdit collection of edit items.
 *
 * Realizes the part of a block chain of edits
 */
export class MultiEditResolved {
  /**
   * Constructor
   * @param previous The previous item in the chain or undefined if this is the first item.
   * @param edit The edit to be applied as part of this step.
   * @param data The data
   */
  constructor(
    public readonly previous: MultiEditResolved | undefined,
    public readonly edit: Edit,
    public readonly data: MultiEdit,
  ) {
    const previousHash = previous ? previous._hash : '';

    this._hash = hsh({
      previous: previousHash,
      edit: edit._hash,
      _hash: '',
    })._hash;
  }

  // ...........................................................................
  static fromData(
    multiEdit: MultiEdit,
    multiEditDb: Record<string, MultiEdit>,
    editDb: Record<string, Edit>,
  ): MultiEditResolved {
    // Resolve previous item
    const previousUnresolved = multiEdit.previous
      ? multiEditDb[multiEdit.previous]
      : undefined;

    const previousResolved = previousUnresolved
      ? MultiEditResolved.fromData(previousUnresolved, multiEditDb, editDb)
      : undefined;

    // Resolve edit
    const editResolved = editDb[multiEdit.edit];

    // Create result object
    const result: MultiEditResolved = new MultiEditResolved(
      previousResolved,
      editResolved,
      multiEdit,
    );

    // Return result
    return result;
  }

  // ...........................................................................
  /**
   * The hash of the edit item.
   */
  _hash: string;

  // ...........................................................................
  static get empty(): MultiEditResolved {
    return new MultiEditResolved(undefined, initialEdit, emptyMultiEdit());
  }

  // ...........................................................................
  /**
   * An example multi edit with two predecessors.
   */
  static get example(): MultiEditResolved {
    return MultiEditResolved.fromData(
      exampleMultiEdit2(),
      exampleMultiEditDb(),
      exampleEditDb(),
    );
  }

  /**
   * The third step of the example multi edit.
   */
  static get exampleStep0and1and2(): MultiEditResolved {
    return this.example;
  }

  /**
   * The second step of the example multi edit.
   */
  static get exampleStep0and1(): MultiEditResolved {
    return this.exampleStep0and1and2.previous!;
  }

  /**
   * The first step of the example multi edit.
   */
  static get exampleStep0(): MultiEditResolved {
    return this.exampleStep0and1.previous!;
  }
}
