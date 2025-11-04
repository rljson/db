// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';
import { Json } from '@rljson/json';

import {
  exampleEditSetAllEndingWithOTo1234,
  exampleEditSetAllStartingWithOToTrue,
  exampleEditSetAllTrueToDone,
  initialEdit,
} from './edit.ts';

/**
 * An item in an MultiEdit collection of edit items.
 *
 * Realizes the part of a block chain of edits
 */
export interface MultiEdit extends Json {
  /**
   * The hash of the preceeding edit item. Is empty for the first item.
   */
  previous: string;

  /**
   * The hash of the edit or the edit itself applied as part of this item.
   */
  edit: string;

  /**
   * The json hash of the own item.
   */
  _hash: string;
}

// .............................................................................
/**
 * An empty multi edit
 * which sets the width of all articles starting with UEs to 1111
 */
export const emptyMultiEdit = (): MultiEdit =>
  hip({
    previous: '',
    edit: initialEdit._hash,
    _hash: '',
  });

// .............................................................................
/**
 * The first multi edit,
 * which sets all rows starting with O to 1234
 */
export const exampleMultiEdit0 = (): MultiEdit =>
  hip({
    previous: '',
    edit: exampleEditSetAllEndingWithOTo1234()._hash,
    _hash: '',
  });

/**
 * The second multi edit sets all items smaller 2 to true
 */
export const exampleMultiEdit1 = (): MultiEdit =>
  hip({
    previous: exampleMultiEdit0()._hash,
    edit: exampleEditSetAllStartingWithOToTrue()._hash,
    _hash: '',
  });

/**
 * The second multi edit sets all true items to done.
 */
export const exampleMultiEdit2 = (): MultiEdit =>
  hip({
    previous: exampleMultiEdit1()._hash,
    edit: exampleEditSetAllTrueToDone()._hash,
    _hash: '',
  });

export const exampleMultiEditDb = (): Record<string, MultiEdit> => {
  const a = exampleMultiEdit0();
  const b = exampleMultiEdit1();
  const c = exampleMultiEdit2();

  return hip({
    _hash: '' as any,
    [a._hash]: a,
    [b._hash]: b,
    [c._hash]: c,
  });
};
