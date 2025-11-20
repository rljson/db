// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { JsonH } from '@rljson/json';
import { Rljson } from '@rljson/rljson';

import { traverse } from 'object-traversal';

export const makeUniqueArrayByHash = <T extends JsonH>(arr: T[]): T[] => {
  const seen = new Map<string, T>();
  const result: T[] = [];
  for (const item of arr) {
    if (!seen.has(item._hash)) {
      seen.set(item._hash, item);
      result.push(item);
    }
  }
  return result;
};

export const makeUnique = (rljson: Rljson): Rljson => {
  traverse(rljson, ({ parent, key, value }) => {
    if (key == '_data' && Array.isArray(value)) {
      parent![key] = makeUniqueArrayByHash<JsonH>(value);
    }
  });
  return rljson;
};
