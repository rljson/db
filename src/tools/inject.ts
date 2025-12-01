// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/* v8 ignore next -- @preserve */
export const inject = (tree: any, path: (string | number)[], value: any) => {
  for (let i = 0; i < path.length; i++) {
    const segment = path[i];
    if (i === path.length - 1) {
      tree[segment] = value;
      delete tree['_hash'];
    } else {
      if (!tree[segment]) {
        tree[segment] = {};
      }
      tree = tree[segment];
    }
  }
};
