// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { MultiEditResolved } from '../edit/multi-edit-resolved.ts';

import { View } from './view.ts';

/**
 * Caches views e.g. for increasing performance of multi edits.
 */
export class ViewCache {
  constructor(public readonly cacheSize = 5) {}

  /**
   * Write a view to the cache
   * @param master The master view the multi edit belongs to
   * @param multiEdit The multi edit the view should be cached
   * @param processed The view to be cached
   */
  set(master: View, multiEdit: MultiEditResolved, processed: View) {
    const key = this.cacheKey(master, multiEdit);

    if (this._cache.size >= this.cacheSize) {
      const firstCachKey = this._cache.keys().next().value!;
      this._cache.delete(firstCachKey);
    }
    this._cache.set(key, processed);
  }

  /**
   * Get a view from the cache
   * @param master The master view the multi edit is applied to
   * @param multiEdit The multi edit the view should be taken from cache
   * @returns The view from the cache or undefined if not found.
   */
  get(master: View, multiEdit: MultiEditResolved): View | undefined {
    const key = this.cacheKey(master, multiEdit);
    return this._cache.get(key);
  }

  /**
   * Check the key
   * @param master The master view to which the multiEdit is applied
   * @param multiEdit The multiEdit to be applied to the view
   * @returns The cache key to the cached result
   */
  cacheKey(master: View, multiEdit: MultiEditResolved): string {
    return master._hash + '_' + multiEdit._hash;
  }

  // ######################
  // Private
  // ######################

  private _cache = new Map<string, View>();
}
