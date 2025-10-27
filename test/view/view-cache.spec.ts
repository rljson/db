// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { beforeEach, describe, expect, it } from 'vitest';

import { MultiEditResolved } from '../../src/edit/edit/multi-edit-resolved';
import { View } from '../../src/edit/view/view';
import { ViewCache } from '../../src/edit/view/view-cache';

describe.skip('ViewCache', () => {
  let cache: ViewCache;
  let _cache: Map<string, any>;
  const view0: any = {};
  const view1: any = {};
  const view2: any = {};

  const multiEdit2 = MultiEditResolved.example;
  const multiEdit1 = multiEdit2.previous!;
  const multiEdit0 = multiEdit1.previous!;

  const master = {
    _hash: 'hash0',
  } as View;

  beforeEach(() => {
    cache = new ViewCache(2);
    _cache = (cache as any)._cache as Map<string, any>;
  });

  const allKeys = () => {
    return Array.from(_cache.keys());
  };

  describe('add', () => {
    it('adds the view to the cache', () => {
      const _cache = (cache as any)._cache as Map<string, any>;
      expect(_cache).toBeDefined();

      const view: any = {};
      cache.set(master, multiEdit0, view);
      expect(cache.get(master, multiEdit0)).toBe(view);
      const hash = multiEdit0._hash;

      // Get all keys from cache
      expect(allKeys()).toEqual([`hash0_${hash}`]);
    });

    it('removes the oldes key if cache is full', () => {
      const _cache = (cache as any)._cache as Map<string, any>;
      expect(_cache).toBeDefined();

      cache.set(master, multiEdit0, view0);
      cache.set(master, multiEdit1, view1);
      cache.set(master, multiEdit2, view2);

      expect(cache.get(master, multiEdit0)).toBeUndefined();
      expect(cache.get(master, multiEdit1)).toBe(view1);
      expect(cache.get(master, multiEdit2)).toBe(view2);
    });
  });
});
