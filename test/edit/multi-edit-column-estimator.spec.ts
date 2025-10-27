// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { beforeAll, describe, expect, it } from 'vitest';

import { MultiEditColumnEstimator } from '../../src/edit/edit/multi-edit-column-estimator';
import { MultiEditResolved } from '../../src/edit/edit/multi-edit-resolved';

describe('MultiEditColumnEstimator', () => {
  let columnEstimator: MultiEditColumnEstimator;

  beforeAll(() => {
    columnEstimator = MultiEditColumnEstimator.example;
  });

  describe('columnSelection', () => {
    it('a selection of all columns needed to apply the multi edit', () => {
      expect(columnEstimator.columnSelection.routes).toEqual([
        'basicTypes/stringsRef/value',
        'basicTypes/numbersRef/intsRef/value',
        'basicTypes/numbersRef/floatsRef/value',
        'basicTypes/booleansRef/value',
        'complexTypes/jsonObjectsRef/value',
      ]);

      expect(columnEstimator.columnSelection.aliases).toEqual([
        'value',
        'value1',
        'value2',
        'value3',
        'value4',
      ]);
    });
  });

  describe('cache', () => {
    it('caches the column selection for a multi edit', () => {
      const cache = new Map<string, string[]>();
      const cacheSize = 2;
      const step0 = MultiEditResolved.exampleStep0;
      const step0and1 = MultiEditResolved.exampleStep0and1;
      const step0and1and2 = MultiEditResolved.exampleStep0and1and2;

      // ..........................
      // Apply the first multi edit
      // Create a new column estimator using the cache
      columnEstimator = new MultiEditColumnEstimator(
        MultiEditResolved.exampleStep0,
        cache,
        cacheSize,
      );

      // The cache was not yet used because it was empty
      expect(columnEstimator.cacheUsages).toBe(0);

      // But the result is cached for later reuse.
      const key0 = step0._hash;

      expect(Object.fromEntries(cache)).toEqual({
        [key0]: [
          'basicTypes/stringsRef/value',
          'basicTypes/numbersRef/intsRef/value',
          'basicTypes/numbersRef/floatsRef/value',
        ],
      });

      {
      }

      // ...........................
      // Apply the second multi edit
      columnEstimator = new MultiEditColumnEstimator(
        MultiEditResolved.exampleStep0and1,
        cache,
        cacheSize,
      );

      // The additional step was added to the cache
      const key1 = step0and1._hash;
      expect(Object.fromEntries(cache)).toEqual({
        [key0]: [
          'basicTypes/stringsRef/value',
          'basicTypes/numbersRef/intsRef/value',
          'basicTypes/numbersRef/floatsRef/value',
        ],
        [key1]: [
          'basicTypes/stringsRef/value',
          'basicTypes/numbersRef/intsRef/value',
          'basicTypes/numbersRef/floatsRef/value',
          'basicTypes/booleansRef/value',
        ],
      });

      // The cache was used one time
      expect(columnEstimator.cacheUsages).toBe(1);

      // ..........................
      // Apply the third multi edit
      columnEstimator = new MultiEditColumnEstimator(
        MultiEditResolved.exampleStep0and1and2,
        cache,
        cacheSize,
      );

      // The additional step was added to the cache.
      // But the first step was removed because the cache size is 2.
      const key2 = step0and1and2._hash;
      expect(Object.fromEntries(cache)).toEqual({
        [key1]: [
          'basicTypes/stringsRef/value',
          'basicTypes/numbersRef/intsRef/value',
          'basicTypes/numbersRef/floatsRef/value',
          'basicTypes/booleansRef/value',
        ],
        [key2]: [
          'basicTypes/stringsRef/value',
          'basicTypes/numbersRef/intsRef/value',
          'basicTypes/numbersRef/floatsRef/value',
          'basicTypes/booleansRef/value',
          'complexTypes/jsonObjectsRef/value',
        ],
      });
      expect(columnEstimator.cacheUsages).toBe(1);
    });
  });
});
