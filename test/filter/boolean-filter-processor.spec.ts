// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { beforeEach, describe, expect, it } from 'vitest';

import {
  exampleBooleanFilter,
  parseBooleanSearch,
} from '../../src/edit/filter/boolean-filter';
import { BooleanFilterProcessor } from '../../src/edit/filter/boolean-filter-processor';

describe('BooleanFilterProcessor', () => {
  describe('fromModel, example', () => {
    it('creates a new instance', () => {
      const filterProcessor = BooleanFilterProcessor.example;
      expect(filterProcessor.operator).toBe(exampleBooleanFilter().operator);
      expect(filterProcessor.search).toBe(true);
    });
  });

  describe('equals', () => {
    let a0: BooleanFilterProcessor;
    let a1: BooleanFilterProcessor;
    let changedOperator: BooleanFilterProcessor;
    let changedValue: BooleanFilterProcessor;

    beforeEach(() => {
      a0 = new BooleanFilterProcessor('equals', true);
      a1 = new BooleanFilterProcessor('equals', true);
      changedOperator = new BooleanFilterProcessor('notEquals', true);
      changedValue = new BooleanFilterProcessor('equals', false);
    });

    describe('returns true', () => {
      it('when instances are the same', () => {
        expect(a0.equals(a0)).toBe(true);
      });

      it(
        'when instances are not the same, ' + 'but operator and filter',
        () => {
          expect(a0.equals(a1)).toBe(true);
        },
      );
    });

    describe('returns false', () => {
      it('when operator changes', () => {
        expect(a0.equals(changedOperator)).toBe(false);
      });

      it('when value changes', () => {
        expect(a0.equals(changedValue)).toBe(false);
      });
    });
  });

  describe('matches', () => {
    it('with operator equals', () => {
      const filter = new BooleanFilterProcessor('equals', true);
      expect(filter.matches(true)).toBe(true);
      expect(filter.matches(false)).toBe(false);
    });
    it('with operator notEquals', () => {
      const filter = new BooleanFilterProcessor('notEquals', true);
      expect(filter.matches(true)).toBe(false);
      expect(filter.matches(false)).toBe(true);
    });

    it('with search containing various true values', () => {
      const searchVals = [
        'y',
        'ye',
        'yes',
        'yess',
        'j',
        'ja',
        'jaa',
        1,
        2,
        100,
        '1',
        '2',
        '100',
        true,
      ];
      for (const search of searchVals) {
        const filter = new BooleanFilterProcessor('equals', search);
        expect(filter.matches(true)).toBe(true);
        expect(filter.matches(false)).toBe(false);
      }
    });

    it('with search containing various false values', () => {
      const searchVals = [
        'f',
        'fa',
        'fal',
        'fals',
        'false',
        'falsch',
        0,
        '0',
        '00',
        [1, 2, 3],
        false,
      ];
      for (const search of searchVals) {
        const parsedSearch = parseBooleanSearch(search);
        const filter = new BooleanFilterProcessor('equals', parsedSearch);
        expect(filter.matches(true)).toBe(false);
        expect(filter.matches(false)).toBe(true);
      }
    });

    describe('special case: ', () => {
      it('"YES" will be matched as true', () => {
        const filter = new BooleanFilterProcessor('equals', true);
        expect(filter.matches('YES')).toBe(true);
      });

      it('"NO" will be matched as false', () => {
        const filter = new BooleanFilterProcessor('equals', true);
        expect(filter.matches('NO')).toBe(false);
      });

      it('1 will be matched as true', () => {
        const filter = new BooleanFilterProcessor('equals', true);
        expect(filter.matches(1)).toBe(true);
      });

      it('0 will be matched as true', () => {
        const filter = new BooleanFilterProcessor('equals', true);
        expect(filter.matches(0)).toBe(false);
      });

      describe('null will be matched', () => {
        describe('as true', () => {
          it('when search is empty', () => {
            const filter = new BooleanFilterProcessor('equals', null);
            expect(filter.matches(null)).toBe(true);
          });
        });

        describe('as false', () => {
          it('when search is given', () => {
            const filter0 = new BooleanFilterProcessor('equals', true);
            expect(filter0.matches(null)).toBe(false);

            const filter1 = new BooleanFilterProcessor('equals', false);
            expect(filter1.matches(null)).toBe(false);

            const search = parseBooleanSearch(undefined);
            const filter2 = new BooleanFilterProcessor('notEquals', search);
            expect(filter2.matches(null)).toBe(true);

            const search2 = parseBooleanSearch(null);
            const filter3 = new BooleanFilterProcessor('notEquals', search2);
            expect(filter3.matches(null)).toBe(true);
          });
        });
      });
    });
  });
});
