// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { beforeEach, describe, expect, it } from 'vitest';

import { exampleNumberFilter } from '../../src/edit/filter/number-filter';
import { NumberFilterProcessor } from '../../src/edit/filter/number-filter-processor';

describe('NumberFilterProcessor', () => {
  describe('fromModel, example', () => {
    it('creates a new instance', () => {
      const filterProcessor = NumberFilterProcessor.example;
      expect(filterProcessor.operator).toBe(exampleNumberFilter().operator);
      expect(filterProcessor.search).toBe(1000);
    });
  });

  describe('equals', () => {
    let a0: NumberFilterProcessor;
    let a1: NumberFilterProcessor;
    let changedOperator: NumberFilterProcessor;
    let changedValue: NumberFilterProcessor;

    beforeEach(() => {
      a0 = new NumberFilterProcessor('equals', 42);
      a1 = new NumberFilterProcessor('equals', 42);
      changedOperator = new NumberFilterProcessor('notEquals', 42);
      changedValue = new NumberFilterProcessor('equals', 43);
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
    describe('with operator equals', () => {
      it('and search value 42', () => {
        const filter = new NumberFilterProcessor('equals', 42);
        expect(filter.matches(42)).toBe(true);
        expect(filter.matches(43)).toBe(false);
      });
      it('and search value 0', () => {
        const filter = new NumberFilterProcessor('equals', 0);
        expect(filter.matches(0)).toBe(true);
        expect(filter.matches(1)).toBe(false);
      });
    });
    it('with operator notEquals', () => {
      const filter = new NumberFilterProcessor('notEquals', 42);
      expect(filter.matches(42)).toBe(false);
      expect(filter.matches(43)).toBe(true);
    });
    it('with operator greaterThan', () => {
      const filter = new NumberFilterProcessor('greaterThan', 42);
      expect(filter.matches(41)).toBe(false);
      expect(filter.matches(42)).toBe(false);
      expect(filter.matches(43)).toBe(true);
    });
    it('with operator greaterThanOrEquals', () => {
      const filter = new NumberFilterProcessor('greaterThanOrEquals', 42);
      expect(filter.matches(41)).toBe(false);
      expect(filter.matches(42)).toBe(true);
      expect(filter.matches(43)).toBe(true);
    });
    it('with operator lessThan', () => {
      const filter = new NumberFilterProcessor('lessThan', 42);
      expect(filter.matches(41)).toBe(true);
      expect(filter.matches(42)).toBe(false);
      expect(filter.matches(43)).toBe(false);
    });
    it('with operator lessThanOrEquals', () => {
      const filter = new NumberFilterProcessor('lessThanOrEquals', 42);
      expect(filter.matches(41)).toBe(true);
      expect(filter.matches(42)).toBe(true);
      expect(filter.matches(43)).toBe(false);
    });

    describe('with operator filtrex', () => {
      describe('uses not filtrex but equals operator', () => {
        describe('when an number is given as expression', () => {
          it('with a number in a string', () => {
            const filter = new NumberFilterProcessor('filtrex', '42');
            expect(filter.matches(42)).toBe(true);
            expect(filter.matches(43)).toBe(false);
          });

          it('with a number as number', () => {
            const filter = new NumberFilterProcessor('filtrex', 42);
            expect(filter.matches(42)).toBe(true);
            expect(filter.matches(43)).toBe(false);
          });
        });
      });

      it('returns always true if search is empty', () => {
        const filter = new NumberFilterProcessor('filtrex', '');
        expect(filter.matches(42)).toBe(true);
        expect(filter.matches(43)).toBe(true);
        expect(filter.matches(null)).toBe(true);
      });

      describe('returns always false', () => {
        it('if search is given and value is null', () => {
          const filter = new NumberFilterProcessor('filtrex', '42');
          expect(filter.matches(null)).toBe(false);
        });

        describe('as long expression is not valid', () => {
          it('e.g. the expression contains wrong operators', () => {
            const filter = new NumberFilterProcessor('filtrex', 'x $< 42');
            expect(filter.matches(42)).toBe(false);
            expect(filter.matches(43)).toBe(false);
          });

          it('e.g. the expression contains non existing variables', () => {
            const filter1 = new NumberFilterProcessor('filtrex', 'x < 42 + k');
            expect(filter1.matches(42)).toBe(false);
            expect(filter1.matches(43)).toBe(false);
          });
        });
      });

      describe('evals the expression', () => {
        it('v > 42', () => {
          const filter = new NumberFilterProcessor('filtrex', 'v > 42');
          expect(filter.matches(42)).toBe(false);
          expect(filter.matches(43)).toBe(true);
        });

        it('v >= 42 and v <= 50', () => {
          const filter = new NumberFilterProcessor(
            'filtrex',
            'v >= 42 and v <= 50',
          );
          expect(filter.matches(41)).toBe(false);
          expect(filter.matches(42)).toBe(true);
          expect(filter.matches(43)).toBe(true);
          expect(filter.matches(50)).toBe(true);
          expect(filter.matches(51)).toBe(false);
        });
      });
    });
  });

  describe('special cases', () => {
    it('parses a number in a string', () => {
      const filter = new NumberFilterProcessor('equals', '42');
      expect(filter.matches(42)).toBe(true);
      expect(filter.matches(43)).toBe(false);
    });
  });
});
