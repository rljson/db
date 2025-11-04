// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { beforeEach, describe, expect, it } from 'vitest';

import { exampleStringFilter } from '../../../src/join/filter/string-filter';
import { StringFilterProcessor } from '../../../src/join/filter/string-filter-processor';


describe('StringFilterProcessor', () => {
  describe('fromModel, example', () => {
    it('creates a new instance', () => {
      const filterProcessor = StringFilterProcessor.example;
      const stringFilter = exampleStringFilter();
      expect(filterProcessor.operator).toBe(stringFilter.operator);
      expect(filterProcessor.search).toBe('t');
      expect(filterProcessor.matchCase).toBe(stringFilter.matchCase);
    });
  });

  describe('equals', () => {
    let a0: StringFilterProcessor;
    let a1: StringFilterProcessor;
    let changedOperator: StringFilterProcessor;
    let changedValue: StringFilterProcessor;
    let changedMatchCase: StringFilterProcessor;

    beforeEach(() => {
      a0 = new StringFilterProcessor('equals', '42', true);
      a1 = new StringFilterProcessor('equals', '42', true);
      changedOperator = new StringFilterProcessor('notEquals', '42', true);
      changedValue = new StringFilterProcessor('equals', '43', true);
      changedMatchCase = new StringFilterProcessor('equals', '42', false);
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

      it('when matchCase changes', () => {
        expect(a0.equals(changedMatchCase)).toBe(false);
      });
    });
  });

  describe('matches', () => {
    it('returns true when search value is empty', () => {
      const filter = new StringFilterProcessor('equals', '');
      expect(filter.matches('foo')).toBe(true);
      expect(filter.matches('')).toBe(true);
    });
    describe('with matchCase true', () => {
      const matchCase = true;

      it('with operator equals', () => {
        const filter = new StringFilterProcessor('equals', 'foo', matchCase);
        expect(filter.matches('foo')).toBe(true);
        expect(filter.matches('bar')).toBe(false);
      });

      it('with operator equals', () => {
        const filter = new StringFilterProcessor('equals', 'Foo', matchCase);
        expect(filter.matches('foo')).toBe(false);
        expect(filter.matches('bar')).toBe(false);
      });

      it('with operator notEquals', () => {
        const filter = new StringFilterProcessor('notEquals', 'foo', matchCase);
        expect(filter.matches('foo')).toBe(false);
        expect(filter.matches('bar')).toBe(true);
      });
      it('with operator contains', () => {
        const filter = new StringFilterProcessor('contains', 'foo', matchCase);
        expect(filter.matches('foo')).toBe(true);
        expect(filter.matches('foobar')).toBe(true);
        expect(filter.matches('bar')).toBe(false);
      });
      it('with operator notContains', () => {
        const filter = new StringFilterProcessor(
          'notContains',
          'foo',
          matchCase,
        );
        expect(filter.matches('foo')).toBe(false);
        expect(filter.matches('foobar')).toBe(false);
        expect(filter.matches('bar')).toBe(true);
      });
      it('with operator startsWith', () => {
        const filter = new StringFilterProcessor(
          'startsWith',
          'foo',
          matchCase,
        );
        expect(filter.matches('foo')).toBe(true);
        expect(filter.matches('foobar')).toBe(true);
        expect(filter.matches('bar')).toBe(false);
      });
      it('with operator endsWith', () => {
        const filter = new StringFilterProcessor('endsWith', 'foo', matchCase);
        expect(filter.matches('foo')).toBe(true);
        expect(filter.matches('foobar')).toBe(false);
        expect(filter.matches('bar')).toBe(false);
      });
      it('with operator regExp', () => {
        const filter = new StringFilterProcessor('regExp', '^foo', matchCase);
        expect(filter.matches('foo')).toBe(true);
        expect(filter.matches('foobar')).toBe(true);
        expect(filter.matches('bar')).toBe(false);
        expect(filter.matches('$^$([\n\\')).toBe(false);
      });
    });

    describe('with matchCase false', () => {
      const noCaseMatch = false;
      it('with operator equals', () => {
        const filter = new StringFilterProcessor('equals', 'foo', noCaseMatch);
        expect(filter.matches('foo')).toBe(true);
        expect(filter.matches('bar')).toBe(false);
        expect(filter.matches('Foo')).toBe(true);
      });

      it('with operator notEquals', () => {
        const filter = new StringFilterProcessor(
          'notEquals',
          'foo',
          noCaseMatch,
        );
        expect(filter.matches('foo')).toBe(false);
        expect(filter.matches('bar')).toBe(true);
        expect(filter.matches('Foo')).toBe(false);
      });
      it('with operator contains', () => {
        const filter = new StringFilterProcessor(
          'contains',
          'foo',
          noCaseMatch,
        );
        expect(filter.matches('foo')).toBe(true);
        expect(filter.matches('foobar')).toBe(true);
        expect(filter.matches('bar')).toBe(false);
        expect(filter.matches('Foo')).toBe(true);
      });
      it('with operator notContains', () => {
        const filter = new StringFilterProcessor('notContains', 'foo', false);
        expect(filter.matches('foo')).toBe(false);
        expect(filter.matches('foobar')).toBe(false);
        expect(filter.matches('bar')).toBe(true);
        expect(filter.matches('Foo')).toBe(false);
      });
      it('with operator startsWith', () => {
        const filter = new StringFilterProcessor('startsWith', 'foo', false);
        expect(filter.matches('foo')).toBe(true);
        expect(filter.matches('foobar')).toBe(true);
        expect(filter.matches('bar')).toBe(false);
        expect(filter.matches('Foo')).toBe(true);
      });
      it('with operator endsWith', () => {
        const filter = new StringFilterProcessor(
          'endsWith',
          'foo',
          noCaseMatch,
        );
        expect(filter.matches('foo')).toBe(true);
        expect(filter.matches('foobar')).toBe(false);
        expect(filter.matches('bar')).toBe(false);
        expect(filter.matches('Foo')).toBe(true);
      });
      it('with operator regExp', () => {
        const filter = new StringFilterProcessor('regExp', '^foo', noCaseMatch);
        expect(filter.matches('foo')).toBe(true);
        expect(filter.matches('foobar')).toBe(true);
        expect(filter.matches('bar')).toBe(false);
        expect(filter.matches('Foo')).toBe(true);
      });
    });

    describe('special case: ', () => {
      it('numbers will be converted to string before matching', () => {
        const filter = new StringFilterProcessor('equals', '42');
        expect(filter.matches(42)).toBe(true);
        expect(filter.matches(43)).toBe(false);
      });

      it('boolean will be converted to string before matching', () => {
        const filter = new StringFilterProcessor('equals', 'true');
        expect(filter.matches(true)).toBe(true);
        expect(filter.matches(false)).toBe(false);
      });

      it(
        'if an search is given and value is null, ' +
          'it will be matched as false',
        () => {
          const filter = new StringFilterProcessor('equals', 'foo');
          expect(filter.matches(null)).toBe(false);
        },
      );

      it('if search is empty, null will be matched as true', () => {
        const filter = new StringFilterProcessor('equals', '');
        expect(filter.matches(null)).toBe(true);
      });

      describe('spaces are removed before matching', () => {
        it('to match things like "30 cm" and "30cm', () => {
          const filter = new StringFilterProcessor('equals', '   30 cm ');
          expect(filter.matches('30cm')).toBe(true);
          expect(filter.matches('30 cm')).toBe(true);
          expect(filter.matches(' 30 cm ')).toBe(true);
        });
      });
    });
  });
});
