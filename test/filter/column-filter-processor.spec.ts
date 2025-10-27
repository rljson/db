// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, expect, it } from 'vitest';

import { BooleanFilterProcessor } from '../../src/edit/filter/boolean-filter-processor';
import { ColumnFilterProcessor } from '../../src/edit/filter/column-filter-processor';
import { NumberFilterProcessor } from '../../src/edit/filter/number-filter-processor';
import { exampleStringFilter } from '../../src/edit/filter/string-filter';
import { StringFilterProcessor } from '../../src/edit/filter/string-filter-processor';

describe('ColumnFilterProcessor', () => {
  describe('fromModel', () => {
    it('with StringFilter', () => {
      const processor = ColumnFilterProcessor.fromModel(exampleStringFilter());
      expect(processor).toBeInstanceOf(StringFilterProcessor);
    });
  });

  describe('operatorsForType', () => {
    it('string', () => {
      expect(ColumnFilterProcessor.operatorsForType('string')).toEqual([
        'contains',
        'equals',
        'notEquals',
        'startsWith',
        'notContains',
        'endsWith',
        'regExp',
      ]);
    });

    it('number', () => {
      expect(ColumnFilterProcessor.operatorsForType('number')).toEqual([
        'equals',
        'notEquals',
        'greaterThan',
        'greaterThanOrEquals',
        'lessThan',
        'lessThanOrEquals',
        'filtrex',
      ]);
    });

    it('boolean', () => {
      expect(ColumnFilterProcessor.operatorsForType('boolean')).toEqual([
        'equals',
        'notEquals',
      ]);
    });

    it('unknown', () => {
      expect(ColumnFilterProcessor.operatorsForType('string')).toEqual([
        'contains',
        'equals',
        'notEquals',
        'startsWith',
        'notContains',
        'endsWith',
        'regExp',
      ]);
    });
  });

  describe('translateOperator, translationsForType', () => {
    describe('provides translations for all operators', () => {
      describe('language en', () => {
        it('string', () => {
          const translations = ColumnFilterProcessor.translationsForType(
            'string',
            'en',
          );

          expect(translations).toEqual([
            'Contains',
            'Equals',
            'Not equals',
            'Starts with',
            'Not contains',
            'Ends with',
            'Regular expression',
          ]);
        });

        it('number', () => {
          const translations = ColumnFilterProcessor.translationsForType(
            'number',
            'en',
          );

          expect(translations).toEqual([
            'Equals',
            'Not equals',
            'Greater than',
            'Greater than or Equals',
            'Less than',
            'Less than or equals',
            'Expression',
          ]);
        });

        it('boolean', () => {
          const translations = ColumnFilterProcessor.translationsForType(
            'boolean',
            'en',
          );

          expect(translations).toEqual(['Equals', 'Not equals']);
        });

        it('other', () => {
          const translations = ColumnFilterProcessor.translationsForType(
            'other',
            'en',
          );

          expect(translations).toEqual([
            'Contains',
            'Equals',
            'Not equals',
            'Starts with',
            'Not contains',
            'Ends with',
            'Regular expression',
          ]);
        });
      });

      describe('language de', () => {
        it('string', () => {
          const translations = ColumnFilterProcessor.translationsForType(
            'string',
            'de',
          );

          expect(translations).toEqual([
            'Enthält',
            'Gleich',
            'Ungleich',
            'Beginnt mit',
            'Enthält nicht',
            'Endet mit',
            'Regulärer Ausdruck',
          ]);
        });

        it('number', () => {
          const translations = ColumnFilterProcessor.translationsForType(
            'number',
            'de',
          );

          expect(translations).toEqual([
            'Gleich',
            'Ungleich',
            'Größer',
            'Größer oder gleich',
            'Kleiner als',
            'Kleiner gleich',
            'Ausdruck',
          ]);
        });

        it('boolean', () => {
          const translations = ColumnFilterProcessor.translationsForType(
            'boolean',
            'de',
          );

          expect(translations).toEqual(['Gleich', 'Ungleich']);
        });
      });
    });
  });

  describe('fromModel', () => {
    it('string', () => {
      const filter = ColumnFilterProcessor.fromModel({
        type: 'string',
        column: 'a',
        operator: 'contains',
        search: 'foo',
        _hash: '',
      });
      expect(filter).toBeInstanceOf(StringFilterProcessor);
    });

    it('number', () => {
      const filter = ColumnFilterProcessor.fromModel({
        type: 'number',
        column: 'a',
        operator: 'equals',
        search: 24,
        _hash: '',
      });
      expect(filter).toBeInstanceOf(NumberFilterProcessor);
    });

    it('boolean', () => {
      const filter = ColumnFilterProcessor.fromModel({
        type: 'boolean',
        column: 'a',
        operator: 'equals',
        search: true,
        _hash: '',
      });
      expect(filter).toBeInstanceOf(BooleanFilterProcessor);
    });
  });

  describe('equals', () => {
    const a = new ColumnFilterProcessor();
    const b = new ColumnFilterProcessor();
    it('returns true when instances are the same', () => {
      expect(a.equals(a)).toBe(true);
    });

    it('returns true when instances are not the same', () => {
      expect(a.equals(b)).toBe(false);
    });
  });
});
