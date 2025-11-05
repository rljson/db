/* eslint-disable @typescript-eslint/no-unused-vars */

// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { BooleanFilterProcessor } from './boolean-filter-processor.ts';
import { BooleanFilter } from './boolean-filter.ts';
import { ColumnFilter } from './column-filter.ts';
import { NumberFilterProcessor } from './number-filter-processor.ts';
import { NumberFilter } from './number-filter.ts';
import { StringFilterProcessor } from './string-filter-processor.ts';
import { StringFilter } from './string-filter.ts';

export class ColumnFilterProcessor {
  /* v8 ignore next -- @preserve */
  matches(_cellValue: string): boolean {
    /* v8 ignore next -- @preserve */
    return true;
  }

  equals(_other: ColumnFilterProcessor): boolean {
    return this === _other;
  }

  // ...........................................................................
  static fromModel(model: ColumnFilter<any>): ColumnFilterProcessor {
    switch (model.type) {
      case 'string':
        return StringFilterProcessor.fromModel(model as StringFilter);
      case 'number':
        return NumberFilterProcessor.fromModel(model as NumberFilter);
      case 'boolean':
        return BooleanFilterProcessor.fromModel(model as BooleanFilter);
      /* v8 ignore next -- @preserve */
      default:
        return StringFilterProcessor.fromModel(model as StringFilter);
    }
  }

  // ...........................................................................
  /* v8 ignore stop */
  static operatorsForType(type: string): string[] {
    switch (type) {
      case 'string':
        return StringFilterProcessor.allOperators;
      case 'number':
        return NumberFilterProcessor.allOperators;
      case 'boolean':
        return BooleanFilterProcessor.allOperators;
      default:
        return StringFilterProcessor.allOperators;
    }
  }

  static translationsForType(type: string, language: 'de' | 'en'): string[] {
    const operators = ColumnFilterProcessor.operatorsForType(type);
    const translations = [];

    for (const operator of operators) {
      translations.push(
        ColumnFilterProcessor.translateOperator(operator, language),
      );
    }

    return translations;
  }

  static translateOperator(operator: string, language: 'de' | 'en') {
    const translations: Record<string, Record<string, string>> = {
      equals: { en: 'Equals', de: 'Gleich' },
      notEquals: { en: 'Not equals', de: 'Ungleich' },
      greaterThan: { en: 'Greater than', de: 'Größer' },
      greaterThanOrEquals: {
        en: 'Greater than or Equals',
        de: 'Größer oder gleich',
      },
      lessThan: { en: 'Less than', de: 'Kleiner als' },
      lessThanOrEquals: {
        en: 'Less than or equals',
        de: 'Kleiner gleich',
      },
      startsWith: { en: 'Starts with', de: 'Beginnt mit' },
      contains: { en: 'Contains', de: 'Enthält' },
      notContains: { en: 'Not contains', de: 'Enthält nicht' },
      endsWith: { en: 'Ends with', de: 'Endet mit' },
      regExp: { en: 'Regular expression', de: 'Regulärer Ausdruck' },
      filtrex: { en: 'Expression', de: 'Ausdruck' },
    };

    return translations[operator][language];
  }
}
