// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Route } from '@rljson/rljson';

import { beforeEach, describe, expect, it } from 'vitest';

import { ColumnSelection } from '../../src/edit/selection/column-selection';

import { expectGolden } from '../setup/goldens';

describe('ColumnSelection', () => {
  let selection: ColumnSelection;

  beforeEach(() => {
    selection = ColumnSelection.example();
  });

  it('empty', () => {
    const empty = ColumnSelection.empty();
    expect(empty.columns).toEqual([]);
  });

  describe('fromRoutes', () => {
    describe('fromRoutes', () => {
      it('takes the last route segment as alias', () => {
        const selection = ColumnSelection.fromRoutes([
          Route.fromFlat('a/b/c'),
          Route.fromFlat('d/e/f'),
          Route.fromFlat('h/i/g'),
          Route.fromFlat('h/i/g'),
        ]);

        expect(selection.aliases).toEqual(['c', 'f', 'g']);
        expect(selection.routes).toEqual(['a/b/c', 'd/e/f', 'h/i/g']);
      });

      it('appends a number to the alias if it already exists', () => {
        const selection = ColumnSelection.fromRoutes([
          Route.fromFlat('a/b/c'),
          Route.fromFlat('d/e/c'),
          Route.fromFlat('f/g/c'),
        ]);

        expect(selection.aliases).toEqual(['c', 'c1', 'c2']);
        expect(selection.routes).toEqual(['a/b/c', 'd/e/c', 'f/g/c']);
      });
    });
  });

  it('routes', () => {
    expectGolden('column-selection/routes.json').toBe(selection.routes);
  });

  describe('route(alias)', () => {
    it('returns the route for the given alias', () => {
      expect(selection.route('stringCol')).toBe('basicTypes/stringsRef/value');

      expect(selection.route('intCol')).toBe(
        'basicTypes/numbersRef/intsRef/value',
      );

      expect(selection.route('floatCol')).toBe(
        'basicTypes/numbersRef/floatsRef/value',
      );
    });

    it('throws an error for an unknown alias', () => {
      expect(() => selection.route('unknown')).toThrowError(
        'Unknown column alias or route: unknown',
      );
    });
  });

  it('routeHashes', () => {
    expectGolden('column-selection/route-hashes.json').toBe(
      selection.routeHashes,
    );
  });

  it('routeSegments', () => {
    expectGolden('column-selection/route-segments.json').toBe(
      selection.routeHashes,
    );
  });

  describe('alias(route))', () => {
    it('returns the alias for the given route', () => {
      expect(selection.alias('basicTypes/stringsRef/value')).toBe('stringCol');
      expect(selection.alias('basicTypes/numbersRef/intsRef/value')).toBe(
        'intCol',
      );
    });

    it('throws an error for an unknown route', () => {
      expect(() =>
        selection.alias('basicTypes/stringsRef/unknown'),
      ).toThrowError(
        'Unknown column alias or route: basicTypes/stringsRef/unknown',
      );
    });
  });

  describe('merge', () => {
    it('merges multiple column selections into one', () => {
      const selection1 = new ColumnSelection([
        {
          key: 'a',
          alias: 'a',
          route: 'a/b/c',
          titleLong: '',
          titleShort: '',
          type: 'string',
        },
        {
          key: 'd',
          alias: 'd',
          route: 'd/e/f',
          titleLong: '',
          titleShort: '',
          type: 'string',
        },
      ]);
      const selection2 = new ColumnSelection([
        {
          key: 'g',
          alias: 'g',
          route: 'h/i/g',
          titleLong: '',
          titleShort: '',
          type: 'string',
        },
        {
          key: 'd',
          alias: 'd',
          route: 'd/e/f',
          titleLong: '',
          titleShort: '',
          type: 'string',
        },
      ]);
      const selection3 = new ColumnSelection([
        {
          key: 'a',
          alias: 'a',
          route: 'a/b/c',
          titleLong: '',
          titleShort: '',
          type: 'string',
        },
        {
          key: 'h',
          alias: 'h',
          route: 'h/i/g',
          titleLong: '',
          titleShort: '',
          type: 'string',
        },
      ]);
      const merged = ColumnSelection.merge([
        selection1,
        selection2,
        selection3,
      ]);
      expect(merged.routes).toEqual(['a/b/c', 'd/e/f', 'h/i/g']);
      expect(merged.aliases).toEqual(['c', 'f', 'g']);
    });
  });

  it('aliases', () => {
    expectGolden('column-selection/aliases.json').toBe(selection.aliases);
  });

  it('metadata', () => {
    expectGolden('column-selection/title-short.json').toBe(
      selection.metadata('titleShort'),
    );

    expectGolden('column-selection/title-long.json').toBe(
      selection.metadata('titleLong'),
    );
  });

  describe('should throw', () => {
    it('throws when alias is duplicated', () => {
      expect(
        () => new ColumnSelection(ColumnSelection.exampleBroken()),
      ).toThrow('Duplicate alias: stringCol');
    });
  });

  describe('columnIndex(aliasOrRoute)', () => {
    describe('returns the column index for', () => {
      it('the given alias', () => {
        expect(selection.columnIndex('stringCol')).toBe(0);
        expect(selection.columnIndex('intCol')).toBe(1);
      });

      it('the given route', () => {
        expect(
          selection.columnIndex('basicTypes/numbersRef/intsRef/value'),
        ).toBe(1);

        expect(
          selection.columnIndex('basicTypes/numbersRef/floatsRef/value'),
        ).toBe(2);
      });

      it('the given route segments', () => {
        expect(
          selection.columnIndex(['basicTypes', 'stringsRef', 'value']),
        ).toBe(0);
        expect(
          selection.columnIndex([
            'basicTypes',
            'numbersRef',
            'intsRef',
            'value',
          ]),
        ).toBe(1);
      });

      it('the given route hash', () => {
        const h = ColumnSelection.calcHash;
        expect(
          selection.columnIndex(h('basicTypes/numbersRef/intsRef/value')),
        ).toBe(1);

        expect(
          selection.columnIndex(h('basicTypes/numbersRef/floatsRef/value')),
        ).toBe(2);
      });

      it('the given column index', () => {
        expect(selection.columnIndex(0)).toBe(0);
        expect(selection.columnIndex(1)).toBe(1);
        expect(selection.columnIndex(2)).toBe(2);
      });
    });

    describe('return -1', () => {
      it('not when throwIfNotExisting is false and column is not existing', () => {
        const throwIfNotExisting = true;

        expect(selection.columnIndex('unknown', !throwIfNotExisting)).toBe(-1);
      });
    });

    describe('throws an error for an unknown alias or route', () => {
      it('when throwIfNotExisting is true', () => {
        expect(() => selection.columnIndex('unknown')).toThrowError(
          'Unknown column alias or route: unknown',
        );
      });
    });

    describe('column(aliasRouteOrHash)', () => {
      it('returns the column config for the desired column', () => {
        const result = selection.column('stringCol');
        expectGolden('column-selection/column.json').toBe(result);
      });
    });

    describe('count', () => {
      it('returns the count', () => {
        expect(selection.count).toBe(7);
      });
    });

    describe('check', () => {
      describe('throws an error', () => {
        it('when an alias is upper camel case', () => {
          expect(() =>
            ColumnSelection.check(['ShortTitle'], ['shortTextsDe/shortText']),
          ).toThrowError(
            'Invalid alias "ShortTitle". Aliases must be lower camel case.',
          );
        });

        it('when an route contains special chars', () => {
          expect(() =>
            ColumnSelection.check(
              ['shortTitle'],
              ['shortTextsDe#shortTextsDeRef/shortText$'],
            ),
          ).toThrowError(
            'Invalid route "shortTextsDe#shortTextsDeRef/shortText$". ' +
              'Routes must only contain letters, numbers and slashes.',
          );
        });

        it('when the parts of an route are not lower camel case strings', () => {
          expect(() =>
            ColumnSelection.check(
              ['shortTitle'],
              ['shortTextsDe/ShortTextsDeRef/shortText'],
            ),
          ).toThrowError(
            'Invalid route segment "ShortTextsDeRef". ' +
              'Route segments must be lower camel case.',
          );
        });

        it('when an route occurs more than once', () => {
          expect(() =>
            ColumnSelection.check(
              ['shortTitle', 'shortTitle2'],
              ['shortTextsDe/shortText', 'shortTextsDe/shortText'],
            ),
          ).toThrowError(
            'Duplicate route shortTextsDe/shortText. ' +
              'A column must only occur once.',
          );
        });
      });
    });

    describe('addedColumns', () => {
      it('returns the missing columns', () => {
        const current = ColumnSelection.fromRoutes([
          Route.fromFlat('a/b/c'),
          Route.fromFlat('d/e/f'),
          Route.fromFlat('h/i/g'),
        ]);

        const previous = ColumnSelection.fromRoutes([
          Route.fromFlat('k/j/l'),
          Route.fromFlat('d/e/f'),
          Route.fromFlat('x/y/z'),
        ]);

        const addedColumns = previous.addedColumns(current);
        expect(addedColumns).toEqual(['k/j/l', 'x/y/z']);
      });
    });
  });
});
