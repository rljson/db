// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';
import { Json } from '@rljson/json';

import {
  emptyRowFilter,
  exampleRowFilter,
  RowFilter,
} from '../join/filter/row-filter.ts';

import { EditAction, exampleEditAction } from './edit-action.ts';

/** An edit step applied to an catalog */
export interface Edit extends Json {
  /** The name of the step. */
  name: string;

  /** The filters of the step. */
  filter: RowFilter;

  /** The actions applied by the step */
  actions: EditAction[];

  /** The hash of the step */
  _hash: string;
}

// ...........................................................................
export const initialEdit: Edit = hip<Edit>({
  name: '',
  filter: emptyRowFilter,
  actions: [],
  _hash: '',
});

// .............................................................................
/** An example edit step model for test purposes */
export const exampleEdit = (): Edit =>
  hip<Edit>({
    name: 'Example Step',
    filter: exampleRowFilter(),
    actions: [exampleEditAction()],
    _hash: '',
  });

// .............................................................................
export const exampleEditSetAllEndingWithOTo1234 = (): Edit =>
  hip<Edit>({
    name: 'Set all ending with o to 1234',
    filter: {
      columnFilters: [
        {
          type: 'string',
          column: 'basicTypes/stringsRef/value',
          operator: 'endsWith',
          search: 'o',
          _hash: '',
        },
      ],
      operator: 'and',
      _hash: '',
    },
    actions: [
      {
        route: 'basicTypes/numbersRef/intsRef/value',
        setValue: 1234,
        _hash: '',
      },
      {
        route: 'basicTypes/numbersRef/floatsRef/value',
        setValue: 1.234,
        _hash: '',
      },
    ],
    _hash: '',
  });

// .............................................................................
export const exampleEditSetAllStartingWithOToTrue = (): Edit =>
  hip<Edit>({
    name: 'Set all smaller 2 to to true',
    filter: {
      columnFilters: [
        {
          type: 'string',
          column: 'basicTypes/stringsRef/value',
          operator: 'startsWith',
          search: 'O',
          _hash: '',
        },
      ],
      operator: 'and',
      _hash: '',
    },
    actions: [
      {
        route: 'basicTypes/booleansRef/value',
        setValue: true,
        _hash: '',
      },
    ],
    _hash: '',
  });

// .............................................................................
export const exampleEditSetAllTrueToDone = (): Edit =>
  hip<Edit>({
    name: 'Set all true to done',
    filter: {
      columnFilters: [
        {
          type: 'boolean',
          column: 'basicTypes/booleansRef/value',
          operator: 'equals',
          search: true,
          _hash: '',
        },
      ],
      operator: 'and',
      _hash: '',
    },
    actions: [
      {
        route: 'complexTypes/jsonObjectsRef/value',
        setValue: { done: true },
        _hash: '',
      },
    ],
    _hash: '',
  });

// .............................................................................
export const exampleEditCarsExample = (): Edit =>
  hip<Edit>({
    name: 'Set all cars that are not electric to electric',
    filter: {
      columnFilters: [
        {
          type: 'boolean',
          column: `carCake/carGeneralLayer/carGeneral/isElectric`,
          operator: 'equals',
          search: false,
          _hash: '',
        },
        {
          type: 'string',
          column: `carCake/carTechnicalLayer/carTechnical/transmission`,
          operator: 'equals',
          search: 'Automatic',
          _hash: '',
        },
      ],
      operator: 'and',
      _hash: '',
    },
    actions: [
      {
        route: `carCake/carGeneralLayer/carGeneral/isElectric`,
        setValue: true,
      },
      {
        route: `carCake/carTechnicalLayer/carTechnical/gears`,
        setValue: 5,
      },
    ],
    _hash: '',
  });

// ..............................................................................
/// An example database with edits
export const exampleEditDb = (): Record<string, Edit> => {
  const a = exampleEditSetAllEndingWithOTo1234();
  const b = exampleEditSetAllStartingWithOToTrue();
  const c = exampleEditSetAllTrueToDone();

  return {
    [a._hash]: a,
    [b._hash]: b,
    [c._hash]: c,
  };
};
