// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Json } from '@rljson/json';

import { ColumnFilter } from '../join/filter/column-filter.ts';
import { RowFilter } from '../join/filter/row-filter.ts';
import { JoinProcessType } from '../join/join.ts';
import {
  ColumnInfo,
  ColumnSelection,
} from '../join/selection/column-selection.ts';
import { SetValue } from '../join/set-value/set-value.ts';
import { RowSortType } from '../join/sort/row-sort.ts';

export type EditActionProcessType = JoinProcessType;

export type EditActionColumnSelection = ColumnInfo[];
export type EditActionRowFilter = RowFilter;
export type EditActionSetValue = SetValue;
export type EditActionRowSort = RowSortType;

export interface EditAction extends Json {
  name: string;
  type: EditActionProcessType;
  data:
    | EditActionColumnSelection
    | EditActionRowFilter
    | EditActionSetValue
    | EditActionRowSort;
  _hash: string;
}

//..................................................................................

/**
 * Example EditAction of type 'selection'
 * @returns An example EditAction representing a column selection
 */
export const exampleEditActionColumnSelection = (): EditAction => ({
  name: 'Car Selection',
  type: 'selection',
  data: ColumnSelection.exampleCarsColumnSelection().columns,
  _hash: '',
});

//..................................................................................

/**
 * Example EditAction of type 'selection' with only some columns
 * @returns An example EditAction representing a column selection with limited columns
 */
export const exampleEditActionColumnSelectionOnlySomeColumns =
  (): EditAction => ({
    name: 'Car Selection - Some Columns',
    type: 'selection',
    data: ColumnSelection.exampleCarsColumnSelectionOnlySomeColumns().columns,
    _hash: '',
  });

//..................................................................................

/**
 * Example EditAction of type 'filter'
 * @returns An example EditAction representing a row filter
 */
export const exampleEditActionRowFilter = (): EditAction => ({
  name: 'Electric Cars Filter',
  type: 'filter',
  data: {
    columnFilters: [
      {
        type: 'boolean',
        column: 'carCake/carGeneralLayer/carGeneral/isElectric',
        operator: 'equals',
        search: true,
        _hash: '',
      },
      {
        type: 'number',
        column: 'carCake/carTechnicalLayer/carTechnical/carDimensions/length',
        operator: 'greaterThan',
        search: 4000,
        _hash: '',
      },
    ] as ColumnFilter<any>[],
    operator: 'and',
    value: true,
    _hash: '',
  } as EditActionRowFilter,
  _hash: '',
});

/**
 * Example EditAction of type 'setValue'
 * @returns An example EditAction representing a set value action
 */

export const exampleEditActionSetValue = (): EditAction => ({
  name: 'Set Service Interval Edit',
  type: 'setValue',
  data: {
    route: 'carCake/carGeneralLayer/carGeneral/serviceIntervals',
    value: [15000, 30000, 45000, 60000],
    _hash: '',
  } as EditActionSetValue,
  _hash: '',
});

/**
 * Example EditAction of type 'sort'
 * @returns An example EditAction representing a row sort action
 */
export const exampleEditActionRowSort = (): EditAction => ({
  name: 'Sort By Brand Edit',
  type: 'sort',
  data: {
    ['carCake/carGeneralLayer/carGeneral/brand']: 'asc',
  } as EditActionRowSort,
  _hash: '',
});
