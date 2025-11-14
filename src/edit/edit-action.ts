// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { EditAction } from '@rljson/rljson';

import { ColumnFilter } from '../join/filter/column-filter.ts';
import { RowFilter } from '../join/filter/row-filter.ts';
import {
  ColumnInfo,
  ColumnSelection,
} from '../join/selection/column-selection.ts';
import { SetValue } from '../join/set-value/set-value.ts';
import { RowSortType } from '../join/sort/row-sort.ts';

export interface EditActionColumnSelection extends EditAction {
  type: 'selection';
  data: {
    columns: ColumnInfo[];
  };
}

export interface EditActionRowFilter extends EditAction {
  type: 'filter';
  data: RowFilter;
}

export interface EditActionSetValue extends EditAction {
  type: 'setValue';
  data: SetValue;
}

export interface EditActionRowSort extends EditAction {
  type: 'sort';
  data: RowSortType;
}

//..................................................................................

/**
 * Example EditAction of type 'selection'
 * @returns An example EditAction representing a column selection
 */
export const exampleEditActionColumnSelection =
  (): EditActionColumnSelection => ({
    name: 'Car Selection',
    type: 'selection',
    data: {
      columns: ColumnSelection.exampleCarsColumnSelection().columns,
    },
    _hash: '',
  });

//..................................................................................

/**
 * Example EditAction of type 'selection' with only some columns
 * @returns An example EditAction representing a column selection with limited columns
 */
export const exampleEditActionColumnSelectionOnlySomeColumns =
  (): EditActionColumnSelection => ({
    name: 'Car Selection - Some Columns',
    type: 'selection',
    data: {
      columns:
        ColumnSelection.exampleCarsColumnSelectionOnlySomeColumns().columns,
    },
    _hash: '',
  });

//..................................................................................

/**
 * Example EditAction of type 'filter'
 * @returns An example EditAction representing a row filter
 */
export const exampleEditActionRowFilter = (): EditActionRowFilter => ({
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
  },
  _hash: '',
});

/**
 * Example EditAction of type 'setValue'
 * @returns An example EditAction representing a set value action
 */

export const exampleEditActionSetValue = (): EditActionSetValue => ({
  name: 'Set: Service Intervals to [15000, 30000, 45000, 60000]',
  type: 'setValue',
  data: {
    route: 'carCake/carGeneralLayer/carGeneral/serviceIntervals',
    value: [15000, 30000, 45000, 60000],
    _hash: '',
  },
  _hash: '',
});

/**
 * Example EditAction of type 'setValue' for a referenced column
 * @returns An example EditAction representing a set value action for a referenced column
 */
export const exampleEditSetValueReferenced = (): EditActionSetValue => ({
  name: 'Set: Length to 4200',
  type: 'setValue',
  data: {
    route: 'carCake/carTechnicalLayer/carTechnical/carDimensions/length',
    value: 4800,
    _hash: '',
  },
  _hash: '',
});

/**
 * Example EditAction of type 'sort'
 * @returns An example EditAction representing a row sort action
 */
export const exampleEditActionRowSort = (): EditActionRowSort => ({
  name: 'Sort By Brand Edit',
  type: 'sort',
  data: {
    ['carCake/carGeneralLayer/carGeneral/brand']: 'asc',
  },
  _hash: '',
});
