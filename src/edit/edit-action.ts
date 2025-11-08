// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Json } from '@rljson/json';

import { RowFilter } from '../join/filter/row-filter.ts';
import { JoinProcessType } from '../join/join.ts';
import { ColumnInfo } from '../join/selection/column-selection.ts';
import { SetValue } from '../join/set-value/set-value.ts';
import { RowSortType } from '../join/sort/row-sort.ts';

export type EditActionProcessType = JoinProcessType;

export type EditActionColumnSelection = ColumnInfo[];
export type EditActionRowFilter = RowFilter;
export type EditActionSetValue = SetValue;
export type EditActionRowSort = RowSortType;

export interface EditAction extends Json {
  name: string;
  action: EditActionProcessType;
  data:
    | EditActionColumnSelection
    | EditActionRowFilter
    | EditActionSetValue
    | EditActionRowSort;
  _hash: string;
}

export const exampleEditActionColumnSelection: EditAction = {
  name: 'Car Selection',
  action: 'selection',
  data: [
    {
      key: 'brand',
      route: '/carCake/carGeneralLayer/carGeneral/brand',
      alias: 'Brand',
      titleLong: 'Car Brand',
      titleShort: 'Brand',
      type: 'string',
      _hash: '',
    } as ColumnInfo,
    {
      key: 'type',
      route: '/carCake/carGeneralLayer/carGeneral/type',
      alias: 'Type',
      titleLong: 'Car Type',
      titleShort: 'Type',
      type: 'string',
      _hash: '',
    } as ColumnInfo,
    {
      key: 'serviceIntervals',
      route: '/carCake/carGeneralLayer/carGeneral/serviceIntervals',
      alias: 'Service Intervals',
      titleLong: 'Car Service Intervals',
      titleShort: 'Service Intervals',
      type: 'jsonValue',
      _hash: '',
    } as ColumnInfo,
    {
      key: 'isElectric',
      route: '/carCake/carGeneralLayer/carGeneral/isElectric',
      alias: 'Electric',
      titleLong: 'Is Electric Car',
      titleShort: 'Electric',
      type: 'boolean',
      _hash: '',
    } as ColumnInfo,
    {
      key: 'height',
      route: '/carCake/carTechnicalLayer/carTechnical/dimensions/height',
      alias: 'Height',
      titleLong: 'Car Height',
      titleShort: 'Height',
      type: 'number',
      _hash: '',
    } as ColumnInfo,
    {
      key: 'width',
      route: '/carCake/carTechnicalLayer/carTechnical/dimensions/width',
      alias: 'Width',
      titleLong: 'Car Width',
      titleShort: 'Width',
      type: 'number',
      _hash: '',
    } as ColumnInfo,
    {
      key: 'length',
      route: '/carCake/carTechnicalLayer/carTechnical/dimensions/length',
      alias: 'Length',
      titleLong: 'Car Length',
      titleShort: 'Length',
      type: 'number',
      _hash: '',
    } as ColumnInfo,
    {
      key: 'engine',
      route: '/carCake/carTechnicalLayer/carTechnical/engine',
      alias: 'Engine',
      titleLong: 'Car Engine',
      titleShort: 'Engine',
      type: 'string',
      _hash: '',
    } as ColumnInfo,
    {
      key: 'repairedByWorkshop',
      route: '/carCake/carTechnicalLayer/carTechnical/repairedByWorkshop',
      alias: 'Repaired By Workshop',
      titleLong: 'Was Repaired By Workshop',
      titleShort: 'Repaired By Workshop',
      type: 'boolean',
      _hash: '',
    } as ColumnInfo,
  ] as EditActionColumnSelection,
  _hash: '',
};
