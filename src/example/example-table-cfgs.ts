import { hip } from '@rljson/hash';
import {
  ColumnCfg,
  createCakeTableCfg,
  createLayerTableCfg,
  createSliceIdsTableCfg,
  TableCfg,
} from '@rljson/rljson';

// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

export const exampleTableCfgs = (): Map<string, TableCfg> => {
  //............................................................................
  //Car Example TableCfgs
  //............................................................................

  const carSliceIdTableCfg = hip<TableCfg>(
    createSliceIdsTableCfg('carSliceId'),
  ) as TableCfg;

  const carGeneralTableCfg = hip<TableCfg>({
    key: 'carGeneral',
    type: 'components',
    columns: [
      { key: '_hash', type: 'string', titleLong: 'Hash', titleShort: 'Hash' },
      { key: 'brand', type: 'string', titleLong: 'Brand', titleShort: 'Brand' },
      { key: 'type', type: 'string', titleLong: 'Type', titleShort: 'Type' },
      { key: 'doors', type: 'number', titleLong: 'Doors', titleShort: 'Doors' },
      {
        key: 'energyConsumption',
        type: 'number',
        titleLong: 'Energy Consumption',
        titleShort: 'Energy',
      },
      {
        key: 'units',
        type: 'json',
        titleLong: 'Energy Unit',
        titleShort: 'Unit',
      },
      {
        key: 'serviceIntervals',
        type: 'jsonArray',
        titleLong: 'Service Intervals',
        titleShort: 'Intervals',
      },
      {
        key: 'isElectric',
        type: 'boolean',
        titleLong: 'Is Electric',
        titleShort: 'Electric',
      },
      {
        key: 'meta',
        type: 'jsonValue',
        titleLong: 'Meta Information',
        titleShort: 'Meta',
      },
    ] as ColumnCfg[],
    isHead: false,
    isRoot: false,
    isShared: true,
  } as TableCfg);

  const carDimensionsTableCfg = hip<TableCfg>({
    key: 'carDimensions',
    type: 'components',
    columns: [
      { key: '_hash', type: 'string', titleLong: 'Hash', titleShort: 'Hash' },
      {
        key: 'height',
        type: 'number',
        titleLong: 'Height',
        titleShort: 'Height',
      },
      { key: 'width', type: 'number', titleLong: 'Width', titleShort: 'Width' },
      {
        key: 'length',
        type: 'number',
        titleLong: 'Length',
        titleShort: 'Length',
      },
    ],
    isHead: false,
    isRoot: false,
    isShared: true,
  }) as TableCfg;

  const carTechnicalTableCfg = hip<TableCfg>({
    key: 'carTechnical',
    type: 'components',
    columns: [
      { key: '_hash', type: 'string', titleLong: 'Hash', titleShort: 'Hash' },
      {
        key: 'engine',
        type: 'string',
        titleLong: 'Engine',
        titleShort: 'Engine',
      },
      {
        key: 'transmission',
        type: 'string',
        titleLong: 'Transmission',
        titleShort: 'Transmission',
      },
      { key: 'gears', type: 'number', titleLong: 'Gears', titleShort: 'Gears' },
      {
        key: 'dimensions',
        type: 'jsonValue',
        titleLong: 'Car Dimensions Ref',
        titleShort: 'Dimensions Ref',
        ref: {
          tableKey: 'carDimensions',
        },
      } as ColumnCfg,
      {
        key: 'repairedByWorkshop',
        type: 'string',
        titleLong: 'Repaired By Workshop',
        titleShort: 'Workshop',
      },
    ],
    isHead: false,
    isRoot: false,
    isShared: true,
  }) as TableCfg;

  const carColorTableCfg = hip<TableCfg>({
    key: 'carColor',
    type: 'components',
    columns: [
      { key: '_hash', type: 'string', titleLong: 'Hash', titleShort: 'Hash' },
      { key: 'sides', type: 'string', titleLong: 'Sides', titleShort: 'Sides' },
      { key: 'roof', type: 'string', titleLong: 'Roof', titleShort: 'Roof' },
      {
        key: 'highlights',
        type: 'string',
        titleLong: 'Highlights',
        titleShort: 'Highlights',
      },
    ],
    isHead: false,
    isRoot: false,
    isShared: true,
  }) as TableCfg;

  const carGeneralLayerTableCfg = hip<TableCfg>(
    createLayerTableCfg('carGeneralLayer'),
  ) as TableCfg;

  const carTechnicalLayerTableCfg = hip<TableCfg>(
    createLayerTableCfg('carTechnicalLayer'),
  ) as TableCfg;

  const carColorLayerTableCfg = hip<TableCfg>(
    createLayerTableCfg('carColorLayer'),
  ) as TableCfg;

  const carCakeTableCfg = hip<TableCfg>(
    createCakeTableCfg('carCake'),
  ) as TableCfg;

  //............................................................................
  //Site Example TableCfgs
  //............................................................................

  const siteSliceIdTableCfg = hip<TableCfg>(
    createSliceIdsTableCfg('siteSliceId'),
  );

  const siteGeneralTableCfg = hip<TableCfg>({
    key: 'siteGeneral',
    type: 'components',
    columns: [
      { key: '_hash', type: 'string', titleLong: 'Hash', titleShort: 'Hash' },
      {
        key: 'manager',
        type: 'string',
        titleLong: 'Site Manager',
        titleShort: 'Manager',
      },
      {
        key: 'street',
        type: 'string',
        titleLong: 'Street',
        titleShort: 'Street',
      },
      {
        key: 'houseNr',
        type: 'number',
        titleLong: 'House Number',
        titleShort: 'House Number',
      },
      {
        key: 'postCode',
        type: 'number',
        titleLong: 'Postal Code',
        titleShort: 'Postal Code',
      },
      {
        key: 'city',
        type: 'string',
        titleLong: 'City',
        titleShort: 'City',
      },
      {
        key: 'cars',
        type: 'jsonArray',
        titleLong: 'Cars Ref',
        titleShort: 'Cars Ref',
        ref: {
          tableKey: 'carCake',
        },
      } as ColumnCfg,
    ] as ColumnCfg[],
    isHead: false,
    isRoot: false,
    isShared: true,
  } as TableCfg);

  return new Map([
    ['carSliceId', carSliceIdTableCfg],
    ['carGeneral', carGeneralTableCfg],
    ['carDimensions', carDimensionsTableCfg],
    ['carTechnical', carTechnicalTableCfg],
    ['carColor', carColorTableCfg],
    ['carGeneralLayer', carGeneralLayerTableCfg],
    ['carTechnicalLayer', carTechnicalLayerTableCfg],
    ['carColorLayer', carColorLayerTableCfg],
    ['carCake', carCakeTableCfg],
    ['siteSliceId', siteSliceIdTableCfg],
    ['siteGeneral', siteGeneralTableCfg],
  ]);
};
