// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';
import { Json } from '@rljson/json';
import {
  CakesTable,
  ComponentsTable,
  LayersTable,
  Rljson,
  SliceIdsTable,
  TableCfg,
  TablesCfgTable,
} from '@rljson/rljson';

export interface CarsExample extends Rljson {
  carSliceId: SliceIdsTable;
  carGeneral: ComponentsTable<CarGeneral>;
  carTechnical: ComponentsTable<Json>;
  carColor: ComponentsTable<Json>;
  carGeneralLayer: LayersTable;
  carTechnicalLayer: LayersTable;
  carColorLayer: LayersTable;
  carCake: CakesTable;
  wheelSliceId: SliceIdsTable;
  wheelBrand: ComponentsTable<Json>;
  wheelDimension: ComponentsTable<Json>;
  wheelBrandLayer: LayersTable;
  wheelDimensionLayer: LayersTable;
  wheelCake: CakesTable;
  tableCfgs: TablesCfgTable;
}

export interface CarGeneral extends Json {
  brand: string;
  type: string;
  doors: number;
}

export const carsExample = (): CarsExample => {
  //CarSliceId
  //................................................................
  const carSliceIdTableCfg = hip<TableCfg>({
    key: 'carSliceId',
    type: 'sliceIds',
    columns: [
      { key: '_hash', type: 'string' },
      { key: 'add', type: 'jsonArray' },
    ],
    isHead: false,
    isRoot: false,
    isShared: true,
  }) as TableCfg;

  const carSliceId = hip<any>({
    _tableCfg: carSliceIdTableCfg._hash,
    _type: 'sliceIds',
    _data: [
      {
        add: ['VIN1', 'VIN2'],
        _hash: '',
      },
    ],
    _hash: '',
  }) as SliceIdsTable;

  //CarGeneral
  //................................................................
  const carGeneralTableCfg = hip<any>({
    key: 'carGeneral',
    type: 'components',
    columns: [
      { key: '_hash', type: 'string' },
      { key: 'brand', type: 'string' },
      { key: 'type', type: 'string' },
      { key: 'doors', type: 'number' },
    ],
    isHead: false,
    isRoot: false,
    isShared: true,
  }) as TableCfg;

  const carGeneral = hip<any>({
    _tableCfg: carGeneralTableCfg._hash,
    _type: 'components',
    _data: [
      {
        brand: 'Volkswagen',
        type: 'Polo',
        doors: 5,
        _hash: '',
      },
      {
        brand: 'Volkswagen',
        type: 'Golf',
        doors: 3,
        _hash: '',
      },
    ],
    _hash: '',
  }) as ComponentsTable<CarGeneral>;

  //CarTechnical
  //................................................................
  const carTechnicalTableCfg = hip<any>({
    key: 'carTechnical',
    type: 'components',
    columns: [
      { key: '_hash', type: 'string' },
      { key: 'engine', type: 'string' },
      { key: 'transmission', type: 'string' },
      { key: 'gears', type: 'number' },
    ],
    isHead: false,
    isRoot: false,
    isShared: true,
  }) as TableCfg;

  const carTechnical = hip<any>({
    _tableCfg: carTechnicalTableCfg._hash,
    _type: 'components',
    _data: [
      {
        engine: 'Diesel',
        transmission: 'Manual',
        gears: 6,
        _hash: '',
      },
      {
        engine: 'Petrol',
        transmission: 'Automatic',
        gears: 7,
        _hash: '',
      },
    ],
    _hash: '',
  }) as ComponentsTable<Json>;

  //CarColor
  //................................................................
  const carColorTableCfg = hip<any>({
    key: 'carColor',
    type: 'components',
    columns: [
      { key: '_hash', type: 'string' },
      { key: 'sides', type: 'string' },
      { key: 'roof', type: 'string' },
      { key: 'highlights', type: 'string' },
    ],
    isHead: false,
    isRoot: false,
    isShared: true,
  }) as TableCfg;

  const carColor = hip<any>({
    _tableCfg: carColorTableCfg._hash,
    _type: 'components',
    _data: [
      {
        sides: 'green',
        roof: 'white',
        highlights: 'chrome',
        _hash: '',
      },
      {
        sides: 'blue',
        roof: 'black',
        highlights: 'chrome',
        _hash: '',
      },
    ],
    _hash: '',
  }) as ComponentsTable<Json>;

  //CarLayers and CarCake
  //................................................................
  const carGeneralLayerTableCfg = hip<TableCfg>({
    key: 'carGeneralLayer',
    type: 'layers',
    columns: [
      { key: '_hash', type: 'string' },
      { key: 'sliceIdsTable', type: 'string' },
      { key: 'sliceIdsTableRow', type: 'string' },
      { key: 'componentsTable', type: 'string' },
      { key: 'add', type: 'json' },
    ],
    isHead: false,
    isRoot: false,
    isShared: true,
  }) as TableCfg;

  const carGeneralLayer = hip<any>({
    _tableCfg: carGeneralLayerTableCfg._hash,
    _type: 'layers',
    _data: [
      {
        add: {
          VIN1: carGeneral._data[0]._hash || '',
          VIN2: carGeneral._data[1]._hash || '',
          _hash: '',
        },
        sliceIdsTable: 'carSliceId',
        sliceIdsTableRow: carSliceId._data[0]._hash || '',
        componentsTable: 'carGeneral',
        _hash: '',
      },
    ],
    _hash: '',
  }) as LayersTable;

  const carTechnicalLayerTableCfg = hip<TableCfg>({
    key: 'carTechnicalLayer',
    type: 'layers',
    columns: [
      { key: '_hash', type: 'string' },
      { key: 'sliceIdsTable', type: 'string' },
      { key: 'sliceIdsTableRow', type: 'string' },
      { key: 'componentsTable', type: 'string' },
      { key: 'add', type: 'json' },
    ],
    isHead: false,
    isRoot: false,
    isShared: true,
  }) as TableCfg;

  const carTechnicalLayer = hip<any>({
    _tableCfg: carTechnicalLayerTableCfg._hash,
    _type: 'layers',
    _data: [
      {
        add: {
          VIN1: carTechnical._data[0]._hash || '',
          VIN2: carTechnical._data[1]._hash || '',
          _hash: '',
        },
        sliceIdsTable: 'carSliceId',
        sliceIdsTableRow: carSliceId._data[0]._hash || '',
        componentsTable: 'carTechnical',
        _hash: '',
      },
    ],
    _hash: '',
  }) as LayersTable;

  const carColorLayerTableCfg = hip<TableCfg>({
    key: 'carColorLayer',
    type: 'layers',
    columns: [
      { key: '_hash', type: 'string' },
      { key: 'sliceIdsTable', type: 'string' },
      { key: 'sliceIdsTableRow', type: 'string' },
      { key: 'componentsTable', type: 'string' },
      { key: 'add', type: 'json' },
    ],
    isHead: false,
    isRoot: false,
    isShared: true,
  }) as TableCfg;

  const carColorLayer = hip<any>({
    _tableCfg: carColorLayerTableCfg._hash,
    _type: 'layers',
    _data: [
      {
        add: {
          VIN1: carColor._data[0]._hash || '',
          VIN2: carColor._data[1]._hash || '',
          _hash: '',
        },
        sliceIdsTable: 'carSliceId',
        sliceIdsTableRow: carSliceId._data[0]._hash || '',
        componentsTable: 'carColor',
        _hash: '',
      },
    ],
    _hash: '',
  }) as LayersTable;

  const carCakeTableCfg = hip<TableCfg>({
    key: 'carCake',
    type: 'cakes',
    columns: [
      { key: '_hash', type: 'string' },
      { key: 'sliceIdsTable', type: 'string' },
      { key: 'sliceIdsRow', type: 'string' },
      { key: 'layers', type: 'json' },
    ],
    isHead: false,
    isRoot: false,
    isShared: true,
  }) as TableCfg;

  const carCake = hip<any>({
    _tableCfg: carCakeTableCfg._hash,
    _type: 'cakes',
    _data: [
      {
        sliceIdsTable: 'carSliceId',
        sliceIdsRow: carSliceId._data[0]._hash || '',
        layers: {
          carGeneralLayer: carGeneralLayer._data[0]._hash || '',
          carTechnicalLayer: carTechnicalLayer._data[0]._hash || '',
          carColorLayer: carColorLayer._data[0]._hash || '',
        },
      },
    ],
  }) as CakesTable;

  //WheelSliceId
  //................................................................

  const wheelSliceIdTableCfg = hip<TableCfg>({
    key: 'wheelSliceId',
    type: 'sliceIds',
    columns: [
      { key: '_hash', type: 'string' },
      { key: 'add', type: 'jsonArray' },
    ],
    isHead: false,
    isRoot: false,
    isShared: true,
  }) as TableCfg;

  const wheelSliceId = hip<any>({
    _tableCfg: wheelSliceIdTableCfg._hash,
    _type: 'sliceIds',
    _data: [
      {
        add: ['BOB37382', 'BOB37383'],
        _hash: '',
      },
    ],
    _hash: '',
  }) as SliceIdsTable;

  //WheelBrand
  //................................................................
  const wheelBrandTableCfg = hip<any>({
    key: 'wheelBrand',
    type: 'components',
    columns: [
      { key: '_hash', type: 'string' },
      { key: 'brand', type: 'string' },
    ],
    isHead: false,
    isRoot: false,
    isShared: true,
  }) as TableCfg;

  const wheelBrand = hip<any>({
    _tableCfg: wheelBrandTableCfg._hash,
    _type: 'components',
    _data: [
      {
        brand: 'Borbet',
        _hash: '',
      },
      {
        brand: 'Borbet',
        _hash: '',
      },
    ],
    _hash: '',
  }) as ComponentsTable<Json>;

  //WheelDimension
  //................................................................

  const wheelDimensionTableCfg = hip<any>({
    key: 'wheelDimension',
    type: 'components',
    columns: [
      { key: '_hash', type: 'string' },
      { key: 'dimension', type: 'string' },
    ],
    isHead: false,
    isRoot: false,
    isShared: true,
  }) as TableCfg;

  const wheelDimension = hip<any>({
    _tableCfg: wheelDimensionTableCfg._hash,
    _type: 'components',
    _data: [
      {
        dimension: '185/60 R16',
        _hash: '',
      },
      {
        dimension: '195/55 R16',
        _hash: '',
      },
    ],
    _hash: '',
  }) as ComponentsTable<Json>;

  //WheelLayers and WheelCake
  //................................................................

  const wheelBrandLayerTableCfg = hip<TableCfg>({
    key: 'wheelBrandLayer',
    type: 'layers',
    columns: [
      { key: '_hash', type: 'string' },
      { key: 'sliceIdsTable', type: 'string' },
      { key: 'sliceIdsTableRow', type: 'string' },
      { key: 'componentsTable', type: 'string' },
      { key: 'add', type: 'json' },
    ],
    isHead: false,
    isRoot: false,
    isShared: true,
  }) as TableCfg;

  const wheelBrandLayer = hip<any>({
    _tableCfg: wheelBrandLayerTableCfg._hash,
    _type: 'layers',
    _data: [
      {
        add: {
          BOB37382: wheelBrand._data[0]._hash || '',
          BOB37383: wheelBrand._data[1]._hash || '',
          _hash: '',
        },
        sliceIdsTable: 'wheelSliceId',
        sliceIdsTableRow: wheelSliceId._data[0]._hash || '',
        componentsTable: 'wheelBrand',
        _hash: '',
      },
    ],
    _hash: '',
  }) as LayersTable;

  const wheelDimensionLayerTableCfg = hip<TableCfg>({
    key: 'wheelDimensionLayer',
    type: 'layers',
    columns: [
      { key: '_hash', type: 'string' },
      { key: 'sliceIdsTable', type: 'string' },
      { key: 'sliceIdsTableRow', type: 'string' },
      { key: 'componentsTable', type: 'string' },
      { key: 'add', type: 'json' },
    ],
    isHead: false,
    isRoot: false,
    isShared: true,
  }) as TableCfg;

  const wheelDimensionLayer = hip<any>({
    _tableCfg: wheelDimensionLayerTableCfg._hash,
    _type: 'layers',
    _data: [
      {
        add: {
          BOB37382: wheelDimension._data[0]._hash || '',
          BOB37383: wheelDimension._data[1]._hash || '',
          _hash: '',
        },
        sliceIdsTable: 'wheelSliceId',
        sliceIdsTableRow: wheelSliceId._data[0]._hash || '',
        componentsTable: 'wheelDimension',
        _hash: '',
      },
    ],
    _hash: '',
  }) as LayersTable;

  const wheelCakeTableCfg = hip<TableCfg>({
    key: 'wheelCake',
    type: 'cakes',
    columns: [
      { key: '_hash', type: 'string' },
      { key: 'sliceIdsTable', type: 'string' },
      { key: 'sliceIdsRow', type: 'string' },
      { key: 'layers', type: 'json' },
    ],
    isHead: false,
    isRoot: false,
    isShared: true,
  }) as TableCfg;

  const wheelCake = hip<any>({
    _tableCfg: wheelCakeTableCfg._hash,
    _type: 'cakes',
    _data: [
      {
        sliceIdsTable: 'wheelSliceId',
        sliceIdsRow: wheelSliceId._data[0]._hash || '',
        layers: {
          wheelBrandLayer: wheelBrandLayer._data[0]._hash || '',
          wheelDimensionLayer: wheelDimensionLayer._data[0]._hash || '',
        },
      },
    ],
  }) as CakesTable;

  const tableCfgs = {
    _data: [
      carSliceIdTableCfg,
      carGeneralTableCfg,
      carTechnicalTableCfg,
      carColorTableCfg,
      carGeneralLayerTableCfg,
      carTechnicalLayerTableCfg,
      carColorLayerTableCfg,
      carCakeTableCfg,
      wheelSliceIdTableCfg,
      wheelBrandTableCfg,
      wheelDimensionTableCfg,
      wheelBrandLayerTableCfg,
      wheelDimensionLayerTableCfg,
      wheelCakeTableCfg,
    ],
  } as TablesCfgTable;

  const carsExample = {
    carSliceId,
    carGeneral,
    carTechnical,
    carColor,
    carGeneralLayer,
    carTechnicalLayer,
    carColorLayer,
    carCake,
    wheelSliceId,
    wheelBrand,
    wheelDimension,
    wheelBrandLayer,
    wheelDimensionLayer,
    wheelCake,
    tableCfgs,
  };

  return carsExample;
};
