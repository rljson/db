// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';
import { Json, JsonH, JsonValueH } from '@rljson/json';
import {
  CakesTable,
  ColumnCfg,
  ComponentsTable,
  createCakeTableCfg,
  createLayerTableCfg,
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
  carDimensions: ComponentsTable<CarDimension>;
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

export interface CarGeneral extends JsonH {
  brand: string;
  type: string;
  doors: number;
  energyConsumption: number;
  units: JsonValueH;
  serviceIntervals: number[];
  isElectric: boolean;
}

export interface CarDimension extends JsonH {
  height: number;
  width: number;
  length: number;
}

export interface CarTechnical extends JsonH {
  engine: string;
  transmission: string;
  gears: number;
  carDimensionsRef: string;
}

export const carsExample = (): CarsExample => {
  //CarSliceId
  //................................................................
  const carSliceIdTableCfg = hip<TableCfg>({
    key: 'carSliceId',
    type: 'sliceIds',
    columns: [
      { key: '_hash', type: 'string', titleLong: 'Hash', titleShort: 'Hash' },
      { key: 'add', type: 'jsonArray', titleLong: 'Add', titleShort: 'Add' },
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

  const carGeneral = hip<ComponentsTable<CarGeneral>>({
    _tableCfg: carGeneralTableCfg._hash as string,
    _type: 'components',
    _data: [
      {
        brand: 'Volkswagen',
        type: 'Polo',
        doors: 5,
        energyConsumption: 7.4,
        units: {
          energy: 'l/100km',
          _hash: '',
        },
        serviceIntervals: [15000, 30000, 45000],
        isElectric: false,
        meta: {
          pressText: 'A popular compact car.',
          _hash: '',
        },
        _hash: '',
      },
      {
        brand: 'Volkswagen',
        type: 'Golf',
        doors: 3,
        energyConsumption: 6.2,
        units: {
          energy: 'l/100km',
          _hash: '',
        },
        serviceIntervals: [15000, 30000, 45000],
        isElectric: false,
        meta: {
          pressText: 'A well-known hatchback.',
          _hash: '',
        },
        _hash: '',
      },
      {
        brand: 'Audi',
        type: 'Q4 E-tron',
        doors: 5,
        energyConsumption: 18.0,
        units: {
          energy: 'kWh/100km',
          _hash: '',
        },
        serviceIntervals: [20000, 40000, 60000],
        isElectric: true,
        meta: [
          {
            pressText: 'A stylish electric SUV.',
            _hash: '',
          },
          {
            pressText: 'Combines performance with sustainability.',
            _hash: '',
          },
        ],
        _hash: '',
      },
    ],
    _hash: '',
  }) as ComponentsTable<CarGeneral>;

  //CarDimensions
  //................................................................
  const carDimensionsTableCfg = hip<any>({
    key: 'carDimensions',
    type: 'components',
    columns: [
      { key: '_hash', type: 'string' },
      { key: 'height', type: 'number' },
      { key: 'width', type: 'number' },
      { key: 'length', type: 'number' },
    ],
    isHead: false,
    isRoot: false,
    isShared: true,
  }) as TableCfg;

  const carDimensions = hip<any>({
    _tableCfg: carDimensionsTableCfg._hash,
    _type: 'components',
    _data: [
      {
        height: 1400,
        width: 1800,
        length: 4000,
        _hash: '',
      },
      {
        height: 1450,
        width: 1850,
        length: 4100,
        _hash: '',
      },
    ],
    _hash: '',
  } as ComponentsTable<CarDimension>) as ComponentsTable<CarDimension>;

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
      { key: 'carDimensionsRef', type: 'string' },
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
        carDimensionsRef: carDimensions._data[0]._hash,
        _hash: '',
      },
      {
        engine: 'Petrol',
        transmission: 'Automatic',
        gears: 7,
        carDimensionsRef: carDimensions._data[1]._hash,
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
  const carGeneralLayerTableCfg = hip<TableCfg>(
    createLayerTableCfg('carGeneralLayer'),
  ) as TableCfg;

  const carGeneralLayer = hip<any>({
    _tableCfg: carGeneralLayerTableCfg._hash,
    _type: 'layers',
    _data: [
      {
        add: {
          VIN1: carGeneral._data[0]._hash,
          VIN2: carGeneral._data[1]._hash,
          _hash: '',
        },
        sliceIdsTable: 'carSliceId',
        sliceIdsTableRow: carSliceId._data[0]._hash,
        componentsTable: 'carGeneral',
        _hash: '',
      },
    ],
    _hash: '',
  }) as LayersTable;

  const carTechnicalLayerTableCfg = hip<TableCfg>(
    createLayerTableCfg('carTechnicalLayer'),
  ) as TableCfg;

  const carTechnicalLayer = hip<any>({
    _tableCfg: carTechnicalLayerTableCfg._hash,
    _type: 'layers',
    _data: [
      {
        add: {
          VIN1: carTechnical._data[0]._hash,
          VIN2: carTechnical._data[1]._hash,
          _hash: '',
        },
        sliceIdsTable: 'carSliceId',
        sliceIdsTableRow: carSliceId._data[0]._hash,
        componentsTable: 'carTechnical',
        _hash: '',
      },
    ],
    _hash: '',
  }) as LayersTable;

  const carColorLayerTableCfg = hip<TableCfg>(
    createLayerTableCfg('carColorLayer'),
  ) as TableCfg;

  const carColorLayer = hip<any>({
    _tableCfg: carColorLayerTableCfg._hash,
    _type: 'layers',
    _data: [
      {
        add: {
          VIN1: carColor._data[0]._hash,
          VIN2: carColor._data[1]._hash,
          _hash: '',
        },
        sliceIdsTable: 'carSliceId',
        sliceIdsTableRow: carSliceId._data[0]._hash,
        componentsTable: 'carColor',
        _hash: '',
      },
    ],
    _hash: '',
  }) as LayersTable;

  const carCakeTableCfg = hip<TableCfg>(
    createCakeTableCfg('carCake'),
  ) as TableCfg;

  const carCake = hip<any>({
    _tableCfg: carCakeTableCfg._hash,
    _type: 'cakes',
    _data: [
      {
        sliceIdsTable: 'carSliceId',
        sliceIdsRow: carSliceId._data[0]._hash,
        layers: {
          carGeneralLayer: carGeneralLayer._data[0]._hash,
          carTechnicalLayer: carTechnicalLayer._data[0]._hash,
          carColorLayer: carColorLayer._data[0]._hash,
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
      { key: '_hash', type: 'string', titleLong: 'Hash', titleShort: 'Hash' },
      { key: 'add', type: 'jsonArray', titleLong: 'Add', titleShort: 'Add' },
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

  const wheelBrandLayerTableCfg = hip<TableCfg>(
    createLayerTableCfg('wheelBrandLayer'),
  ) as TableCfg;

  const wheelBrandLayer = hip<any>({
    _tableCfg: wheelBrandLayerTableCfg._hash,
    _type: 'layers',
    _data: [
      {
        add: {
          BOB37382: wheelBrand._data[0]._hash,
          BOB37383: wheelBrand._data[1]._hash,
          _hash: '',
        },
        sliceIdsTable: 'wheelSliceId',
        sliceIdsTableRow: wheelSliceId._data[0]._hash,
        componentsTable: 'wheelBrand',
        _hash: '',
      },
    ],
    _hash: '',
  }) as LayersTable;

  const wheelDimensionLayerTableCfg = hip<TableCfg>(
    createLayerTableCfg('wheelDimensionLayer'),
  ) as TableCfg;

  const wheelDimensionLayer = hip<any>({
    _tableCfg: wheelDimensionLayerTableCfg._hash,
    _type: 'layers',
    _data: [
      {
        add: {
          BOB37382: wheelDimension._data[0]._hash,
          BOB37383: wheelDimension._data[1]._hash,
          _hash: '',
        },
        sliceIdsTable: 'wheelSliceId',
        sliceIdsTableRow: wheelSliceId._data[0]._hash,
        componentsTable: 'wheelDimension',
        _hash: '',
      },
    ],
    _hash: '',
  }) as LayersTable;

  const wheelCakeTableCfg = hip<TableCfg>(
    createCakeTableCfg('wheelCake'),
  ) as TableCfg;

  const wheelCake = hip<any>({
    _tableCfg: wheelCakeTableCfg._hash,
    _type: 'cakes',
    _data: [
      {
        sliceIdsTable: 'wheelSliceId',
        sliceIdsRow: wheelSliceId._data[0]._hash,
        layers: {
          wheelBrandLayer: wheelBrandLayer._data[0]._hash,
          wheelDimensionLayer: wheelDimensionLayer._data[0]._hash,
        },
      },
    ],
  }) as CakesTable;

  const tableCfgs = {
    _data: [
      carSliceIdTableCfg,
      carGeneralTableCfg,
      carDimensionsTableCfg,
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
    carDimensions,
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
