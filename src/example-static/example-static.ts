// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip, hsh, rmhsh } from '@rljson/hash';
import { Json, JsonH, JsonValueH } from '@rljson/json';
import {
  CakeReference,
  CakesTable,
  ColumnCfg,
  ComponentsTable,
  createCakeTableCfg,
  createLayerTableCfg,
  createSliceIdsTableCfg,
  Layer,
  LayersTable,
  Rljson,
  SliceId,
  SliceIds,
  SliceIdsTable,
  TableCfg,
  TablesCfgTable,
} from '@rljson/rljson';

//Corporate Group
//Companies
//Branches
//Sites

export interface StaticExample extends Rljson {
  carSliceId: SliceIdsTable;
  carGeneral: ComponentsTable<CarGeneral>;
  carTechnical: ComponentsTable<Json>;
  carColor: ComponentsTable<Json>;
  carDimensions: ComponentsTable<CarDimension>;
  carGeneralLayer: LayersTable;
  carTechnicalLayer: LayersTable;
  carColorLayer: LayersTable;
  carCake: CakesTable;
  seriesSliceId: SliceIdsTable;
  seriesGeneral: ComponentsTable<Json>;
  seriesCars: ComponentsTable<Json>;
  seriesGeneralLayer: LayersTable;
  seriesCarsLayer: LayersTable;
  seriesCake: CakesTable;
  catalogSliceId: SliceIdsTable;
  catalogSeries: ComponentsTable<Json>;
  catalogSeriesLayer: LayersTable;
  catalogCake: CakesTable;
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

const chainLayers = (layers: Layer[]): Layer[] => {
  const chainedLayers: Layer[] = [];
  for (let i = 0; i < layers.length; i++) {
    const newLayer = { ...rmhsh(layers[i]) };
    if (i == 0) {
      chainedLayers.push(hsh<Layer>(newLayer));
      continue;
    }

    if (chainedLayers[i - 1]._hash) {
      newLayer.base = chainedLayers[i - 1]._hash as string;
    }
    chainedLayers.push(hsh<Layer>(newLayer));
  }
  return chainedLayers;
};

const chainSliceIds = (sliceIds: SliceIds[]): SliceIds[] => {
  const chainedSliceIds: SliceIds[] = [];
  for (let i = 0; i < sliceIds.length; i++) {
    const newSliceIds = { ...rmhsh(sliceIds[i]) };
    if (i == 0) {
      chainedSliceIds.push(hsh<SliceIds>(newSliceIds));
      continue;
    }
    if (chainedSliceIds[i - 1]._hash) {
      newSliceIds.base = chainedSliceIds[i - 1]._hash as string;
    }
    chainedSliceIds.push(hsh<SliceIds>(newSliceIds));
  }
  return chainedSliceIds;
};

export const staticExample = (): StaticExample => {
  //................................................................
  //Car Data Tables
  //................................................................

  //CarSliceId
  //................................................................
  const carSliceIdTableCfg = hip<TableCfg>(
    createSliceIdsTableCfg('carSliceId'),
  ) as TableCfg;

  const carSliceIdData: Array<SliceIds> = [
    {
      add: ['VIN1', 'VIN2', 'VIN3', 'VIN4', 'VIN5', 'VIN6', 'VIN7', 'VIN8'],
      _hash: '',
    } as SliceIds,
    {
      add: ['VIN9', 'VIN10'],
      _hash: '',
    } as SliceIds,
    {
      add: ['VIN11', 'VIN12'],
      _hash: '',
    } as SliceIds,
  ].map((sliceIds) => hsh<SliceIds>(sliceIds as SliceIds));

  const carSliceId = hip<any>({
    _tableCfg: carSliceIdTableCfg._hash,
    _type: 'sliceIds',
    _data: chainSliceIds(carSliceIdData),
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
      {
        brand: 'Audi',
        type: 'Q6 E-tron',
        doors: 5,
        energyConsumption: 16.0,
        units: {
          energy: 'kWh/100km',
          _hash: '',
        },
        serviceIntervals: [20000, 40000, 60000],
        isElectric: true,
        meta: [
          {
            pressText: 'A premium electric SUV with advanced features.',
            _hash: '',
          },
          {
            pressText: 'Offers a blend of luxury and eco-friendliness.',
            _hash: '',
          },
        ],
        _hash: '',
      },
      {
        brand: 'BMW',
        type: 'i4',
        doors: 5,
        energyConsumption: 19.5,
        units: {
          energy: 'kWh/100km',
          _hash: '',
        },
        serviceIntervals: [20000, 40000, 60000],
        isElectric: true,
        meta: [
          {
            pressText: 'A sporty electric sedan.',
            _hash: '',
          },
          {
            pressText: 'Delivers dynamic performance with zero emissions.',
            _hash: '',
          },
        ],
        _hash: '',
      },
      {
        brand: 'BMW',
        type: '3 Series',
        doors: 4,
        energyConsumption: 5.8,
        units: {
          energy: 'l/100km',
          _hash: '',
        },
        serviceIntervals: [15000, 30000, 45000],
        isElectric: false,
        meta: {
          pressText: 'A classic executive car.',
          _hash: '',
        },
        _hash: '',
      },
      {
        brand: 'Tesla',
        type: 'Model 3',
        doors: 4,
        energyConsumption: 15.0,
        units: {
          energy: 'kWh/100km',
          _hash: '',
        },
        serviceIntervals: [25000, 50000, 75000],
        isElectric: true,
        meta: {
          pressText: 'A revolutionary electric sedan.',
          _hash: '',
        },
        _hash: '',
      },
      {
        brand: 'Tesla',
        type: 'Model Y',
        doors: 5,
        energyConsumption: 16.5,
        units: {
          energy: 'kWh/100km',
          _hash: '',
        },
        serviceIntervals: [25000, 50000, 75000],
        isElectric: true,
        meta: {
          pressText: 'A versatile electric SUV.',
          _hash: '',
        },
        _hash: '',
      },
      {
        brand: 'Ford',
        type: 'Mustang Mach-E',
        doors: 5,
        energyConsumption: 17.0,
        units: {
          energy: 'kWh/100km',
          _hash: '',
        },
        serviceIntervals: [20000, 40000, 60000],
        isElectric: true,
        meta: {
          pressText: 'An electric SUV with Mustang heritage.',
          _hash: '',
        },
        _hash: '',
      },
      {
        brand: 'Chevrolet',
        type: 'Bolt EV',
        doors: 5,
        energyConsumption: 14.0,
        units: {
          energy: 'kWh/100km',
          _hash: '',
        },
        serviceIntervals: [20000, 40000, 60000],
        isElectric: true,
        meta: {
          pressText: 'An affordable electric hatchback.',
          _hash: '',
        },
        _hash: '',
      },
      {
        brand: 'Nissan',
        type: 'Leaf',
        doors: 5,
        energyConsumption: 15.5,
        units: {
          energy: 'kWh/100km',
          _hash: '',
        },
        serviceIntervals: [20000, 40000, 60000],
        isElectric: true,
        meta: {
          pressText: 'A pioneering electric vehicle.',
          _hash: '',
        },
        _hash: '',
      },
      {
        brand: 'Hyundai',
        type: 'Kona Electric',
        doors: 5,
        energyConsumption: 14.5,
        units: {
          energy: 'kWh/100km',
          _hash: '',
        },
        serviceIntervals: [20000, 40000, 60000],
        isElectric: true,
        meta: {
          pressText: 'A compact electric SUV.',
          _hash: '',
        },
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
      {
        height: 1600,
        width: 1900,
        length: 4500,
        _hash: '',
      },
      {
        height: 1650,
        width: 1950,
        length: 4700,
        _hash: '',
      },
      {
        height: 1500,
        width: 1820,
        length: 4200,
        _hash: '',
      },
      {
        height: 1550,
        width: 1880,
        length: 4300,
        _hash: '',
      },
      {
        height: 1450,
        width: 1830,
        length: 4150,
        _hash: '',
      },
      {
        height: 1700,
        width: 2000,
        length: 4800,
        _hash: '',
      },
      {
        height: 1350,
        width: 1750,
        length: 3900,
        _hash: '',
      },
      {
        height: 1750,
        width: 2050,
        length: 4900,
        _hash: '',
      },
      {
        height: 1600,
        width: 1920,
        length: 4600,
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
      {
        key: 'dimensions',
        type: 'jsonValue',
        ref: {
          tableKey: 'carDimensions',
          type: 'components',
        },
      } as ColumnCfg,
      { key: 'repairedByWorkshop', type: 'string' },
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
        dimensions: carDimensions._data[0]._hash,
        _hash: '',
      },
      {
        engine: 'Petrol',
        transmission: 'Automatic',
        gears: 7,
        dimensions: carDimensions._data[1]._hash,
        repairedByWorkshop: 'Workshop A',
        _hash: '',
      },
      {
        engine: 'Electric',
        transmission: 'Single-Speed',
        gears: 1,
        dimensions: [
          carDimensions._data[1]._hash,
          carDimensions._data[2]._hash,
        ],
        _hash: '',
      },
      {
        engine: 'Electric',
        transmission: 'Single-Speed',
        gears: 1,
        dimensions: carDimensions._data[3]._hash,
        repairedByWorkshop: 'Workshop B',
        _hash: '',
      },
      {
        engine: 'Electric',
        transmission: 'Single-Speed',
        gears: 1,
        dimensions: carDimensions._data[4]._hash,
        _hash: '',
      },
      {
        engine: 'Petrol',
        transmission: 'Manual',
        gears: 6,
        dimensions: carDimensions._data[5]._hash,
        repairedByWorkshop: 'Workshop A',
        _hash: '',
      },
      {
        engine: 'Electric',
        transmission: 'Single-Speed',
        gears: 1,
        dimensions: carDimensions._data[6]._hash,
        _hash: '',
      },
      {
        engine: 'Electric',
        transmission: 'Single-Speed',
        gears: 1,
        dimensions: carDimensions._data[7]._hash,
        repairedByWorkshop: 'Workshop C',
        _hash: '',
      },
      {
        engine: 'Electric',
        transmission: 'Single-Speed',
        gears: 1,
        dimensions: carDimensions._data[8]._hash,
        _hash: '',
      },
      {
        engine: 'Electric',
        transmission: 'Single-Speed',
        gears: 1,
        dimensions: carDimensions._data[9]._hash,
        repairedByWorkshop: 'Workshop B',
        _hash: '',
      },
      {
        engine: 'Electric',
        transmission: 'Single-Speed',
        gears: 1,
        dimensions: carDimensions._data[10]._hash,
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
      {
        sides: 'red',
        roof: 'red',
        highlights: 'black',
        _hash: '',
      },
      {
        sides: 'silver',
        roof: 'silver',
        highlights: 'chrome',
        _hash: '',
      },
      {
        sides: 'black',
        roof: 'black',
        highlights: 'black',
        _hash: '',
      },
      {
        sides: 'white',
        roof: 'white',
        highlights: 'chrome',
        _hash: '',
      },
      {
        sides: 'grey',
        roof: 'black',
        highlights: 'chrome',
        _hash: '',
      },
      {
        sides: 'red',
        roof: 'white',
        highlights: 'black',
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

  const carGeneralLayerData: Array<Layer> = [
    {
      add: {
        VIN1: carGeneral._data[0]._hash,
        VIN2: carGeneral._data[1]._hash,
        VIN3: carGeneral._data[2]._hash,
        VIN4: carGeneral._data[3]._hash,
        VIN5: carGeneral._data[4]._hash,
        VIN6: carGeneral._data[5]._hash,
        VIN7: carGeneral._data[6]._hash,
        VIN8: carGeneral._data[7]._hash,
        _hash: '',
      },
      sliceIdsTable: 'carSliceId',
      sliceIdsTableRow: carSliceId._data[0]._hash as string,
      componentsTable: 'carGeneral',
      _hash: '',
    } as Layer,
    {
      add: {
        VIN9: carGeneral._data[8]._hash,
        VIN10: carGeneral._data[9]._hash,
        _hash: '',
      },
      sliceIdsTable: 'carSliceId',
      sliceIdsTableRow: carSliceId._data[1]._hash as string,
      componentsTable: 'carGeneral',
      _hash: '',
    },
    {
      add: {
        VIN11: carGeneral._data[10]._hash,
        VIN12: carGeneral._data[11]._hash,
        _hash: '',
      },
      sliceIdsTable: 'carSliceId',
      sliceIdsTableRow: carSliceId._data[2]._hash as string,
      componentsTable: 'carGeneral',
      _hash: '',
    },
  ].map((layer) => hsh<Layer>(layer as Layer));

  const carGeneralLayer = hip<any>({
    _tableCfg: carGeneralLayerTableCfg._hash,
    _type: 'layers',
    _data: chainLayers(carGeneralLayerData),
    _hash: '',
  }) as LayersTable;

  const carTechnicalLayerTableCfg = hip<TableCfg>(
    createLayerTableCfg('carTechnicalLayer'),
  ) as TableCfg;

  const carTechnicalLayerData: Array<Layer> = [
    {
      add: {
        VIN1: carTechnical._data[0]._hash,
        VIN2: carTechnical._data[1]._hash,
        VIN3: carTechnical._data[2]._hash,
        VIN4: carTechnical._data[2]._hash,
        VIN5: carTechnical._data[4]._hash,
        VIN6: carTechnical._data[5]._hash,
        VIN7: carTechnical._data[6]._hash,
        VIN8: carTechnical._data[2]._hash,
        _hash: '',
      },
      sliceIdsTable: 'carSliceId',
      sliceIdsTableRow: carSliceId._data[0]._hash,
      componentsTable: 'carTechnical',
      _hash: '',
    } as Layer,
    {
      add: {
        VIN9: carTechnical._data[7]._hash,
        VIN10: carTechnical._data[8]._hash,
        _hash: '',
      },
      sliceIdsTable: 'carSliceId',
      sliceIdsTableRow: carSliceId._data[1]._hash,
      componentsTable: 'carTechnical',
      _hash: '',
    } as Layer,
    {
      add: {
        VIN11: carTechnical._data[9]._hash,
        VIN12: carTechnical._data[10]._hash,
        _hash: '',
      },
      sliceIdsTable: 'carSliceId',
      sliceIdsTableRow: carSliceId._data[2]._hash,
      componentsTable: 'carTechnical',
      _hash: '',
    } as Layer,
  ].map((layer) => hsh<Layer>(layer as Layer));

  const carTechnicalLayer = hip<any>({
    _tableCfg: carTechnicalLayerTableCfg._hash,
    _type: 'layers',
    _data: chainLayers(carTechnicalLayerData),
    _hash: '',
  }) as LayersTable;

  const carColorLayerTableCfg = hip<TableCfg>(
    createLayerTableCfg('carColorLayer'),
  ) as TableCfg;

  const carColorLayerData: Array<Layer> = [
    {
      add: {
        VIN1: carColor._data[0]._hash,
        VIN2: carColor._data[1]._hash,
        VIN3: carColor._data[2]._hash,
        VIN4: carColor._data[3]._hash,
        VIN5: carColor._data[4]._hash,
        VIN6: carColor._data[5]._hash,
        VIN7: carColor._data[6]._hash,
        VIN8: carColor._data[7]._hash,
        _hash: '',
      },
      sliceIdsTable: 'carSliceId',
      sliceIdsTableRow: carSliceId._data[0]._hash,
      componentsTable: 'carColor',
      _hash: '',
    } as Layer,
    {
      add: {
        VIN9: carColor._data[3]._hash,
        VIN10: carColor._data[3]._hash,
        _hash: '',
      },
      sliceIdsTable: 'carSliceId',
      sliceIdsTableRow: carSliceId._data[1]._hash,
      componentsTable: 'carColor',
      _hash: '',
    } as Layer,
    {
      add: {
        VIN11: carColor._data[7]._hash,
        VIN12: carColor._data[4]._hash,
        _hash: '',
      },
      sliceIdsTable: 'carSliceId',
      sliceIdsTableRow: carSliceId._data[2]._hash,
      componentsTable: 'carColor',
      _hash: '',
    } as Layer,
  ].map((layer) => hsh<Layer>(layer as Layer));

  const carColorLayer = hip<any>({
    _tableCfg: carColorLayerTableCfg._hash,
    _type: 'layers',
    _data: chainLayers(carColorLayerData),
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
      {
        sliceIdsTable: 'carSliceId',
        sliceIdsRow: carSliceId._data[1]._hash,
        layers: {
          carGeneralLayer: carGeneralLayer._data[1]._hash,
          carTechnicalLayer: carTechnicalLayer._data[1]._hash,
          carColorLayer: carColorLayer._data[1]._hash,
        },
      },
      {
        sliceIdsTable: 'carSliceId',
        sliceIdsRow: carSliceId._data[2]._hash,
        layers: {
          carGeneralLayer: carGeneralLayer._data[2]._hash,
          carTechnicalLayer: carTechnicalLayer._data[2]._hash,
          carColorLayer: carColorLayer._data[2]._hash,
        },
      },
    ],
  }) as CakesTable;

  //................................................................
  //Series Data Tables
  //................................................................

  //SeriesSliceId
  //................................................................

  const seriesSliceIdTableCfg = hip<TableCfg>(
    createSliceIdsTableCfg('seriesSliceId'),
  ) as TableCfg;

  const seriesSliceIdData: Array<SliceIds> = [
    {
      add: ['Serie0', 'Serie1', 'Serie2', 'Serie3'],
      _hash: '',
    } as SliceIds,
    {
      add: ['Serie4', 'Serie5', 'Serie6', 'Serie7'],
      _hash: '',
    } as SliceIds,
    {
      add: ['Serie8', 'Serie9', 'Serie10', 'Serie11'],
      _hash: '',
    } as SliceIds,
  ].map((sliceIds) => hsh<SliceIds>(sliceIds as SliceIds));

  const seriesSliceId = hip<any>({
    _tableCfg: seriesSliceIdTableCfg._hash,
    _type: 'sliceIds',
    _data: chainSliceIds(seriesSliceIdData),
    _hash: '',
  }) as SliceIdsTable;

  //SeriesGeneral
  //................................................................
  const seriesGeneralTableCfg = hip<TableCfg>({
    key: 'seriesGeneral',
    type: 'components',
    columns: [
      { key: '_hash', type: 'string', titleShort: 'Hash', titleLong: 'Hash' },
      { key: 'name', type: 'string', titleShort: 'Name', titleLong: 'Name' },
    ] as ColumnCfg[],
    isHead: false,
    isRoot: false,
    isShared: true,
  } as TableCfg);

  const seriesGeneral = hip<ComponentsTable<Json>>({
    _tableCfg: seriesGeneralTableCfg._hash as string,
    _type: 'components',
    _data: [
      {
        name: 'Polo',
        _hash: '',
      },
      {
        name: 'Golf',
        _hash: '',
      },
      {
        name: 'Q4',
        _hash: '',
      },
      {
        name: 'Q6',
        _hash: '',
      },
      {
        name: 'i4',
        _hash: '',
      },
      {
        name: '3',
        _hash: '',
      },
      {
        name: 'Model 3',
        _hash: '',
      },
      {
        name: 'Model Y',
        _hash: '',
      },
      {
        name: 'Mustang',
        _hash: '',
      },
      {
        name: 'Bolt',
        _hash: '',
      },
      {
        name: 'Leaf',
        _hash: '',
      },
      {
        name: 'Kona',
        _hash: '',
      },
    ],
    _hash: '',
  }) as ComponentsTable<Json>;

  //................................................................
  //SeriesCars

  const seriesCarsTableCfg = hip<TableCfg>({
    key: 'seriesCars',
    type: 'components',
    columns: [
      { key: '_hash', type: 'string', titleShort: 'Hash', titleLong: 'Hash' },
      {
        key: 'cars',
        type: 'jsonArray',
        titleShort: 'Cars',
        titleLong: 'Cars References',
        ref: {
          tableKey: 'carCake',
          type: 'cakes',
        },
      } as ColumnCfg,
    ] as ColumnCfg[],
    isHead: false,
    isRoot: false,
    isShared: true,
  } as TableCfg);

  const seriesCars = hip<
    ComponentsTable<{ _hash: string; cars: CakeReference[] }>
  >({
    _tableCfg: seriesCarsTableCfg._hash as string,
    _type: 'components',
    _data: [
      {
        cars: [
          {
            ref: carCake._data[2]._hash as string,
            sliceIds: ['VIN1'] as SliceId[],
          },
        ],
        _hash: '',
      },
      {
        cars: [
          {
            ref: carCake._data[2]._hash as string,
            sliceIds: ['VIN2'] as SliceId[],
          },
        ],
        _hash: '',
      },
      {
        cars: [
          {
            ref: carCake._data[0]._hash as string,
            sliceIds: ['VIN3'] as SliceId[],
          },
        ],
        _hash: '',
      },
      {
        cars: [
          {
            ref: carCake._data[0]._hash as string,
            sliceIds: ['VIN4'] as SliceId[],
          },
        ],
        _hash: '',
      },
      {
        cars: [
          {
            ref: carCake._data[1]._hash as string,
            sliceIds: ['VIN5'] as SliceId[],
          },
        ],
        _hash: '',
      },
      {
        cars: [
          {
            ref: carCake._data[1]._hash as string,
            sliceIds: ['VIN6'] as SliceId[],
          },
        ],
        _hash: '',
      },
      {
        cars: [
          {
            ref: carCake._data[2]._hash as string,
            sliceIds: ['VIN7'] as SliceId[],
          },
        ],
        _hash: '',
      },
      {
        cars: [
          {
            ref: carCake._data[2]._hash as string,
            sliceIds: ['VIN8'] as SliceId[],
          },
        ],
        _hash: '',
      },
      {
        cars: [
          {
            ref: carCake._data[2]._hash as string,
            sliceIds: ['VIN9'] as SliceId[],
          },
        ],
        _hash: '',
      },
      {
        cars: [
          {
            ref: carCake._data[2]._hash as string,
            sliceIds: ['VIN10'] as SliceId[],
          },
        ],
        _hash: '',
      },
      {
        cars: [
          {
            ref: carCake._data[2]._hash as string,
            sliceIds: ['VIN11'] as SliceId[],
          },
        ],
        _hash: '',
      },
      {
        cars: [
          {
            ref: carCake._data[2]._hash as string,
            sliceIds: ['VIN12'] as SliceId[],
          },
        ],
        _hash: '',
      },
    ],
    _hash: '',
  });

  //SeriesLayers and SeriesCake
  //................................................................

  const seriesGeneralLayerTableCfg = hip<TableCfg>(
    createLayerTableCfg('seriesGeneralLayer'),
  ) as TableCfg;

  const seriesGeneralLayerData: Array<Layer> = [
    {
      add: {
        Serie0: seriesGeneral._data[0]._hash,
        Serie1: seriesGeneral._data[1]._hash,
        Serie2: seriesGeneral._data[2]._hash,
        Serie3: seriesGeneral._data[3]._hash,
        _hash: '',
      },
      sliceIdsTable: 'seriesSliceId',
      sliceIdsTableRow: seriesSliceId._data[0]._hash as string,
      componentsTable: 'seriesGeneral',
      _hash: '',
    } as Layer,
    {
      add: {
        Serie4: seriesGeneral._data[4]._hash,
        Serie5: seriesGeneral._data[5]._hash,
        Serie6: seriesGeneral._data[6]._hash,
        Serie7: seriesGeneral._data[7]._hash,
        _hash: '',
      },
      sliceIdsTable: 'seriesSliceId',
      sliceIdsTableRow: seriesSliceId._data[1]._hash as string,
      componentsTable: 'seriesGeneral',
      _hash: '',
    },
    {
      add: {
        Serie8: seriesGeneral._data[8]._hash,
        Serie9: seriesGeneral._data[9]._hash,
        Serie10: seriesGeneral._data[10]._hash,
        Serie11: seriesGeneral._data[11]._hash,
        _hash: '',
      },
      sliceIdsTable: 'seriesSliceId',
      sliceIdsTableRow: seriesSliceId._data[2]._hash as string,
      componentsTable: 'seriesGeneral',
      _hash: '',
    },
  ].map((layer) => hsh<Layer>(layer as Layer));

  const seriesGeneralLayer = hip<any>({
    _tableCfg: seriesGeneralLayerTableCfg._hash,
    _type: 'layers',
    _data: chainLayers(seriesGeneralLayerData),
    _hash: '',
  }) as LayersTable;

  const seriesCarsLayerTableCfg = hip<TableCfg>(
    createLayerTableCfg('seriesCarsLayer'),
  ) as TableCfg;

  const seriesCarsLayerData: Array<Layer> = [
    {
      add: {
        Serie0: seriesCars._data[0]._hash,
        Serie1: seriesCars._data[1]._hash,
        Serie2: seriesCars._data[2]._hash,
        Serie3: seriesCars._data[3]._hash,
        _hash: '',
      },
      sliceIdsTable: 'seriesSliceId',
      sliceIdsTableRow: seriesSliceId._data[0]._hash,
      componentsTable: 'seriesCars',
      _hash: '',
    } as Layer,
    {
      add: {
        Serie4: seriesCars._data[4]._hash,
        Serie5: seriesCars._data[5]._hash,
        Serie6: seriesCars._data[6]._hash,
        Serie7: seriesCars._data[7]._hash,
        _hash: '',
      },
      sliceIdsTable: 'seriesSliceId',
      sliceIdsTableRow: seriesSliceId._data[1]._hash,
      componentsTable: 'seriesCars',
      _hash: '',
    } as Layer,
    {
      add: {
        Serie8: seriesCars._data[8]._hash,
        Serie9: seriesCars._data[9]._hash,
        Serie10: seriesCars._data[10]._hash,
        Serie11: seriesCars._data[11]._hash,
        _hash: '',
      },
      sliceIdsTable: 'seriesSliceId',
      sliceIdsTableRow: seriesSliceId._data[2]._hash,
      componentsTable: 'seriesCars',
      _hash: '',
    } as Layer,
  ].map((layer) => hsh<Layer>(layer as Layer));

  const seriesCarsLayer = hip<any>({
    _tableCfg: seriesCarsLayerTableCfg._hash,
    _type: 'layers',
    _data: chainLayers(seriesCarsLayerData),
  });

  const seriesCakeTableCfg = hip<TableCfg>(
    createCakeTableCfg('seriesCake'),
  ) as TableCfg;

  const seriesCake = hip<any>({
    _tableCfg: seriesCakeTableCfg._hash,
    _type: 'cakes',
    _data: [
      {
        sliceIdsTable: 'seriesSliceId',
        sliceIdsRow: seriesSliceId._data[0]._hash,
        layers: {
          seriesGeneralLayer: seriesGeneralLayer._data[0]._hash,
          seriesCarsLayer: seriesCarsLayer._data[0]._hash,
        },
      },
      {
        sliceIdsTable: 'seriesSliceId',
        sliceIdsRow: seriesSliceId._data[1]._hash,
        layers: {
          seriesGeneralLayer: seriesGeneralLayer._data[1]._hash,
          seriesCarsLayer: seriesCarsLayer._data[1]._hash,
        },
      },
      {
        sliceIdsTable: 'seriesSliceId',
        sliceIdsRow: seriesSliceId._data[2]._hash,
        layers: {
          seriesGeneralLayer: seriesGeneralLayer._data[2]._hash,
          seriesCarsLayer: seriesCarsLayer._data[2]._hash,
        },
      },
    ],
  }) as CakesTable;

  //................................................................
  //Series Data Tables
  //................................................................

  //SeriesSliceId
  //................................................................

  const catalogSliceIdTableCfg = hip<TableCfg>(
    createSliceIdsTableCfg('catalogSliceId'),
  ) as TableCfg;

  const catalogSliceIdData: Array<SliceIds> = [
    {
      add: ['Catalog0', 'Catalog1'],
      _hash: '',
    } as SliceIds,
  ].map((sliceIds) => hsh<SliceIds>(sliceIds as SliceIds));

  const catalogSliceId = hip<any>({
    _tableCfg: catalogSliceIdTableCfg._hash,
    _type: 'sliceIds',
    _data: chainSliceIds(catalogSliceIdData),
    _hash: '',
  }) as SliceIdsTable;

  //CatalogSeries
  //................................................................
  const catalogSeriesTableCfg = hip<TableCfg>({
    key: 'catalogSeries',
    type: 'components',
    columns: [
      { key: '_hash', type: 'string', titleShort: 'Hash', titleLong: 'Hash' },
      {
        key: 'series',
        type: 'jsonArray',
        titleShort: 'Series',
        titleLong: 'Series References',
        ref: {
          tableKey: 'seriesCake',
          type: 'cakes',
        },
      } as ColumnCfg,
    ] as ColumnCfg[],
    isHead: false,
    isRoot: false,
    isShared: true,
  } as TableCfg);

  const catalogSeries = hip<
    ComponentsTable<{ _hash: string; series: CakeReference[] }>
  >({
    _tableCfg: catalogSeriesTableCfg._hash as string,
    _type: 'components',
    _data: [
      {
        series: [
          {
            ref: seriesCake._data[0]._hash as string,
            sliceIds: ['Serie0', 'Serie1', 'Serie2', 'Serie3'] as SliceId[],
          },
          {
            ref: seriesCake._data[1]._hash as string,
            sliceIds: ['Serie4', 'Serie5', 'Serie6', 'Serie7'] as SliceId[],
          },
        ],
        _hash: '',
      },
      {
        series: [
          {
            ref: seriesCake._data[2]._hash as string,
          },
        ],
        _hash: '',
      },
    ],
    _hash: '',
  });

  //CatalogLayers and CatalogCake
  //................................................................

  const catalogSeriesLayerTableCfg = hip<TableCfg>(
    createLayerTableCfg('catalogSeriesLayer'),
  ) as TableCfg;

  const catalogSeriesLayerData: Array<Layer> = [
    {
      add: {
        Catalog0: catalogSeries._data[0]._hash,
        Catalog1: catalogSeries._data[1]._hash,
        _hash: '',
      },
      sliceIdsTable: 'catalogSliceId',
      sliceIdsTableRow: catalogSliceId._data[0]._hash as string,
      componentsTable: 'catalogSeries',
      _hash: '',
    } as Layer,
  ].map((layer) => hsh<Layer>(layer as Layer));

  const catalogSeriesLayer = hip<any>({
    _tableCfg: catalogSeriesLayerTableCfg._hash,
    _type: 'layers',
    _data: chainLayers(catalogSeriesLayerData),
    _hash: '',
  }) as LayersTable;

  const catalogCakeTableCfg = hip<TableCfg>(
    createCakeTableCfg('catalogCake'),
  ) as TableCfg;

  const catalogCake = hip<any>({
    _tableCfg: catalogCakeTableCfg._hash,
    _type: 'cakes',
    _data: [
      {
        sliceIdsTable: 'catalogSliceId',
        sliceIdsRow: catalogSliceId._data[0]._hash,
        layers: {
          catalogSeriesLayer: catalogSeriesLayer._data[0]._hash,
        },
      },
    ],
  }) as CakesTable;

  //................................................................
  //TablesCfg
  //................................................................
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
      seriesSliceIdTableCfg,
      seriesGeneralTableCfg,
      seriesCarsTableCfg,
      seriesGeneralLayerTableCfg,
      seriesCarsLayerTableCfg,
      seriesCakeTableCfg,
      catalogSliceIdTableCfg,
      catalogSeriesTableCfg,
      catalogSeriesLayerTableCfg,
      catalogCakeTableCfg,
    ],
  } as TablesCfgTable;

  const staticExample = {
    carSliceId,
    carGeneral,
    carDimensions,
    carTechnical,
    carColor,
    carGeneralLayer,
    carTechnicalLayer,
    carColorLayer,
    carCake,
    seriesSliceId,
    seriesGeneral,
    seriesCars,
    seriesGeneralLayer,
    seriesCarsLayer,
    seriesCake,
    catalogSliceId,
    catalogSeries,
    catalogSeriesLayer,
    catalogCake,
    tableCfgs,
  };

  return staticExample;
};
