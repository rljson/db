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

const carSliceId = {
  _type: 'sliceIds',
  _data: [
    {
      add: ['VIN1', 'VIN2'],
      _hash: 'gaY8E_pzUVZYJT1RTMWsrh',
    },
  ],
  _hash: '8CPu6Tw2zuVzD1j6cr--Xp',
} as SliceIdsTable;

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

const carGeneralTableCfgs = {
  _data: [carGeneralTableCfg],
} as TablesCfgTable;

const carGeneral = {
  _tableCfg: carGeneralTableCfg._hash,
  _type: 'components',
  _data: [
    {
      brand: 'Volkswagen',
      type: 'Polo',
      doors: 5,
      _hash: 'kGUWVQqc3s04pEFCrAhPBU',
    },
    {
      brand: 'Volkswagen',
      type: 'Golf',
      doors: 3,
      _hash: '32MNv-aROHGLPpb96BMIqq',
    },
  ],
  _hash: 'r7D4l_Jc472Bkaw9-deTjq',
} as ComponentsTable<Json>;

const carTechnical = {
  _type: 'components',
  _data: [
    {
      engine: 'Diesel',
      transmission: 'Manual',
      gears: 6,
      _hash: 'bS4c-w-oYiodBMxv4cRPXs',
    },
    {
      engine: 'Petrol',
      transmission: 'Automatic',
      gears: 7,
      _hash: 'cE6GU1sf4CvCGuGozsgaa7',
    },
  ],
  _hash: 'yERt7m3fnOwmFObYk_DWRz',
} as ComponentsTable<Json>;

const carColor = {
  _type: 'components',
  _data: [
    {
      sides: 'green',
      roof: 'white',
      highlights: 'chrome',
      _hash: 'aCjcRQw-KWRYJe6ttLx-Un',
    },
    {
      sides: 'blue',
      roof: 'black',
      highlights: 'chrome',
      _hash: '5vRZZ-vTbp1zhman-kDIV3',
    },
  ],
  _hash: 'uB6She7CuR0X9A4_qK5Q5U',
} as ComponentsTable<Json>;

const carWheel = {
  _type: 'components',
  _data: [
    {
      _hash: 'RBNvo1WzZ4oRRq0W9-hknp',
    },
    {
      _hash: 'RBNvo1WzZ4oRRq0W9-hknp',
    },
  ],
  _hash: 'du-6IBU7HSLFj3IqLTN9Pp',
} as ComponentsTable<Json>;

const carGeneralLayer = {
  _type: 'layers',
  _data: [
    {
      add: {
        VIN1: 'kGUWVQqc3s04pEFCrAhPBU',
        VIN2: '32MNv-aROHGLPpb96BMIqq',
        _hash: 'FGfUNShhXdib0UXiIyQHSs',
      },
      sliceIdsTable: 'carSliceId',
      sliceIdsTableRow: 'gaY8E_pzUVZYJT1RTMWsrh',
      componentsTable: 'carGeneral',
      _hash: 'Gv1hBZjCrOPaW1T9qwB7i1',
    },
  ],
  _hash: 'l1EqfFPrepjZPGklNgZm2T',
} as LayersTable;

const carTechnicalLayer = {
  _type: 'layers',
  _data: [
    {
      add: {
        VIN1: 'bS4c-w-oYiodBMxv4cRPXs',
        VIN2: 'cE6GU1sf4CvCGuGozsgaa7',
        _hash: 'vui_APtNHzJHtoYF7-n228',
      },
      sliceIdsTable: 'carSliceId',
      sliceIdsTableRow: 'gaY8E_pzUVZYJT1RTMWsrh',
      componentsTable: 'carTechnical',
      _hash: '3FqkEWxnPT6ctdPHk04XBL',
    },
  ],
  _hash: 'wSHzbkD7z3rBsLvq3ReY0w',
} as LayersTable;

const carColorLayer = {
  _type: 'layers',
  _data: [
    {
      add: {
        VIN1: 'aCjcRQw-KWRYJe6ttLx-Un',
        VIN2: '5vRZZ-vTbp1zhman-kDIV3',
        _hash: '7_J6jgSc-UYL5SZYS4ZgL0',
      },
      sliceIdsTable: 'carSliceId',
      sliceIdsTableRow: 'gaY8E_pzUVZYJT1RTMWsrh',
      componentsTable: 'carColor',
      _hash: '3-Xak37KCHtV-LXU084JCt',
    },
  ],
  _hash: '2CPFGUver5DOm8s0_3WPQT',
} as LayersTable;

const carWheelLayer = {
  _type: 'layers',
  _data: [
    {
      add: {
        VIN1: 'RBNvo1WzZ4oRRq0W9-hknp',
        VIN2: 'RBNvo1WzZ4oRRq0W9-hknp',
        _hash: 'OJtYyRy9yW1COM_YBlCUH2',
      },
      sliceIdsTable: 'carSliceId',
      sliceIdsTableRow: 'gaY8E_pzUVZYJT1RTMWsrh',
      componentsTable: 'carWheel',
      _hash: 'tYWK2dZbgVJv2xmI5knESg',
    },
  ],
  _hash: '6qlGNIlZgHHttBg8DAQNro',
} as LayersTable;

const carCake = {
  _type: 'cakes',
  _data: [
    {
      sliceIdsTable: 'carSliceId',
      sliceIdsRow: 'gaY8E_pzUVZYJT1RTMWsrh',
      layers: {
        carGeneralLayer: 'Gv1hBZjCrOPaW1T9qwB7i1',
        carTechnicalLayer: '3FqkEWxnPT6ctdPHk04XBL',
        carColorLayer: '3-Xak37KCHtV-LXU084JCt',
        carWheelLayer: 'tYWK2dZbgVJv2xmI5knESg',
      },
    },
  ],
} as CakesTable;

const wheelSliceId = {
  _type: 'sliceIds',
  _data: [
    {
      add: ['BOB37382', 'BOB37383'],
      _hash: 'JPP7A-skpEe1KuDS11mtBH',
    },
  ],
  _hash: '3yLoZHlJMhlsY7SvmpNBg1',
} as SliceIdsTable;

const wheelBrand = {
  _type: 'components',
  _data: [
    {
      brand: 'Borbet',
      _hash: 'VQxyOOlL5pVCWUoteV_x8V',
    },
    {
      brand: 'Borbet',
      _hash: 'VQxyOOlL5pVCWUoteV_x8V',
    },
  ],
  _hash: '5rbdtYEHQzUvEvC_85h-vx',
} as ComponentsTable<Json>;

const wheelDimension = {
  _type: 'components',
  _data: [
    {
      dimension: '185/60 R16',
      _hash: 'pQB1jQ9uRxoYAcVc25VA20',
    },
    {
      dimension: '195/55 R16',
      _hash: '7G8GafBJu0ohK_1lcTWgEn',
    },
  ],
  _hash: 'NxACjUL9cXgXcalytGsor3',
} as ComponentsTable<Json>;

const wheelBrandLayer = {
  _type: 'layers',
  _data: [
    {
      add: {
        BOB37382: 'VQxyOOlL5pVCWUoteV_x8V',
        BOB37383: 'VQxyOOlL5pVCWUoteV_x8V',
        _hash: 'Iwr3s5WQxdptlIlbsV6FHu',
      },
      sliceIdsTable: 'wheelSliceId',
      sliceIdsTableRow: 'JPP7A-skpEe1KuDS11mtBH',
      componentsTable: 'wheelBrand',
      _hash: 'QtYdaYaOgULRv2VToNq2td',
    },
  ],
  _hash: 'xB4Dkjv1IwTVUTy5reV4CO',
} as LayersTable;

const wheelDimensionLayer = {
  _type: 'layers',
  _data: [
    {
      add: {
        BOB37382: 'pQB1jQ9uRxoYAcVc25VA20',
        BOB37383: '7G8GafBJu0ohK_1lcTWgEn',
        _hash: '4bIVTR_l-negIVz-bVhb-L',
      },
      sliceIdsTable: 'wheelSliceId',
      sliceIdsTableRow: 'JPP7A-skpEe1KuDS11mtBH',
      componentsTable: 'wheelDimension',
      _hash: '3wKgawhS1yjWlNvS8f85qc',
    },
  ],
  _hash: 'bsdPHbqts6OWx2E-jWXIg6',
} as LayersTable;

const wheelCake = {
  _type: 'cakes',
  _data: [
    {
      sliceIdsTable: 'wheelSliceId',
      sliceIdsRow: 'JPP7A-skpEe1KuDS11mtBH',
      layers: {
        wheelBrandLayer: 'QtYdaYaOgULRv2VToNq2td',
        wheelDimensionLayer: '3wKgawhS1yjWlNvS8f85qc',
      },
    },
  ],
} as CakesTable;

export const carsExample = {
  carSliceId,
  carGeneral,
  carGeneralTableCfgs,
  carTechnical,
  carColor,
  carWheel,
  carGeneralLayer,
  carTechnicalLayer,
  carColorLayer,
  carWheelLayer,
  carCake,
  wheelSliceId,
  wheelBrand,
  wheelDimension,
  wheelBrandLayer,
  wheelDimensionLayer,
  wheelCake,
} as Rljson;
