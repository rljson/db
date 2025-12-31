// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

/* v8 ignore file -- @preserve */

import { DecomposeChart, fromJson } from '@rljson/converter';
import { Rljson } from '@rljson/rljson';

import { readFileSync, writeFileSync } from 'node:fs';


const decompose: DecomposeChart = {
  _sliceId: 'VIN',
  _name: 'Car',
  _skipLayerCreation: ['length', 'width', 'height'],
  general: [
    { origin: 'carGeneral:brand', destination: 'brand' },
    { origin: 'carGeneral:type', destination: 'type' },
    { origin: 'carGeneral:door', destination: 'doors', type: 'number' },
    {
      origin: 'carGeneral:energyConsumption',
      destination: 'energyConsumption',
      type: 'number',
    },
    {
      origin: 'carGeneral:isElectric',
      destination: 'isElectric',
      type: 'boolean',
    },
  ],
  dimensions: {
    length: [
      { origin: 'carDimensions:length', destination: 'length', type: 'number' },
    ],
    width: [
      { origin: 'carDimensions:width', destination: 'width', type: 'number' },
    ],
    height: [
      { origin: 'carDimensions:height', destination: 'height', type: 'number' },
    ],
  },
  technical: [
    { origin: 'carTechnical:engine', destination: 'engine' },
    { origin: 'carTechnical:transmission', destination: 'transmission' },
    { origin: 'carTechnical:gears', destination: 'gears', type: 'number' },
  ],
  color: [
    { origin: 'carColors:sides', destination: 'sides' },
    { origin: 'carColors:roof', destination: 'roof' },
    { origin: 'carColors:highlights', destination: 'highlights' },
  ],
};

export const convertMassData: () => {
  written: boolean;
  result: Rljson;
} = () => {
  const raw = readFileSync(new URL('./data.json', import.meta.url), 'utf-8');
  const json = JSON.parse(raw);

  const output = fromJson(json, decompose);

  try {
    writeFileSync(
      new URL('./data-converted.rljson.json', import.meta.url),
      JSON.stringify(output, null, 2),
      'utf-8',
    );
  } catch (error) {
    if (error instanceof Error) {
      console.error('Error writing file:', error.message);
    }
    return {
      written: false,
      result: output,
    };
  }
  return {
    written: true,
    result: output,
  };
};
