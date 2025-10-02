// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip, hsh } from '@rljson/hash';
import { IoMem } from '@rljson/io';
import { Json, JsonH, JsonValueH } from '@rljson/json';
import { ComponentRef, Edit, Layer, SliceId } from '@rljson/rljson';

import { beforeEach, describe, expect, it } from 'vitest';

import { CarGeneral, carsExample } from '../src/cars-example';
import { Db } from '../src/db';

describe('Db', () => {
  let db: Db;

  beforeEach(async () => {
    //Init io
    const io = new IoMem();
    await io.init();
    await io.isReady();

    //Init Core
    db = new Db(io);

    //Create Tables for TableCfgs in carsExample
    for (const tableCfg of carsExample().tableCfgs._data) {
      await db.core.createEditable(tableCfg);
    }

    //Import Data
    await db.core.import(carsExample());
  });

  describe('core', () => {
    it('should be defined', () => {
      expect(db.core).toBeDefined();
    });
  });

  describe('edit', () => {
    describe('component edit', () => {
      it('basic edit', async () => {
        const origin = 'H45H';
        const carGeneralValue = {
          brand: 'Audi',
          type: 'A4',
          doors: 5,
        } as CarGeneral & JsonValueH;

        const carGeneralEdit: Edit<CarGeneral> = hip<any>({
          value: carGeneralValue,
          route: 'carGeneral',
          origin,
          previous: ['e44O:1759126885655'],
          acknowledged: false,
        } as Edit<CarGeneral>);

        //Run Edit
        const editProtocolRow = await db.core.run(carGeneralEdit);

        //Delete timeId for easier testing
        delete (editProtocolRow as any)['timeId'];

        expect(editProtocolRow).toEqual({
          carGeneralRef: hsh(carGeneralValue as Json)._hash as string,
          origin,
          previous: ['e44O:1759126885655'],
          route: 'carGeneral',
        });

        //Check if edit protocol was written correctly
        const { carGeneralEdits } = await db.core.getProtocol('carGeneral');
        expect(carGeneralEdits?._data.length).toBe(1);

        const readProtocolRow = carGeneralEdits?._data[0];
        delete (readProtocolRow as any)['timeId']; //Delete timeId for easier testing
        delete (readProtocolRow as any)['_hash']; //Delete _hash for easier testing

        expect(readProtocolRow).toEqual(editProtocolRow);

        //Check if component was written correctly
        const { carGeneral } = await db.core.dumpTable('carGeneral');
        expect(carGeneral?._data.length).toBe(3);
      });
      it('basic subsequential edits', async () => {
        const origin = 'H45H';
        const carGeneralFirstValue = {
          brand: 'Audi',
          type: 'A4',
          doors: 5,
        } as CarGeneral & JsonValueH;

        const carGeneralFirstEdit: Edit<CarGeneral> = hip<any>({
          value: carGeneralFirstValue,
          route: 'carGeneral',
          origin,
          acknowledged: false,
        } as Edit<CarGeneral>);

        //Run First Edit
        const editProtocolFirstRow = await db.core.run(carGeneralFirstEdit);

        const carGeneralSecondValue = {
          brand: 'BMW',
          type: 'X3',
          doors: 3,
        } as CarGeneral & JsonValueH;

        const carGeneralSecondEdit: Edit<CarGeneral> = hip<any>({
          value: carGeneralSecondValue,
          previous: [editProtocolFirstRow.timeId as string],
          route: 'carGeneral',
          origin,
          acknowledged: false,
        } as Edit<CarGeneral>);

        //Run Second Edit
        const editProtocolSecondRow = await db.core.run(carGeneralSecondEdit);

        //Check if edit protocol was written correctly
        const { carGeneralEdits } = await db.core.getProtocol('carGeneral', {
          sorted: true,
          ascending: true,
        });
        expect(carGeneralEdits?._data.length).toBe(2);

        expect(editProtocolSecondRow.previous).toEqual([
          editProtocolFirstRow.timeId as string,
        ]);

        const readProtocolFirstRow = carGeneralEdits?._data[0];
        delete (readProtocolFirstRow as any)['timeId']; //Delete timeId for easier testing
        delete (readProtocolFirstRow as any)['_hash']; //Delete _hash for easier testing
        delete (editProtocolFirstRow as any)['timeId']; //Delete timeId for easier testing

        expect(readProtocolFirstRow).toEqual(editProtocolFirstRow);

        const readProtocolSecondRow = carGeneralEdits?._data[1];
        delete (readProtocolSecondRow as any)['timeId']; //Delete timeId for easier testing
        delete (readProtocolSecondRow as any)['_hash']; //Delete _hash for easier testing
        delete (editProtocolSecondRow as any)['timeId']; //Delete timeId for easier testing

        expect(readProtocolSecondRow).toEqual(editProtocolSecondRow);

        //Check if component was written correctly
        const { carGeneral } = await db.core.dumpTable('carGeneral');
        expect(carGeneral?._data.length).toBe(4);
      });
    });
    describe('layer edit', () => {
      it('basic add edit', async () => {
        const origin = 'H45H';
        const carGeneralLayerValue: Layer = {
          add: {
            VIN1: (hsh(carsExample().carGeneral._data[1]) as JsonH)._hash || '',
            VIN2: (hsh(carsExample().carGeneral._data[0]) as JsonH)._hash || '',
          } as Record<SliceId, ComponentRef>,
          sliceIdsTable: 'carSliceId',
          sliceIdsTableRow: (carsExample().carSliceId._data[0]._hash ||
            '') as string,
          componentsTable: 'carGeneral',
        };

        const carGeneralEdit: Edit<Layer> = hip<any>({
          value: carGeneralLayerValue,
          route: 'carGeneralLayer',
          origin,
          acknowledged: false,
        } as Edit<Layer>);

        //Run Edit
        const editProtocolRow = await db.core.run(carGeneralEdit);

        debugger;
      });
    });
  });
});
