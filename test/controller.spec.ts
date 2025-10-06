// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { rmhsh } from '@rljson/hash';
import { IoMem } from '@rljson/io';
import { JsonH } from '@rljson/json';
import { Layer, SliceId, TableCfg } from '@rljson/rljson';

import { beforeEach, describe, expect, it } from 'vitest';

import { CarGeneral, carsExample } from '../src/cars-example';
import {
  CakeController,
  CakeControllerRefs,
  CakeValue,
} from '../src/controller/cake-controller';
import { ComponentController } from '../src/controller/component-controller';
import { createController } from '../src/controller/controller';
import {
  LayerController,
  LayerControllerRefs,
} from '../src/controller/layer-controller';
import { Core } from '../src/core';
import { Db } from '../src/db';

describe('Controller', () => {
  let db: Db;
  let core: Core;

  beforeEach(async () => {
    //Init io
    const io = new IoMem();
    await io.init();
    await io.isReady();

    //Init Db
    db = new Db(io);
    core = db.core;

    //Create Tables for TableCfgs in carsExample
    for (const tableCfg of carsExample().tableCfgs._data) {
      await db.core.createEditable(tableCfg);
    }

    //Import Data
    await db.core.import(carsExample());
  });

  describe('ComponentController', () => {
    describe('Basic operation', () => {
      it('Init', async () => {
        //Create ComponentController
        let componentCtrl: ComponentController<'CarGeneral', CarGeneral>;

        //Wrong TableKey
        componentCtrl = new ComponentController(core, '#');
        await expect(componentCtrl.init()).rejects.toThrow(
          'Table "#" not found',
        );

        //Table not of type components
        // Create a mock table, which is not of type components
        componentCtrl = new ComponentController(core, 'carSliceId');
        await expect(componentCtrl.init()).rejects.toThrow(
          'Table carSliceId is not of type components.',
        );
      });
      it('Table', async () => {
        //Create ComponentController
        const carGeneralComponentController = await createController(
          'components',
          core,
          'carGeneral',
        );

        //Read Table
        const table = await carGeneralComponentController.table();
        expect(table).toBeDefined();
        expect(table._data.map((d: JsonH) => d._hash).sort()).toEqual(
          carsExample()
            .carGeneral._data.map((d: JsonH) => d._hash)
            .sort(),
        );
      });
      it('Get', async () => {
        //Create ComponentController
        const carGeneralComponentController = await createController(
          'components',
          core,
          'carGeneral',
        );

        //Read existing Row
        const firstRowHash = carsExample().carGeneral._data[0]._hash as string;
        const firstRow = await carGeneralComponentController.get(firstRowHash);
        expect(firstRow).toBeDefined();
        expect(firstRow!._hash!).toBeDefined();
        expect(firstRow!._hash!).toStrictEqual(firstRowHash);

        //Read non-existing Row
        const nonExistingRow = await carGeneralComponentController.get('#');
        expect(nonExistingRow).toBeUndefined();
      });

      it('Add', async () => {
        //Create ComponentController
        const carGeneralComponentController = await createController(
          'components',
          core,
          'carGeneral',
        );

        //Add Component
        const origin = 'H45H';
        const carGeneralValue: CarGeneral = {
          brand: 'Toyota',
          type: 'Corolla',
          doors: 4,
          _hash: '', // hash will be generated automatically
        };

        const editProtocolFirstRow = await carGeneralComponentController.add(
          carGeneralValue,
          origin,
        );
        expect(editProtocolFirstRow).toBeDefined();
        expect(editProtocolFirstRow.timeId).toBeDefined();
        expect(editProtocolFirstRow.carGeneralRef).toBeDefined();

        //Check if EditProtocol was written correctly
        const { carGeneral: carGeneralTable } = await db.core.dumpTable(
          'carGeneral',
        );
        expect(carGeneralTable?._data.length).toBe(3); //3 because two rows already existed from carsExample

        //Add another Component, with previous
        const carGeneralValueSecond: CarGeneral = {
          brand: 'Ford',
          type: 'Mustang',
          doors: 2,
          _hash: '', // hash will be generated automatically
        };

        const editProtocolSecondRow = await carGeneralComponentController.add(
          carGeneralValueSecond,
          origin,
          [editProtocolFirstRow.timeId as string],
        );
        expect(editProtocolSecondRow).toBeDefined();
        expect(editProtocolSecondRow.timeId).toBeDefined();
        expect(editProtocolSecondRow.carGeneralRef).toBeDefined();

        //Check if EditProtocol was written correctly
        const { carGeneral: carGeneralTable2 } = await db.core.dumpTable(
          'carGeneral',
        );
        expect(carGeneralTable2?._data.length).toBe(4); // 4 because two rows already existed from carsExample and one from first add
      });
      it('Remove', async () => {
        //LayerController Refs
        const carGeneralLayerRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsTableRow: (carsExample().carSliceId._data[0]._hash ||
            '') as string,
          componentsTable: 'carGeneral',
        } as LayerControllerRefs;

        //Create LayerController
        const carGeneralLayerController = await createController(
          'layers',
          core,
          'carGeneralLayer',
          carGeneralLayerRefs,
        );

        //Remove Layer
        const origin = 'H45H';
        const carGeneralLayerValue: Partial<Layer> = {
          remove: {
            VIN1: (carsExample().carGeneral._data[1]._hash as string) || '',
            VIN2: (carsExample().carGeneral._data[0]._hash as string) || '',
          } as Record<SliceId, string>,
        };

        const editProtocolFirstRow = await carGeneralLayerController.remove(
          carGeneralLayerValue,
          origin,
        );
        expect(editProtocolFirstRow).toBeDefined();
        expect(editProtocolFirstRow.timeId).toBeDefined();
        expect(editProtocolFirstRow.carGeneralLayerRef).toBeDefined();

        //Check if EditProtocol was written correctly
        const { carGeneralLayer: carGeneralEdits } = await db.core.dumpTable(
          'carGeneralLayer',
        );
        expect(carGeneralEdits?._data.length).toBe(2); //2 because one row already existed from carsExample

        //Remove another Layer, with previous
        const carGeneralLayerValueSecond: Partial<Layer> = {
          remove: {
            VIN3: (carsExample().carGeneral._data[1]._hash as string) || '',
          } as Record<SliceId, string>,
        };

        const editProtocolSecondRow = await carGeneralLayerController.remove(
          carGeneralLayerValueSecond,
          origin,
          [editProtocolFirstRow.timeId as string],
        );
        expect(editProtocolSecondRow).toBeDefined();
        expect(editProtocolSecondRow.timeId).toBeDefined();
        expect(editProtocolSecondRow.carGeneralLayerRef).toBeDefined();

        //Check if EditProtocol was written correctly
        const { carGeneralLayer: carGeneralEdits2 } = await db.core.dumpTable(
          'carGeneralLayer',
        );
        expect(carGeneralEdits2?._data.length).toBe(3); // 3 because one row already existed from carsExample and one from first remove

        //Check if previous is set correctly
        expect(editProtocolSecondRow.previous).toEqual([
          editProtocolFirstRow.timeId as string,
        ]);

        //Check if EditProtocol rows are correct
        const { carGeneralLayer: carGeneralEdits3 } = await db.core.dumpTable(
          'carGeneralLayer',
        );
        expect(carGeneralEdits3).toBeDefined();
        expect(carGeneralEdits3?._data).toBeDefined();
        expect(carGeneralEdits3?._data.length).toBe(3); // 3 because one row already existed from carsExample and one from first remove

        expect(carGeneralEdits3?._data[0]).toBeDefined();
        expect(carGeneralEdits3?._data[1]).toBeDefined();
        expect(carGeneralEdits3?._data[2]).toBeDefined();

        expect(carGeneralEdits3?._data.length).toBe(3); // 3 because one row already existed from carsExample and one from first remove
        expect(carGeneralEdits3?._data[0]).toBeDefined();
        expect(carGeneralEdits3?._data[1]).toBeDefined();
        expect(carGeneralEdits3?._data[2]).toBeDefined();
      });
    });
  });
  describe('LayerController', () => {
    describe('Basic operation', () => {
      it('Init', async () => {
        //LayerController Refs
        const carGeneralLayerRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsTableRow: (carsExample().carSliceId._data[0]._hash ||
            '') as string,
          componentsTable: 'carGeneral',
        } as LayerControllerRefs;

        //Create LayerController
        let layerCtrl: LayerController<'CarGeneral'>;

        //Wrong TableKey
        layerCtrl = new LayerController(core, '#', carGeneralLayerRefs);
        await expect(layerCtrl.init()).rejects.toThrow(
          'Table # is not supported by LayerController.',
        );

        //Table not existing
        layerCtrl = new LayerController(core, '#Layer', carGeneralLayerRefs);
        await expect(layerCtrl.init()).rejects.toThrow(
          'Table "#Layer" not found',
        );

        //Table not of type layers
        // Create a mock table, which is not of type layers
        const mockLayerName = 'mockLayer';
        const mockCfg = {
          ...carsExample().tableCfgs._data[1],
          ...{ key: mockLayerName },
        } as TableCfg;
        await db.core.createTable(rmhsh(mockCfg) as TableCfg);

        layerCtrl = new LayerController(
          core,
          mockLayerName,
          carGeneralLayerRefs,
        );
        await expect(layerCtrl.init()).rejects.toThrow(
          'Table mockLayer is not of type layers.',
        );

        //Missing Refs
        layerCtrl = new LayerController(core, 'carGeneralLayer', {
          sliceIdsTable: 'carSliceId',
          componentsTable: 'carGeneral',
        } as LayerControllerRefs);
        await expect(layerCtrl.init()).rejects.toThrow(
          'LayerController refs are not complete. Please provide sliceIdsTable, sliceIdsTableRow and componentsTable.',
        );

        //Valid
        layerCtrl = new LayerController(
          core,
          'carGeneralLayer',
          carGeneralLayerRefs,
        );
        await layerCtrl.init();
        expect(layerCtrl).toBeDefined();
      });
      it('Table', async () => {
        //LayerController Refs
        const carGeneralLayerRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsTableRow: (carsExample().carSliceId._data[0]._hash ||
            '') as string,
          componentsTable: 'carGeneral',
        } as LayerControllerRefs;

        //Create LayerController
        const carGeneralLayerController = await createController(
          'layers',
          core,
          'carGeneralLayer',
          carGeneralLayerRefs,
        );

        //Read Table
        const table = await carGeneralLayerController.table();
        expect(table).toBeDefined();
        expect(table).toEqual(carsExample().carGeneralLayer);
      });
      it('Get', async () => {
        //LayerController Refs
        const carGeneralLayerRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsTableRow: (carsExample().carSliceId._data[0]._hash ||
            '') as string,
          componentsTable: 'carGeneral',
        } as LayerControllerRefs;

        //Create LayerController
        const carGeneralLayerController = await createController(
          'layers',
          core,
          'carGeneralLayer',
          carGeneralLayerRefs,
        );

        //Read existing Row
        const firstRowHash = carsExample().carGeneralLayer._data[0]
          ._hash as string;
        const firstRow = await carGeneralLayerController.get(firstRowHash);
        expect(firstRow).toBeDefined();
        expect(firstRow!._hash!).toBeDefined();
        expect(firstRow!._hash!).toStrictEqual(firstRowHash);

        //Read non-existing Row
        const nonExistingRow = await carGeneralLayerController.get('#');
        expect(nonExistingRow).toBeUndefined();
      });

      it('Add', async () => {
        //LayerController Refs
        const carGeneralLayerRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsTableRow: (carsExample().carSliceId._data[0]._hash ||
            '') as string,
          componentsTable: 'carGeneral',
        } as LayerControllerRefs;

        //Create LayerController
        const carGeneralLayerController = await createController(
          'layers',
          core,
          'carGeneralLayer',
          carGeneralLayerRefs,
        );

        //Add Layer
        const origin = 'H45H';
        const carGeneralLayerValue: Partial<Layer> = {
          add: {
            VIN1: (carsExample().carGeneral._data[1]._hash as string) || '',
            VIN2: (carsExample().carGeneral._data[0]._hash as string) || '',
          } as Record<SliceId, string>,
        };

        const editProtocolFirstRow = await carGeneralLayerController.add(
          carGeneralLayerValue,
          origin,
        );
        expect(editProtocolFirstRow).toBeDefined();
        expect(editProtocolFirstRow.timeId).toBeDefined();
        expect(editProtocolFirstRow.carGeneralLayerRef).toBeDefined();

        //Check if EditProtocol was written correctly
        const { carGeneralLayer: carGeneralLayerTable } =
          await db.core.dumpTable('carGeneralLayer');
        expect(carGeneralLayerTable?._data.length).toBe(2); //2 because one row already existed from carsExample

        //Add another Layer, with previous
        const carGeneralLayerValueSecond: Partial<Layer> = {
          add: {
            VIN3: (carsExample().carGeneral._data[1]._hash as string) || '',
          } as Record<SliceId, string>,
        };

        const editProtocolSecondRow = await carGeneralLayerController.add(
          carGeneralLayerValueSecond,
          origin,
          [editProtocolFirstRow.timeId as string],
        );
        expect(editProtocolSecondRow).toBeDefined();
        expect(editProtocolSecondRow.timeId).toBeDefined();
        expect(editProtocolSecondRow.carGeneralLayerRef).toBeDefined();

        //Check if EditProtocol was written correctly
        const { carGeneralLayer: carGeneralLayerTable2 } =
          await db.core.dumpTable('carGeneralLayer');
        expect(carGeneralLayerTable2?._data.length).toBe(3); // 3 because one row already existed from carsExample and one from first add
      });
      it('Remove', async () => {
        //LayerController Refs
        const carGeneralLayerRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsTableRow: (carsExample().carSliceId._data[0]._hash ||
            '') as string,
          componentsTable: 'carGeneral',
        } as LayerControllerRefs;

        //Create LayerController
        const carGeneralLayerController = await createController(
          'layers',
          core,
          'carGeneralLayer',
          carGeneralLayerRefs,
        );

        //Remove Layer
        const origin = 'H45H';
        const carGeneralLayerValue: Partial<Layer> = {
          remove: {
            VIN1: (carsExample().carGeneral._data[1]._hash as string) || '',
            VIN2: (carsExample().carGeneral._data[0]._hash as string) || '',
          } as Record<SliceId, string>,
        };

        const editProtocolFirstRow = await carGeneralLayerController.remove(
          carGeneralLayerValue,
          origin,
        );
        expect(editProtocolFirstRow).toBeDefined();
        expect(editProtocolFirstRow.timeId).toBeDefined();
        expect(editProtocolFirstRow.carGeneralLayerRef).toBeDefined();

        //Check if EditProtocol was written correctly
        const { carGeneralLayer: carGeneralEdits } = await db.core.dumpTable(
          'carGeneralLayer',
        );
        expect(carGeneralEdits?._data.length).toBe(2); //2 because one row already existed from carsExample

        //Remove another Layer, with previous
        const carGeneralLayerValueSecond: Partial<Layer> = {
          remove: {
            VIN3: (carsExample().carGeneral._data[1]._hash as string) || '',
          } as Record<SliceId, string>,
        };

        const editProtocolSecondRow = await carGeneralLayerController.remove(
          carGeneralLayerValueSecond,
          origin,
          [editProtocolFirstRow.timeId as string],
        );
        expect(editProtocolSecondRow).toBeDefined();
        expect(editProtocolSecondRow.timeId).toBeDefined();
        expect(editProtocolSecondRow.carGeneralLayerRef).toBeDefined();

        //Check if EditProtocol was written correctly
        const { carGeneralLayer: carGeneralEdits2 } = await db.core.dumpTable(
          'carGeneralLayer',
        );
        expect(carGeneralEdits2?._data.length).toBe(3); // 3 because one row already existed from carsExample and one from first remove

        //Check if previous is set correctly
        expect(editProtocolSecondRow.previous).toEqual([
          editProtocolFirstRow.timeId as string,
        ]);

        //Check if EditProtocol rows are correct
        const { carGeneralLayer: carGeneralEdits3 } = await db.core.dumpTable(
          'carGeneralLayer',
        );
        expect(carGeneralEdits3).toBeDefined();
        expect(carGeneralEdits3?._data).toBeDefined();
        expect(carGeneralEdits3?._data.length).toBe(3); // 3 because one row already existed from carsExample and one from first remove

        expect(carGeneralEdits3?._data[0]).toBeDefined();
        expect(carGeneralEdits3?._data[1]).toBeDefined();
        expect(carGeneralEdits3?._data[2]).toBeDefined();

        expect(carGeneralEdits3?._data.length).toBe(3); // 3 because one row already existed from carsExample and one from first remove
        expect(carGeneralEdits3?._data[0]).toBeDefined();
        expect(carGeneralEdits3?._data[1]).toBeDefined();
        expect(carGeneralEdits3?._data[2]).toBeDefined();
      });
    });
  });
  describe('CakeController', () => {
    describe('Basic operation', () => {
      it('Init', async () => {
        //CakeController Refs
        const carCakeRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsRow: (carsExample().carSliceId._data[0]._hash ||
            '') as string,
        } as CakeControllerRefs;

        //Create CakeController
        let cakeCtrl: CakeController<'CarCake'>;

        //Wrong TableKey
        cakeCtrl = new CakeController(core, '#', carCakeRefs);
        await expect(cakeCtrl.init()).rejects.toThrow(
          'Table # is not supported by CakeController.',
        );

        //Table not existing
        cakeCtrl = new CakeController(core, '#Cake', carCakeRefs);
        await expect(cakeCtrl.init()).rejects.toThrow(
          'Table "#Cake" not found',
        );

        //Table not of type cakes
        // Create a mock table, which is not of type cakes
        const mockCakesName = 'mockCake';
        const mockCfg = {
          ...carsExample().tableCfgs._data[1],
          ...{ key: mockCakesName },
        } as TableCfg;
        await db.core.createTable(rmhsh(mockCfg) as TableCfg);

        cakeCtrl = new CakeController(core, mockCakesName, carCakeRefs);
        await expect(cakeCtrl.init()).rejects.toThrow(
          'Table mockCake is not of type cakes.',
        );

        //Missing Refs
        cakeCtrl = new CakeController(core, 'carCake', {
          sliceIdsTable: 'carSliceId',
        } as CakeControllerRefs);
        await expect(cakeCtrl.init()).rejects.toThrow(
          'Refs are not complete on CakeController. Required: sliceIdsTable, sliceIdsRow',
        );

        //Valid
        cakeCtrl = new CakeController(core, 'carCake', carCakeRefs);
        await cakeCtrl.init();
        expect(cakeCtrl).toBeDefined();
      });
      it('Table', async () => {
        //CakeController Refs
        const carCakeRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsRow: (carsExample().carSliceId._data[0]._hash ||
            '') as string,
        } as CakeControllerRefs;

        //Create CakeController
        const carCakeController = await createController(
          'cakes',
          core,
          'carCake',
          carCakeRefs,
        );

        //Read Table
        const table = await carCakeController.table();
        expect(table).toBeDefined();
        expect(table).toEqual(carsExample().carCake);
      });
      it('Get', async () => {
        //CakeController Refs
        const carCakeRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsRow: (carsExample().carSliceId._data[0]._hash ||
            '') as string,
        } as CakeControllerRefs;

        //Create CakeController
        const carCakeController = await createController(
          'cakes',
          core,
          'carCake',
          carCakeRefs,
        );

        //Read existing Row
        const firstRowHash = carsExample().carCake._data[0]._hash as string;
        const firstRow = await carCakeController.get(firstRowHash);
        expect(firstRow).toBeDefined();
        expect(firstRow!._hash!).toBeDefined();
        expect(firstRow!._hash!).toStrictEqual(firstRowHash);

        //Read non-existing Row
        const nonExistingRow = await carCakeController.get('#');
        expect(nonExistingRow).toBeUndefined();
      });

      it('Add', async () => {
        //CakeController Refs
        const carCakeRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsRow: (carsExample().carSliceId._data[0]._hash ||
            '') as string,
        } as CakeControllerRefs;

        //Create CakeController
        const carCakeController = await createController(
          'cakes',
          core,
          'carCake',
          carCakeRefs,
        );

        //Add Cake
        const origin = 'H45H';
        const carCakeValue: CakeValue = {
          layers: {
            carGeneralLayer:
              (carsExample().carGeneralLayer._data[0]._hash as string) || '',
            carTechnicalLayer:
              (carsExample().carTechnicalLayer._data[0]._hash as string) || '',
            carColorLayer:
              (carsExample().carColorLayer._data[0]._hash as string) || '',
          },
          id: 'MyFirstCake',
        };

        const editProtocolFirstRow = await carCakeController.add(
          carCakeValue,
          origin,
        );
        expect(editProtocolFirstRow).toBeDefined();
        expect(editProtocolFirstRow.timeId).toBeDefined();
        expect(editProtocolFirstRow.carCakeRef).toBeDefined();

        //Check if EditProtocol was written correctly
        const { carCake: carCakeTable } = await db.core.dumpTable('carCake');
        expect(carCakeTable?._data.length).toBe(2); //2 because one row already existed from carsExample

        //Add another Cake, with previous
        const carCakeValueSecond: CakeValue = {
          layers: {
            carGeneralLayer:
              (carsExample().carGeneralLayer._data[0]._hash as string) || '',
            carTechnicalLayer:
              (carsExample().carTechnicalLayer._data[0]._hash as string) || '',
            carColorLayer:
              (carsExample().carColorLayer._data[0]._hash as string) || '',
          },
          id: 'MySecondCake',
        };

        const editProtocolSecondRow = await carCakeController.add(
          carCakeValueSecond,
          origin,
          [editProtocolFirstRow.timeId as string],
        );
        expect(editProtocolSecondRow).toBeDefined();
        expect(editProtocolSecondRow.timeId).toBeDefined();
        expect(editProtocolSecondRow.carCakeRef).toBeDefined();

        //Check if EditProtocol was written correctly
        const { carCake: carCakeTable2 } = await db.core.dumpTable('carCake');
        expect(carCakeTable2?._data.length).toBe(3); // 3 because one row already existed from carsExample and one from first add
      });
      it('Remove', async () => {
        //Create CakeController without Refs
        const carCakeController = (await createController(
          'cakes',
          core,
          'carCake',
        )) as CakeController<'CarCake'>;

        //Remove should throw error, because not supported
        await expect(carCakeController.remove()).rejects.toThrow(
          'Remove is not supported on CakeController.',
        );
      });
    });
  });
});
