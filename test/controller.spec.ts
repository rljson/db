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
      await db.core.createTableWithInsertHistory(tableCfg);
    }

    //Import Data
    await db.core.import(carsExample());
  });

  describe('createController Factory', () => {
    describe('create Controllers for Content Types', () => {
      it('Layer', async () => {
        const layerCtrl = await createController(
          'layers',
          core,
          'carGeneralLayer',
          {
            sliceIdsTable: 'carSliceId',
            sliceIdsTableRow: (carsExample().carSliceId._data[0]._hash ||
              '') as string,
            componentsTable: 'carGeneral',
          } as LayerControllerRefs,
        );
        expect(layerCtrl).toBeDefined();
        expect(layerCtrl).toBeInstanceOf(LayerController);
      });
      it('Component', async () => {
        const componentCtrl = await createController(
          'components',
          core,
          'carGeneral',
        );
        expect(componentCtrl).toBeDefined();
        expect(componentCtrl).toBeInstanceOf(ComponentController);
      });
      it('Cake', async () => {
        const cakeCtrl = await createController('cakes', core, 'carCake', {
          sliceIdsTable: 'carSliceId',
          sliceIdsRow: (carsExample().carSliceId._data[0]._hash ||
            '') as string,
        } as CakeControllerRefs);
        expect(cakeCtrl).toBeDefined();
        expect(cakeCtrl).toBeInstanceOf(CakeController);
      });
      it('Unknown', async () => {
        await expect(
          createController('unknown' as any, core, 'carGeneral'),
        ).rejects.toThrow(
          'Controller for type unknown is not implemented yet.',
        );
      });
    });
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

        //Base in Refs provided --> ignored
        componentCtrl = new ComponentController(core, 'carGeneral', {
          base: 'H45H',
        });
        await expect(componentCtrl.init()).toBeDefined();

        //Refs provided --> throw
        componentCtrl = new ComponentController(core, 'carGeneral', {
          sliceIdsTable: 'carSliceId',
          sliceIdsRow: 'H45H',
        });
        await expect(componentCtrl.init()).rejects.toThrow(
          'Refs are not required on ComponentController.',
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
        const {
          ['carGeneral']: { _data: firstRows },
        } = await carGeneralComponentController.get(firstRowHash);
        const firstRow = firstRows[0];
        expect(firstRow).toBeDefined();
        expect(firstRow._hash!).toBeDefined();
        expect(firstRow._hash!).toStrictEqual(firstRowHash);

        //Read non-existing Row
        const nonExistingRow = await carGeneralComponentController.get('#');
        expect(nonExistingRow['carGeneral']._data.length).toBe(0);

        //Read by invalid where
        const invalidWhere = await carGeneralComponentController.get(5 as any);
        expect(invalidWhere).toEqual({});
      });

      it('Get by Hash w/ Filter', async () => {
        //Create ComponentController
        const carGeneralComponentController = await createController(
          'components',
          core,
          'carGeneral',
        );

        //Read existing Row with Filter
        const firstRowHash = carsExample().carGeneral._data[0]._hash as string;
        const filter = { brand: 'Volkswagen' };
        const {
          ['carGeneral']: { _data: firstRows },
        } = await carGeneralComponentController.get(firstRowHash, filter);
        const firstRow = firstRows[0];
        expect(firstRow).toBeDefined();
        expect(firstRow._hash!).toBeDefined();
        expect(firstRow._hash!).toStrictEqual(firstRowHash);

        //Read existing Row with non-matching Filter
        const {
          ['carGeneral']: { _data: nonMatchingRows },
        } = await carGeneralComponentController.get(firstRowHash, {
          brand: 'Ford',
        });
        expect(nonMatchingRows.length).toBe(0);
      });

      it('Get by where object w/ Filter', async () => {
        //Create ComponentController
        const carGeneralComponentController = await createController(
          'components',
          core,
          'carGeneral',
        );

        //Read existing Row with Filter
        const where = carsExample().carGeneral._data[0];
        const matchingFilter = { brand: 'Volkswagen' };
        const {
          ['carGeneral']: { _data: firstRows },
        } = await carGeneralComponentController.get(
          rmhsh(where),
          matchingFilter,
        );
        const firstRow = firstRows[0];
        expect(firstRow).toBeDefined();
        expect(firstRow._hash!).toBeDefined();
        expect(firstRow).toStrictEqual(where);

        //Read existing Row with non-matching Filter
        const nonMatchingFilter = { brand: 'Ford' };
        const {
          ['carGeneral']: { _data: nonMatchingRows },
        } = await carGeneralComponentController.get(
          rmhsh(where),
          nonMatchingFilter,
        );
        expect(nonMatchingRows.length).toBe(0);
      });

      it('Run', async () => {
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

        //Invalid Command
        await expect(
          carGeneralComponentController.insert(
            'update' as any,
            carGeneralValue,
          ),
        ).rejects.toThrow(
          'Command update is not supported by ComponentController.',
        );

        //Invalid Refs
        await expect(
          carGeneralComponentController.insert('add', carGeneralValue, origin, {
            base: 'H45H',
          }),
        ).rejects.toThrow('Refs are not supported on ComponentController.');

        //Valid Run
        const editProtocolFirstRow = await carGeneralComponentController.insert(
          'add',
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
        expect(carGeneralTable?._data.length).toBe(
          carsExample().carGeneral._data.length + 1,
        );

        //Add another Component, with previous
        const carGeneralValueSecond: CarGeneral = {
          brand: 'Ford',
          type: 'Mustang',
          doors: 2,
          _hash: '', // hash will be generated automatically
        };

        const editProtocolSecondRow =
          await carGeneralComponentController.insert(
            'add',
            carGeneralValueSecond,
            origin,
          );
        expect(editProtocolSecondRow).toBeDefined();
        expect(editProtocolSecondRow.timeId).toBeDefined();
        expect(editProtocolSecondRow.carGeneralRef).toBeDefined();

        //Check if EditProtocol was written correctly
        const { carGeneral: carGeneralTable2 } = await db.core.dumpTable(
          'carGeneral',
        );
        expect(carGeneralTable2?._data.length).toBe(
          carsExample().carGeneral._data.length + 2,
        );
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

        //Valid w/o refs
        layerCtrl = new LayerController(core, 'carGeneralLayer');
        await layerCtrl.init();
        expect(layerCtrl).toBeDefined();

        //Valid w/ refs
        layerCtrl = new LayerController(
          core,
          'carGeneralLayer',
          carGeneralLayerRefs,
        );
        await layerCtrl.init();
        expect(layerCtrl).toBeDefined();

        //Valid w/ base only
        const carGeneralLayerRefsWithBase = {
          base: (carsExample().carGeneralLayer._data[0]._hash || '') as string,
        } as LayerControllerRefs;
        layerCtrl = new LayerController(
          core,
          'carGeneralLayer',
          carGeneralLayerRefsWithBase,
        );
        await layerCtrl.init();
        expect(layerCtrl).toBeDefined();

        //Base not existing
        const missingBaseLayerRefs = {
          base: 'NonExisting',
        } as LayerControllerRefs;
        layerCtrl = new LayerController(
          core,
          'carGeneralLayer',
          missingBaseLayerRefs,
        );
        await expect(layerCtrl.init()).rejects.toThrow(
          'Base layer NonExisting does not exist.',
        );
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
        const {
          ['carGeneralLayer']: { _data: firstRows },
        } = await carGeneralLayerController.get(firstRowHash);
        const firstRow = firstRows[0];
        expect(firstRow).toBeDefined();
        expect(firstRow._hash!).toBeDefined();
        expect(firstRow._hash!).toStrictEqual(firstRowHash);

        //Read non-existing Row
        const nonExistingRow = await carGeneralLayerController.get('#');
        expect(nonExistingRow['carGeneralLayer']._data.length).toBe(0);
      });

      it('Run', async () => {
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

        const editProtocolFirstRow = await carGeneralLayerController.insert(
          'add',
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

        const editProtocolSecondRow = await carGeneralLayerController.insert(
          'add',
          carGeneralLayerValueSecond,
          origin,
        );
        expect(editProtocolSecondRow).toBeDefined();
        expect(editProtocolSecondRow.timeId).toBeDefined();
        expect(editProtocolSecondRow.carGeneralLayerRef).toBeDefined();

        //Check if EditProtocol was written correctly
        const { carGeneralLayer: carGeneralLayerTable2 } =
          await db.core.dumpTable('carGeneralLayer');
        expect(carGeneralLayerTable2?._data.length).toBe(3); // 3 because one row already existed from carsExample and one from first add
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

        //Base cake not existing
        const missingBaseCakeRefs = {
          ...carCakeRefs,
          ...{ base: 'NonExisting' },
        } as CakeControllerRefs;
        cakeCtrl = new CakeController(core, 'carCake', missingBaseCakeRefs);
        await expect(cakeCtrl.init()).rejects.toThrow(
          'Base cake NonExisting does not exist.',
        );

        //Valid, w/o refs
        cakeCtrl = new CakeController(core, 'carCake');
        await cakeCtrl.init();
        expect(cakeCtrl).toBeDefined();

        //Valid, w/ base
        const carCakeRefsWithBase = {
          ...carCakeRefs,
          ...{ base: (carsExample().carCake._data[0]._hash || '') as string },
        } as CakeControllerRefs;
        cakeCtrl = new CakeController(core, 'carCake', carCakeRefsWithBase);
        await cakeCtrl.init();
        expect(cakeCtrl).toBeDefined();

        //Valid, w/ refs
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

        //Read existing Row By Hash
        const firstRowHash = carsExample().carCake._data[0]._hash as string;
        const {
          ['carCake']: { _data: firstRows },
        } = await carCakeController.get(firstRowHash);
        const firstRow = firstRows[0];
        expect(firstRow).toBeDefined();
        expect(firstRow._hash!).toBeDefined();
        expect(firstRow._hash!).toStrictEqual(firstRowHash);

        //Read existing Row By where object
        const {
          ['carCake']: { _data: firstRowsByWhere },
        } = await carCakeController.get(rmhsh(firstRow) as string);
        const firstRowByWhere = firstRowsByWhere[0];
        expect(firstRowByWhere).toBeDefined();
        expect(firstRowByWhere._hash!).toBeDefined();
        expect(firstRowByWhere).toStrictEqual(firstRow);

        //Read non-existing Row
        const nonExistingRow = await carCakeController.get('#');
        expect(nonExistingRow['carCake']._data.length).toBe(0);

        //Read by invalid where
        const invalidWhere = await carCakeController.get(5 as any);
        expect(invalidWhere).toEqual({});
      });

      it('Run', async () => {
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

        const editProtocolFirstRow = await carCakeController.insert(
          'add',
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

        const editProtocolSecondRow = await carCakeController.insert(
          'add',
          carCakeValueSecond,
          origin,
        );
        expect(editProtocolSecondRow).toBeDefined();
        expect(editProtocolSecondRow.timeId).toBeDefined();
        expect(editProtocolSecondRow.carCakeRef).toBeDefined();

        //Check if EditProtocol was written correctly
        const { carCake: carCakeTable2 } = await db.core.dumpTable('carCake');
        expect(carCakeTable2?._data.length).toBe(3); // 3 because one row already existed from carsExample and one from first add
      });

      it('Invalid Run command', async () => {
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

        //Run with invalid command
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

        await expect(
          carCakeController.insert('update' as any, carCakeValue, origin),
        ).rejects.toThrow('Command update is not supported by CakeController.');
      });
    });
  });
});
