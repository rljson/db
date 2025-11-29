// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { rmhsh } from '@rljson/hash';
import { IoMem } from '@rljson/io';
import { Json, JsonH } from '@rljson/json';
import {
  Cake,
  CakesTable,
  Layer,
  LayersTable,
  SliceId,
  TableCfg,
} from '@rljson/rljson';

import { beforeEach, describe, expect, it } from 'vitest';

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
import {
  CarDimension,
  CarGeneral,
  staticExample,
} from '../src/example-static/example-static';

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
    for (const tableCfg of staticExample().tableCfgs._data) {
      await db.core.createTableWithInsertHistory(tableCfg);
    }

    //Import Data
    await db.core.import(staticExample());
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
            sliceIdsTableRow: (staticExample().carSliceId._data[0]._hash ||
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
          sliceIdsRow: (staticExample().carSliceId._data[0]._hash ||
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
        let componentCtrl: ComponentController<'CarGeneral', Json, CarGeneral>;

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

        //Refs provided --> throw
        componentCtrl = new ComponentController(core, 'carGeneral', {
          sliceIdsTable: 'carSliceId',
          sliceIdsRow: 'H45H',
        });
        await expect(componentCtrl.init()).rejects.toThrow(
          'Refs are not required on ComponentController.',
        );
      });

      it('getChildRefs', async () => {
        //Create ComponentController
        const carTechnicalComponentController = await createController(
          'components',
          core,
          'carTechnical',
        );

        //Get Child Refs
        const dimensionsRefs = Array.from(
          new Set(
            staticExample()
              .carTechnical._data.map((c) =>
                Array.isArray(c.dimensions) ? c.dimensions : [c.dimensions],
              )
              .flatMap((d) => d as string[]),
          ),
        ).sort((a, b) => b.localeCompare(a));

        const childRefs = (
          await carTechnicalComponentController.getChildRefs({})
        )
          .map((ch) => ch.ref)
          .sort((a, b) => b.localeCompare(a));

        expect(childRefs).toBeDefined();
        expect(childRefs).toEqual(dimensionsRefs);
      });

      it('Table', async () => {
        //Create ComponentController
        const carGeneralComponentController = (await createController(
          'components',
          core,
          'carGeneral',
        )) as ComponentController<'CarGeneral', Json, CarGeneral>;

        //Read Table
        const table = await carGeneralComponentController.table();
        expect(table).toBeDefined();
        expect(table._data.map((d: JsonH) => d._hash).sort()).toEqual(
          staticExample()
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
        const firstRowHash = staticExample().carGeneral._data[0]
          ._hash as string;
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
        const firstRowHash = staticExample().carGeneral._data[0]
          ._hash as string;
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
        const where = staticExample().carGeneral._data[0];
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

      it('Get by where w/ referenced columns (encapsulated refs)', async () => {
        //Create ComponentController
        const carTechnicalComponentController = await createController(
          'components',
          core,
          'carTechnical',
        );

        //Build where
        const where = {
          ...staticExample().carDimensions._data[1],
        } as Partial<CarDimension>;

        const {
          carTechnical: { _data: resultData },
        } = await carTechnicalComponentController.get(rmhsh(where));

        expect(resultData.length).toBe(2);
        expect(resultData.map((r) => r._hash)).toEqual([
          staticExample().carTechnical._data[1]._hash,
          staticExample().carTechnical._data[2]._hash,
        ]);
      });

      it('Get by where w/ ref column values (encapsulated refs)', async () => {
        //Create ComponentController
        const carTechnicalComponentController = await createController(
          'components',
          core,
          'carTechnical',
        );

        //Build where
        const where = {
          height: staticExample().carDimensions._data.map((d) => d.height),
        };

        const {
          carTechnical: { _data: resultData },
        } = await carTechnicalComponentController.get(rmhsh(where));

        expect(resultData.length).toBe(
          staticExample().carTechnical._data.length,
        );
      });

      it('Insert', async () => {
        //Create ComponentController
        const carGeneralComponentController = await createController(
          'components',
          core,
          'carGeneral',
        );

        //Add Component
        const origin = 'H45H';
        const carGeneralValue: Partial<CarGeneral> = {
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
            sliceIdsTable: 'carSliceId',
            sliceIdsRow: 'H45H',
            componentsTable: 'carGeneral',
          }),
        ).rejects.toThrow('Refs are not supported on ComponentController.');

        //Valid Run
        const insertHistoryFirstRows =
          await carGeneralComponentController.insert(
            'add',
            carGeneralValue,
            origin,
          );
        const insertHistoryFirstRow = insertHistoryFirstRows[0];
        expect(insertHistoryFirstRow).toBeDefined();
        expect(insertHistoryFirstRow.timeId).toBeDefined();
        expect(insertHistoryFirstRow.carGeneralRef).toBeDefined();

        //Check if InsertHistory was written correctly
        const { carGeneral: carGeneralTable } = await db.core.dumpTable(
          'carGeneral',
        );
        expect(carGeneralTable?._data.length).toBe(
          staticExample().carGeneral._data.length + 1,
        );

        //Add another Component, with previous
        const carGeneralValueSecond: Partial<CarGeneral> = {
          brand: 'Ford',
          type: 'Mustang',
          doors: 2,
          _hash: '', // hash will be generated automatically
        };

        const insertHistorySecondRows =
          await carGeneralComponentController.insert(
            'add',
            carGeneralValueSecond,
            origin,
          );
        const insertHistorySecondRow = insertHistorySecondRows[0];

        expect(insertHistorySecondRow).toBeDefined();
        expect(insertHistorySecondRow.timeId).toBeDefined();
        expect(insertHistorySecondRow.carGeneralRef).toBeDefined();

        //Check if InsertHistory was written correctly
        const { carGeneral: carGeneralTable2 } = await db.core.dumpTable(
          'carGeneral',
        );
        expect(carGeneralTable2?._data.length).toBe(
          staticExample().carGeneral._data.length + 2,
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
          sliceIdsTableRow: (staticExample().carSliceId._data[0]._hash ||
            '') as string,
          componentsTable: 'carGeneral',
        } as LayerControllerRefs;

        //Create LayerController
        let layerCtrl: LayerController<'CarGeneral', Layer>;

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
          ...staticExample().tableCfgs._data[1],
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
          base: (staticExample().carGeneralLayer._data[0]._hash ||
            '') as string,
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
      it('getChildRefs', async () => {
        //LayerController Refs
        const carGeneralLayerRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsTableRow: (staticExample().carSliceId._data[0]._hash ||
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

        //Get Child Refs by Hash
        const firstRowHash = staticExample().carGeneralLayer._data[0]
          ._hash as string;
        const childRefsByHash = await carGeneralLayerController.getChildRefs(
          firstRowHash,
        );

        expect(childRefsByHash).toBeDefined();
        expect(childRefsByHash.length).toBe(
          staticExample().carSliceId._data[0].add.length,
        );
      });

      it('filterRow', async () => {
        //LayerController Refs
        const carGeneralLayerRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsTableRow: (staticExample().carSliceId._data[0]._hash ||
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

        const filterKey = '';
        const filterValue = Object.values(
          staticExample().carGeneralLayer._data[0].add,
        )[0];

        const positivefilterResult = await carGeneralLayerController.filterRow(
          staticExample().carGeneralLayer._data[0],
          filterKey,
          filterValue,
        );

        expect(positivefilterResult).toBe(true);

        const negativefilterResult = await carGeneralLayerController.filterRow(
          staticExample().carGeneralLayer._data[0],
          filterKey,
          'NonExistingHash',
        );
        expect(negativefilterResult).toBe(false);
      });

      it('Table', async () => {
        //LayerController Refs
        const carGeneralLayerRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsTableRow: (staticExample().carSliceId._data[0]._hash ||
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
        const table = (await carGeneralLayerController.table()) as LayersTable;
        expect(table).toBeDefined();
        expect(table._data.map((l) => l._hash).sort()).toEqual(
          staticExample()
            .carGeneralLayer._data.map((l) => l._hash)
            .sort(),
        );
      });
      it('Get', async () => {
        //LayerController Refs
        const carGeneralLayerRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsTableRow: (staticExample().carSliceId._data[0]._hash ||
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

        //Read existing Row by Hash
        const firstRowHash = staticExample().carGeneralLayer._data[0]
          ._hash as string;
        const {
          ['carGeneralLayer']: { _data: firstRows },
        } = await carGeneralLayerController.get(firstRowHash);
        const firstRow = firstRows[0];
        expect(firstRow).toBeDefined();
        expect(firstRow._hash!).toBeDefined();
        expect(firstRow._hash!).toStrictEqual(firstRowHash);

        //Read existing Row by where object
        const {
          ['carGeneralLayer']: { _data: firstRowsByWhere },
        } = await carGeneralLayerController.get(
          rmhsh(staticExample().carGeneralLayer._data[0]),
        );
        const firstRowByWhere = firstRowsByWhere[0];
        expect(firstRowByWhere).toBeDefined();
        expect(firstRowByWhere._hash!).toBeDefined();
        expect(firstRowByWhere).toStrictEqual(firstRow);

        //Read non-existing Row
        const nonExistingRow = await carGeneralLayerController.get('#');
        expect(nonExistingRow['carGeneralLayer']._data.length).toBe(0);

        //Read by empty
      });

      it('Insert -> Invalid Command', async () => {
        //LayerController Refs
        const carGeneralLayerRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsTableRow: (staticExample().carSliceId._data[0]._hash ||
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

        //Invalid Command
        const origin = 'H45H';
        const layerValue: Partial<Layer> = {
          VIN1: (staticExample().carGeneral._data[1]._hash as string) || '',
          VIN2: (staticExample().carGeneral._data[0]._hash as string) || '',
        } as Record<SliceId, string>;

        await expect(
          carGeneralLayerController.insert('update' as any, layerValue, origin),
        ).rejects.toThrow(
          'Command update is not supported by LayerController.',
        );
      });

      it('Insert -> Add Layer', async () => {
        //LayerController Refs
        const carGeneralLayerRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsTableRow: (staticExample().carSliceId._data[0]._hash ||
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
            VIN1: (staticExample().carGeneral._data[1]._hash as string) || '',
            VIN2: (staticExample().carGeneral._data[0]._hash as string) || '',
          } as Record<SliceId, string>,
        };

        const insertHistoryFirstRows = await carGeneralLayerController.insert(
          'add',
          carGeneralLayerValue,
          origin,
        );
        const insertHistoryFirstRow = insertHistoryFirstRows[0];

        expect(insertHistoryFirstRow).toBeDefined();
        expect(insertHistoryFirstRow.timeId).toBeDefined();
        expect(insertHistoryFirstRow.carGeneralLayerRef).toBeDefined();

        //Check if InsertHistory was written correctly
        const { carGeneralLayer: carGeneralLayerTable } =
          await db.core.dumpTable('carGeneralLayer');
        expect(carGeneralLayerTable?._data.length).toBe(
          staticExample().carGeneralLayer._data.length + 1,
        );

        //Add another Layer, with previous
        const carGeneralLayerValueSecond: Partial<Layer> = {
          add: {
            VIN3: (staticExample().carGeneral._data[1]._hash as string) || '',
          } as Record<SliceId, string>,
        };

        const insertHistorySecondRows = await carGeneralLayerController.insert(
          'add',
          carGeneralLayerValueSecond,
          origin,
        );
        const insertHistorySecondRow = insertHistorySecondRows[0];

        expect(insertHistorySecondRow).toBeDefined();
        expect(insertHistorySecondRow.timeId).toBeDefined();
        expect(insertHistorySecondRow.carGeneralLayerRef).toBeDefined();

        //Check if InsertHistory was written correctly
        const { carGeneralLayer: carGeneralLayerTable2 } =
          await db.core.dumpTable('carGeneralLayer');
        expect(carGeneralLayerTable2?._data.length).toBe(
          staticExample().carGeneralLayer._data.length + 2,
        );
      });

      it('Insert -> Remove Layer', async () => {
        //LayerController Refs
        const carGeneralLayerRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsTableRow: (staticExample().carSliceId._data[0]._hash ||
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
        const removeLayer: Partial<Layer> = {
          remove: {
            VIN1:
              (staticExample().carGeneral._data[1]._hash as string) ||
              ('' as string),
          },
        };

        const insertHistoryFirstRows = await carGeneralLayerController.insert(
          'remove',
          removeLayer,
          origin,
        );
        const insertHistoryFirstRow = insertHistoryFirstRows[0];

        expect(insertHistoryFirstRow).toBeDefined();
        expect(insertHistoryFirstRow.timeId).toBeDefined();
        expect(insertHistoryFirstRow.carGeneralLayerRef).toBeDefined();

        const resultingLayer = await carGeneralLayerController.get(
          insertHistoryFirstRow.carGeneralLayerRef,
        );

        expect(resultingLayer).toBeDefined();
        expect(resultingLayer.carGeneralLayer._data.length).toBe(1);
        expect(rmhsh(resultingLayer.carGeneralLayer._data[0].remove)).toEqual(
          removeLayer.remove,
        );
      });
    });
  });
  describe('CakeController', () => {
    describe('Basic operation', () => {
      it('Init', async () => {
        //CakeController Refs
        const carCakeRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsRow: (staticExample().carSliceId._data[0]._hash ||
            '') as string,
        } as CakeControllerRefs;

        //Create CakeController
        let cakeCtrl: CakeController<'CarCake', Cake>;

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
          ...staticExample().tableCfgs._data[1],
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
          ...{ base: (staticExample().carCake._data[0]._hash || '') as string },
        } as CakeControllerRefs;
        cakeCtrl = new CakeController(core, 'carCake', carCakeRefsWithBase);
        await cakeCtrl.init();
        expect(cakeCtrl).toBeDefined();

        //Valid, w/ refs
        cakeCtrl = new CakeController(core, 'carCake', carCakeRefs);
        await cakeCtrl.init();
        expect(cakeCtrl).toBeDefined();
      });

      it('getChildRefs', async () => {
        //CakeController Refs
        const carCakeRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsRow: (staticExample().carSliceId._data[0]._hash ||
            '') as string,
        } as CakeControllerRefs;

        //Create CakeController
        const carCakeController = await createController(
          'cakes',
          core,
          'carCake',
          carCakeRefs,
        );

        //Get Child Refs
        const childRefs = await carCakeController.getChildRefs(
          staticExample().carCake._data[0]._hash as string,
        );

        expect(childRefs).toBeDefined();
        expect(childRefs.map((ch) => ch.tableKey)).toEqual([
          'carGeneralLayer',
          'carTechnicalLayer',
          'carColorLayer',
        ]);
      });

      it('filterRow', async () => {
        //CakeController Refs
        const carCakeRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsRow: (staticExample().carSliceId._data[0]._hash ||
            '') as string,
        } as CakeControllerRefs;

        //Create CakeController
        const carCakeController = await createController(
          'cakes',
          core,
          'carCake',
          carCakeRefs,
        );

        const filterKey = 'carGeneralLayer';
        const filterValue = staticExample().carCake._data[0].layers[filterKey];

        const positivefilterResult = await carCakeController.filterRow(
          staticExample().carCake._data[0],
          filterKey,
          filterValue,
        );

        expect(positivefilterResult).toBe(true);

        const negativefilterResult = await carCakeController.filterRow(
          staticExample().carCake._data[0],
          filterKey,
          'NonExistingHash',
        );

        expect(negativefilterResult).toBe(false);
      });

      it('Table', async () => {
        //CakeController Refs
        const carCakeRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsRow: (staticExample().carSliceId._data[0]._hash ||
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
        const table = (await carCakeController.table()) as CakesTable;
        expect(table).toBeDefined();
        expect(table._data.map((c) => c._hash).sort()).toEqual(
          staticExample()
            .carCake._data.map((c) => c._hash)
            .sort(),
        );
      });

      it('Get', async () => {
        //CakeController Refs
        const carCakeRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsRow: (staticExample().carSliceId._data[0]._hash ||
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
        const firstRowHash = staticExample().carCake._data[0]._hash as string;
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

        //Read by empty
        const emptyWhere = await carCakeController.get(undefined as any);
        expect(emptyWhere).toEqual({});
      });

      it('Insert', async () => {
        //CakeController Refs
        const carCakeRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsRow: (staticExample().carSliceId._data[0]._hash ||
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
              (staticExample().carGeneralLayer._data[0]._hash as string) || '',
            carTechnicalLayer:
              (staticExample().carTechnicalLayer._data[0]._hash as string) ||
              '',
            carColorLayer:
              (staticExample().carColorLayer._data[0]._hash as string) || '',
          },
          id: 'MyFirstCake',
        };

        const insertHistoryFirstRows = await carCakeController.insert(
          'add',
          carCakeValue,
          origin,
        );
        const insertHistoryFirstRow = insertHistoryFirstRows[0];
        expect(insertHistoryFirstRow).toBeDefined();
        expect(insertHistoryFirstRow.timeId).toBeDefined();
        expect(insertHistoryFirstRow.carCakeRef).toBeDefined();

        //Check if InsertHistory was written correctly
        const { carCake: carCakeTable } = await db.core.dumpTable('carCake');
        expect(carCakeTable?._data.length).toBe(
          staticExample().carCake._data.length + 1,
        );

        //Add another Cake, with previous
        const carCakeValueSecond: CakeValue = {
          layers: {
            carGeneralLayer:
              (staticExample().carGeneralLayer._data[0]._hash as string) || '',
            carTechnicalLayer:
              (staticExample().carTechnicalLayer._data[0]._hash as string) ||
              '',
            carColorLayer:
              (staticExample().carColorLayer._data[0]._hash as string) || '',
          },
          id: 'MySecondCake',
        };

        const insertHistorySecondRows = await carCakeController.insert(
          'add',
          carCakeValueSecond,
          origin,
        );
        const insertHistorySecondRow = insertHistorySecondRows[0];
        expect(insertHistorySecondRow).toBeDefined();
        expect(insertHistorySecondRow.timeId).toBeDefined();
        expect(insertHistorySecondRow.carCakeRef).toBeDefined();

        //Check if InsertHistory was written correctly
        const { carCake: carCakeTable2 } = await db.core.dumpTable('carCake');
        expect(carCakeTable2?._data.length).toBe(
          staticExample().carCake._data.length + 2,
        );
      });

      it('Invalid Insert command', async () => {
        //CakeController Refs
        const carCakeRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsRow: (staticExample().carSliceId._data[0]._hash ||
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
              (staticExample().carGeneralLayer._data[0]._hash as string) || '',
            carTechnicalLayer:
              (staticExample().carTechnicalLayer._data[0]._hash as string) ||
              '',
            carColorLayer:
              (staticExample().carColorLayer._data[0]._hash as string) || '',
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
