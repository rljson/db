// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { rmhsh } from '@rljson/hash';
import { Io, IoMem } from '@rljson/io';
import { Json, JsonH } from '@rljson/json';
import {
  Cake,
  CakesTable,
  createCakeTableCfg,
  createTreesTableCfg,
  Layer,
  LayersTable,
  SliceId,
  SliceIds,
  TableCfg,
  Tree,
  treeFromObject,
  TreesTable,
  TreeWithHash,
} from '@rljson/rljson';

import { beforeEach, describe, expect, it } from 'vitest';

import {
  CakeController,
  CakeControllerRefs,
  CakeValue,
} from '../src/controller/cake-controller';
import { ComponentController } from '../src/controller/component-controller';
import { ControllerRefs, createController } from '../src/controller/controller';
import {
  LayerController,
  LayerControllerRefs,
} from '../src/controller/layer-controller';
import {
  SliceIdController,
  SliceIdControllerRefs,
} from '../src/controller/slice-id-controller';
import { TreeController } from '../src/controller/tree-controller';
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

      it('resolveBaseLayer', async () => {
        //LayerController Refs
        const carGeneralLayerRefs = {
          sliceIdsTable: 'carSliceId',
          sliceIdsTableRow: (staticExample().carSliceId._data[0]._hash ||
            '') as string,
          componentsTable: 'carGeneral',
        } as LayerControllerRefs;

        //Create LayerController
        const carGeneralLayerController = (await createController(
          'layers',
          core,
          'carGeneralLayer',
          carGeneralLayerRefs,
        )) as LayerController<'carGeneralLayer', Layer>;

        const layerHash = staticExample().carGeneralLayer._data[2]
          ._hash as string;
        const {
          ['carGeneralLayer']: { _data: layers },
        } = await carGeneralLayerController.get(layerHash);

        expect(layers.length).toBe(1);

        const layer = layers[0] as Layer;
        expect(layer).toBeDefined();
        expect(Object.keys(rmhsh(layer.add))).toEqual(['VIN11', 'VIN12']);

        const resolvedBaseLayer =
          await carGeneralLayerController.resolveBaseLayer(layer);

        expect(resolvedBaseLayer).toBeDefined();
        expect(Object.keys(rmhsh(resolvedBaseLayer.add))).toEqual([
          'VIN1',
          'VIN2',
          'VIN3',
          'VIN4',
          'VIN5',
          'VIN6',
          'VIN7',
          'VIN8',
          'VIN9',
          'VIN10',
          'VIN11',
          'VIN12',
        ]);
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
  describe('SliceIdController', () => {
    describe('Basic operation', () => {
      it('Init', async () => {
        //Create LayerController
        let sliceIdCtrl: SliceIdController<'SliceId', SliceId[]>;

        //Wrong TableKey
        sliceIdCtrl = new SliceIdController(
          core,
          '#',
          {} as SliceIdControllerRefs,
        );

        await expect(sliceIdCtrl.init()).rejects.toThrow(
          'Table # is not supported by SliceIdController.',
        );

        //Table not of type layers
        // Create a mock table, which is not of type layers
        const mockSliceIdName = 'mockSliceIds';
        const mockCfg = {
          ...staticExample().tableCfgs._data[1],
          ...{ key: mockSliceIdName },
        } as TableCfg;
        await db.core.createTable(rmhsh(mockCfg) as TableCfg);

        sliceIdCtrl = new SliceIdController(core, mockSliceIdName, {});
        await expect(sliceIdCtrl.init()).rejects.toThrow(
          'Table mockSliceIds is not supported by SliceIdController.',
        );

        //Valid w/o refs
        sliceIdCtrl = new SliceIdController(core, 'carSliceId');
        await sliceIdCtrl.init();
        expect(sliceIdCtrl).toBeDefined();

        //Valid w/ refs
        sliceIdCtrl = new SliceIdController(
          core,
          'carSliceId',
          {} as SliceIdControllerRefs,
        );
        await sliceIdCtrl.init();
        expect(sliceIdCtrl).toBeDefined();

        //Valid w/ base only
        const carSliceIdRefsWithBase = {
          base: (staticExample().carSliceId._data[0]._hash || '') as string,
        } as SliceIdControllerRefs;
        sliceIdCtrl = new SliceIdController(
          core,
          'carSliceId',
          carSliceIdRefsWithBase,
        );
        await sliceIdCtrl.init();
        expect(sliceIdCtrl).toBeDefined();

        //Base not existing
        const missingBaseLayerRefs = {
          base: 'NonExisting',
        } as SliceIdControllerRefs;
        sliceIdCtrl = new SliceIdController(
          core,
          'carSliceId',
          missingBaseLayerRefs,
        );
        await expect(sliceIdCtrl.init()).rejects.toThrow(
          'Base sliceId NonExisting does not exist.',
        );
      });

      it('filterRow', async () => {
        //SliceIdController Refs
        const sliceIdControllerRefs = {} as ControllerRefs;

        //Create SliceIdController
        const carSliceIdController = await createController(
          'sliceIds',
          core,
          'carSliceId',
          sliceIdControllerRefs,
        );

        const filterKey = '';
        const filterValue = Object.values(
          staticExample().carSliceId._data[0].add,
        )[0];

        const positivefilterResult = await carSliceIdController.filterRow(
          staticExample().carSliceId._data[0],
          filterKey,
          filterValue,
        );

        expect(positivefilterResult).toBe(true);

        const negativefilterResult = await carSliceIdController.filterRow(
          staticExample().carSliceId._data[0],
          filterKey,
          'NonExistingHash',
        );
        expect(negativefilterResult).toBe(false);
      });

      it('Table', async () => {
        //SliceIdController Refs
        const sliceIdControllerRefs = {} as ControllerRefs;

        //Create SliceIdController
        const carSliceIdController = await createController(
          'sliceIds',
          core,
          'carSliceId',
          sliceIdControllerRefs,
        );

        //Read Table
        const table = (await carSliceIdController.table()) as LayersTable;
        expect(table).toBeDefined();
        expect(table._data.map((l) => l._hash).sort()).toEqual(
          staticExample()
            .carSliceId._data.map((l) => l._hash)
            .sort(),
        );
      });
      it('Get', async () => {
        //SliceIdController Refs
        const sliceIdControllerRefs = {} as ControllerRefs;

        //Create SliceIdController
        const carSliceIdController = await createController(
          'sliceIds',
          core,
          'carSliceId',
          sliceIdControllerRefs,
        );

        //Read existing Row by Hash
        const firstRowHash = staticExample().carSliceId._data[0]
          ._hash as string;
        const {
          ['carSliceId']: { _data: firstRows },
        } = await carSliceIdController.get(firstRowHash);
        const firstRow = firstRows[0];
        expect(firstRow).toBeDefined();
        expect(firstRow._hash!).toBeDefined();
        expect(firstRow._hash!).toStrictEqual(firstRowHash);

        //Read existing Row by where object
        const {
          ['carSliceId']: { _data: firstRowsByWhere },
        } = await carSliceIdController.get(
          rmhsh(staticExample().carSliceId._data[0]),
        );
        const firstRowByWhere = firstRowsByWhere[0];
        expect(firstRowByWhere).toBeDefined();
        expect(firstRowByWhere._hash!).toBeDefined();
        expect(firstRowByWhere).toStrictEqual(firstRow);

        //Read non-existing Row
        const nonExistingRow = await carSliceIdController.get('#');
        expect(nonExistingRow['carSliceId']._data.length).toBe(0);

        //Read by empty
      });

      it('resolveBaseSliceIds', async () => {
        //SliceIdController Refs
        const sliceIdControllerRefs = {} as ControllerRefs;

        //Create SliceIdController
        const carSliceIdController = (await createController(
          'sliceIds',
          core,
          'carSliceId',
          sliceIdControllerRefs,
        )) as SliceIdController<'carSliceId', SliceId[]>;

        const sliceIdHash = staticExample().carSliceId._data[2]._hash as string;
        const {
          ['carSliceId']: { _data: sliceIds },
        } = await carSliceIdController.get(sliceIdHash);

        expect(sliceIds.length).toBe(1);

        const sliceId = sliceIds[0] as SliceIds;
        expect(sliceId).toBeDefined();
        expect(sliceId.add).toEqual(['VIN11', 'VIN12']);

        const resolvedBaseSliceIds =
          await carSliceIdController.resolveBaseSliceIds(sliceId);

        expect(resolvedBaseSliceIds).toBeDefined();
        expect(resolvedBaseSliceIds.add).toEqual([
          'VIN1',
          'VIN2',
          'VIN3',
          'VIN4',
          'VIN5',
          'VIN6',
          'VIN7',
          'VIN8',
          'VIN9',
          'VIN10',
          'VIN11',
          'VIN12',
        ]);
      });

      it('Insert -> Invalid Command', async () => {
        //SliceIdController Refs
        const sliceIdControllerRefs = {} as ControllerRefs;

        //Create SliceIdController
        const carSliceIdController = (await createController(
          'sliceIds',
          core,
          'carSliceId',
          sliceIdControllerRefs,
        )) as SliceIdController<'carSliceId', SliceId[]>;

        //Invalid Command
        const origin = 'H45H';
        const sliceIdValue: SliceId[] = ['VIN13', 'VIN14'];

        await expect(
          carSliceIdController.insert('update' as any, sliceIdValue, origin),
        ).rejects.toThrow(
          'Command update is not supported by SliceIdController.',
        );
      });

      it('Insert -> Add SliceIds', async () => {
        //SliceIdController Refs
        const sliceIdControllerRefs = {} as ControllerRefs;

        //Create SliceIdController
        const carSliceIdController = (await createController(
          'sliceIds',
          core,
          'carSliceId',
          sliceIdControllerRefs,
        )) as SliceIdController<'carSliceId', SliceId[]>;

        //Add SliceIds
        const origin = 'H45H';
        const sliceIdValue: SliceId[] = ['VIN13', 'VIN14'];

        const insertHistoryFirstRows = await carSliceIdController.insert(
          'add',
          sliceIdValue,
          origin,
        );
        const insertHistoryFirstRow = insertHistoryFirstRows[0];

        expect(insertHistoryFirstRow).toBeDefined();
        expect(insertHistoryFirstRow.timeId).toBeDefined();
        expect(insertHistoryFirstRow.carSliceIdRef).toBeDefined();

        //Check if InsertHistory was written correctly
        const { carSliceId: carSliceIdTable } = await db.core.dumpTable(
          'carSliceId',
        );
        expect(carSliceIdTable?._data.length).toBe(
          staticExample().carSliceId._data.length + 1,
        );

        //Add another SliceIds
        const sliceIdSecondValue: SliceId[] = ['VIN15', 'VIN16'];

        const insertHistorySecondRows = await carSliceIdController.insert(
          'add',
          sliceIdSecondValue,
          origin,
        );
        const insertHistorySecondRow = insertHistorySecondRows[0];

        expect(insertHistorySecondRow).toBeDefined();
        expect(insertHistorySecondRow.timeId).toBeDefined();
        expect(insertHistorySecondRow.carSliceIdRef).toBeDefined();

        //Check if InsertHistory was written correctly
        const { carSliceId: carSliceIdTable2 } = await db.core.dumpTable(
          'carSliceId',
        );
        expect(carSliceIdTable2?._data.length).toBe(
          staticExample().carSliceId._data.length + 2,
        );
      });

      it('Insert -> Remove SliceIds', async () => {
        //SliceIdController Refs
        const sliceIdControllerRefs = {} as ControllerRefs;

        //Create SliceIdController
        const carSliceIdController = (await createController(
          'sliceIds',
          core,
          'carSliceId',
          sliceIdControllerRefs,
        )) as SliceIdController<'carSliceId', SliceId[]>;

        //Add SliceIds
        const origin = 'H45H';
        const sliceIdValue: SliceId[] = ['VIN5', 'VIN6'];

        const insertHistoryFirstRows = await carSliceIdController.insert(
          'remove',
          sliceIdValue,
          origin,
        );
        const insertHistoryFirstRow = insertHistoryFirstRows[0];

        expect(insertHistoryFirstRow).toBeDefined();
        expect(insertHistoryFirstRow.timeId).toBeDefined();
        expect(insertHistoryFirstRow.carSliceIdRef).toBeDefined();

        const resultingSliceIds = await carSliceIdController.get(
          insertHistoryFirstRow.carSliceIdRef,
        );

        expect(resultingSliceIds).toBeDefined();
        expect(resultingSliceIds.carSliceId._data.length).toBe(1);
        expect(resultingSliceIds.carSliceId._data[0].remove).toEqual(
          sliceIdValue,
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
  describe('TreeController', () => {
    const treeObject: Json = { a: 1, b: { c: 2, d: [3, 4] } };
    let trees: TreeWithHash[];
    let treesTable: TreesTable;
    const treesTableCfg: TableCfg = createTreesTableCfg('exampleTree');
    let treeIo: Io;
    let treeCore: Core;

    beforeEach(async () => {
      trees = treeFromObject(treeObject);
      treesTable = {
        _type: 'trees',
        _data: trees,
      };

      treeIo = new IoMem();
      await treeIo.init();
      await treeIo.isReady();

      treeCore = new Core(treeIo);

      await treeCore.createTable(rmhsh(treesTableCfg) as TableCfg);
      await treeCore.import({ exampleTree: treesTable });

      const someCakeTableCfgs = createCakeTableCfg('notATreeCake');
      await treeCore.createTable(rmhsh(someCakeTableCfgs) as TableCfg);
    });
    describe('Basic operation', () => {
      it('Init', async () => {
        //Create TreeController
        let treeCtrl: TreeController<'exampleTree', Tree>;

        //Wrong TableKey
        treeCtrl = new TreeController(treeCore, '#');
        await expect(treeCtrl.init()).rejects.toThrow(
          'Table # is not supported by TreeController.',
        );

        //Table not of type trees
        treeCtrl = new TreeController(treeCore, 'notATreeCake');
        await expect(treeCtrl.init()).rejects.toThrow(
          'Table notATreeCake is not supported by TreeController.',
        );

        //Valid
        treeCtrl = new TreeController(treeCore, 'exampleTree');
        await treeCtrl.init();
        expect(treeCtrl).toBeDefined();
      });

      it('getChildRefs', async () => {
        //Create TreeController
        const treeController = await createController(
          'trees',
          treeCore,
          'exampleTree',
        );

        //Get Child Refs
        const childRefs = await treeController.getChildRefs(
          trees[3]._hash as string,
        );

        expect(childRefs).toBeDefined();
        expect(childRefs.length).toBe(2);
      });

      it('Table', async () => {
        //Create TreeController
        const treeController = await createController(
          'trees',
          treeCore,
          'exampleTree',
        );

        //Read Table
        const table = (await treeController.table()) as TreesTable;
        expect(table).toBeDefined();
        expect(table._data.map((t) => t._hash).sort()).toEqual(
          trees.map((t) => t._hash).sort(),
        );
      });

      it('Get', async () => {
        //Create TreeController
        const treeController = await createController(
          'trees',
          treeCore,
          'exampleTree',
        );

        //Read existing Row By Hash
        const firstRowHash = trees[3]._hash as string;
        const {
          ['exampleTree']: { _data: firstRows },
        } = await treeController.get(firstRowHash);
        const firstRow = firstRows.find((r) => r._hash === firstRowHash);
        expect(firstRow).toBeDefined();
        expect(firstRow._hash!).toBeDefined();
        expect(firstRow._hash!).toStrictEqual(firstRowHash);

        //Read existing Row By where object
        const {
          ['exampleTree']: { _data: firstRowsByWhere },
        } = await treeController.get(rmhsh(firstRow) as string);
        const firstRowByWhere = firstRowsByWhere.find(
          (r) => r._hash === firstRowHash,
        );
        expect(firstRowByWhere).toBeDefined();
        expect(firstRowByWhere._hash!).toBeDefined();
        expect(firstRowByWhere).toStrictEqual(firstRow);

        //Read non-existing Row
        const nonExistingRow = await treeController.get('#');
        expect(nonExistingRow['exampleTree']._data.length).toBe(0);

        //Read by invalid where
        await expect(treeController.get(5 as any)).rejects.toThrow(
          'Multiple trees found for where clause. Please specify a more specific query.',
        );
      });

      it('buildTreeFromTrees', async () => {
        //Create TreeController
        const treeController = (await createController(
          'trees',
          treeCore,
          'exampleTree',
        )) as TreeController<'exampleTree', Tree>;

        const treeObjectConverted = await treeController.buildTreeFromTrees(
          trees,
        );

        expect(rmhsh(treeObjectConverted)).toEqual({
          a: {
            value: 1,
          },
          b: {
            c: {
              value: 2,
            },
            d: {
              value: [3, 4],
            },
          },
        });

        const treeObjectConvertedSingle =
          await treeController.buildTreeFromTrees([trees[0]]);

        expect(rmhsh(treeObjectConvertedSingle)).toEqual({
          a: {
            value: 1,
          },
        });

        const treeObjectConvertedEmpty =
          await treeController.buildTreeFromTrees([]);

        expect(rmhsh(treeObjectConvertedEmpty)).toEqual({});
      });

      it('buildCellsFromTree', async () => {
        //Create TreeController
        const treeController = (await createController(
          'trees',
          treeCore,
          'exampleTree',
        )) as TreeController<'exampleTree', Tree>;

        const cells = await treeController.buildCellsFromTree(trees);

        expect(cells.length).toBe(3);

        const cell0 = { ...cells[0], route: cells[0].route.flat };
        expect(rmhsh(cell0 as any)).toEqual({
          value: {
            value: 1,
          },
          row: {
            value: 1,
          },
          path: [['a']],
          route: '/a',
        });

        const cell1 = { ...cells[1], route: cells[1].route.flat };
        expect(rmhsh(cell1 as any)).toEqual({
          value: {
            value: 2,
          },
          row: {
            value: 2,
          },
          path: [['b', 'c']],
          route: '/b/c',
        });

        const cell2 = { ...cells[2], route: cells[2].route.flat };
        expect(rmhsh(cell2 as any)).toEqual({
          value: {
            value: [3, 4],
          },
          row: {
            value: [3, 4],
          },
          path: [['b', 'd']],
          route: '/b/d',
        });

        const empty = await treeController.buildCellsFromTree([]);
        expect(empty.length).toBe(0);
      });

      it('Insert', async () => {
        //Create TreeController
        const treeController = await createController(
          'trees',
          treeCore,
          'exampleTree',
        );

        //Add Tree
        const origin = 'H45H';

        const insertHistoryFirstRows = await treeController.insert(
          'add',
          trees[0],
          origin,
        );
        const insertHistoryFirstRow = insertHistoryFirstRows[0];
        expect(insertHistoryFirstRow).toBeDefined();
        expect(insertHistoryFirstRow.timeId).toBeDefined();
        expect(insertHistoryFirstRow.exampleTreeRef).toBeDefined();

        //Check if InsertHistory was written correctly
        const { exampleTree: exampleTreeTable } = await treeCore.dumpTable(
          'exampleTree',
        );
        expect(exampleTreeTable?._data.length).toBe(treesTable._data.length);

        //Add another Tree
        const insertHistorySecondRows = await treeController.insert(
          'add',
          treeFromObject({ x: 10, y: { z: 20 } })[0],
          origin,
        );
        const insertHistorySecondRow = insertHistorySecondRows[0];
        expect(insertHistorySecondRow).toBeDefined();
        expect(insertHistorySecondRow.timeId).toBeDefined();
        expect(insertHistorySecondRow.exampleTreeRef).toBeDefined();

        //Check if InsertHistory was written correctly
        const { exampleTree: exampleTreeTable2 } = await treeCore.dumpTable(
          'exampleTree',
        );
        expect(exampleTreeTable2?._data.length).toBe(
          treesTable._data.length + 1,
        );
      });
    });
  });
});
