// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip, rmhsh } from '@rljson/hash';
import { IoMem } from '@rljson/io';
import { equals } from '@rljson/json';
import {
  createEditHistoryTableCfg,
  createEditTableCfg,
  createMultiEditTableCfg,
  Edit,
  EditHistory,
  EditsTable,
  InsertHistoryRow,
  MultiEdit,
  MultiEditsTable,
  Route,
  TableKey,
  timeId,
} from '@rljson/rljson';

import { beforeEach, describe, expect, it } from 'vitest';

import { Db } from '../../src/db';
import { exampleEditColumnSelection } from '../../src/edit/edit';
import {
  exampleEditActionColumnSelection,
  exampleEditActionColumnSelectionOnlySomeColumns,
  exampleEditActionPutComponent,
  exampleEditActionRowFilter,
  exampleEditActionRowSort,
  exampleEditActionSetValue,
  exampleEditSetValueReferenced,
} from '../../src/edit/edit-action';
import { MultiEditManager } from '../../src/edit/multi-edit-manager';
import { MultiEditProcessor } from '../../src/edit/multi-edit-processor';
import { staticExample } from '../../src/example-static/example-static';
import { ColumnSelection } from '../../src/join/selection/column-selection';

describe('MultiEditProcessor', () => {
  let db: Db;

  const cakeKey = 'carCake';
  const cakeRef = staticExample().carCake._data[0]._hash as string;

  beforeEach(async () => {
    //Init io
    const io = new IoMem();
    await io.init();
    await io.isReady();

    //Init Core
    db = new Db(io);

    //Create Tables for TableCfgs in carsExample
    for (const tableCfg of staticExample().tableCfgs._data) {
      await db.core.createTableWithInsertHistory(tableCfg);
    }

    //Create Tables for Edit TableCfgs
    await db.core.createTable(createMultiEditTableCfg(cakeKey));
    await db.core.createTable(createEditTableCfg(cakeKey));
    await db.core.createTable(createEditHistoryTableCfg(cakeKey));

    //Import Data
    await db.core.import(staticExample());
  });

  describe('Constructor', () => {
    it('should be defined', async () => {
      const editActionColumnSelection = exampleEditActionColumnSelection();
      const edit: Edit = {
        name: 'Test Edit',
        action: editActionColumnSelection,
        _hash: '',
      } as Edit;

      const editInsertTree = {
        [`${cakeKey}Edits`]: {
          _data: [edit],
          _type: 'edits',
        } as EditsTable,
      };

      const [{ [`${cakeKey}EditsRef`]: editRef }] = await db.insert(
        Route.fromFlat(`/${cakeKey}Edits`),
        editInsertTree,
        { skipHistory: true },
      );

      const multiEdit: MultiEdit = {
        previous: null,
        edit: editRef!,
        _hash: '',
      } as MultiEdit;

      const proc = await MultiEditProcessor.fromMultiEdit(
        db,
        cakeKey,
        cakeRef,
        multiEdit,
      );

      expect(proc).toBeDefined();
      expect(proc.join).toBeDefined();
    });
  });

  describe('edit', async () => {
    let multiEditProc: MultiEditProcessor;
    let multiEdit: MultiEdit;

    beforeEach(async () => {
      const editActionColumnSelection = exampleEditActionColumnSelection();
      const edit: Edit = {
        name: 'Select: brand, type, serviceIntervals, isElectric, height, width, length, engine, repairedByWorkshop',
        action: editActionColumnSelection,
        _hash: '',
      } as Edit;

      const editInsertTree = {
        [`${cakeKey}Edits`]: {
          _data: [edit],
          _type: 'edits',
        } as EditsTable,
      };

      const [{ [`${cakeKey}EditsRef`]: editRef }] = await db.insert(
        Route.fromFlat(`/${cakeKey}Edits`),
        editInsertTree,
        { skipHistory: true },
      );

      multiEdit = hip<MultiEdit>({
        previous: null,
        edit: editRef!,
        _hash: '',
      }) as MultiEdit;

      multiEditProc = await MultiEditProcessor.fromMultiEdit(
        db,
        cakeKey,
        cakeRef,
        multiEdit,
      );
    });

    it('should add an Edit to the MultiEditProcessor', async () => {
      const editActionRowFilter = exampleEditActionRowFilter();

      const edit: Edit = hip<Edit>({
        name: 'Filter: isElectric = true, length > 4000',
        action: editActionRowFilter,
        _hash: '',
      }) as Edit;

      await multiEditProc.edit(edit);

      expect(multiEditProc.join.rows.length).toBeGreaterThan(0);

      //Check filtered values
      // isElectric == true
      expect(
        multiEditProc.join.rows
          .flatMap((r) => r[3])
          .every((isElectric) => isElectric == true),
      ).toBe(true);

      // length > 4000
      const lengths = multiEditProc.join.rows.flatMap((r) => r[6]);

      expect(lengths.every((length) => length > 4000)).toBe(true);

      //Check MultiEdit updated
      expect(multiEditProc.multiEdit.previous).toBe(multiEdit._hash);
      expect(multiEditProc.multiEdit.edit).toBe(edit._hash);
    });

    it('should apply a PutComponent edit incrementally (Switch B)', async () => {
      // multiEditProc already has a Join from the ColumnSelection edit
      // applied in beforeEach, which selects columns under
      // carGeneralLayer/carGeneral -- so the incoming putComponent finds
      // an existing column touching its target layer, exactly as
      // multi-edit-processor.ts's Switch B (`this._join.putComponent()`)
      // requires.
      const editActionPutComponent = exampleEditActionPutComponent();
      const putSliceId = editActionPutComponent.data.sliceId; // VIN99 (new)

      const edit: Edit = hip<Edit>({
        name: 'Put: carGeneral component for a new document',
        action: editActionPutComponent,
        _hash: '',
      }) as Edit;

      await multiEditProc.edit(edit);

      const inserts = multiEditProc.join.insert();
      expect(inserts.length).toBe(1);

      const { route, tree } = inserts[0];
      expect(route.flat).toBe('/carCake/carGeneralLayer/carGeneral');

      const cakeRow = (tree as any).carCake._data[0];
      const layerRow = cakeRow.layers.carGeneralLayer._data[0];
      // Compare content only: the pipeline (rmhsh in Switch B, then
      // fresh hashing on actual insert) does not preserve the '' hash
      // placeholders from the fixture.
      expect(
        rmhsh(layerRow.add[putSliceId].carGeneral._data[0]),
      ).toEqual(rmhsh(editActionPutComponent.data.component));

      // Switch B persisted the extended slice set, and the tree re-points
      // both the layer's and the cake's slice-set refs at it.
      expect(layerRow.sliceIdsTableRow).toBe(cakeRow.sliceIdsRow);

      //Check MultiEdit updated
      expect(multiEditProc.multiEdit.previous).toBe(multiEdit._hash);
      expect(multiEditProc.multiEdit.edit).toBe(edit._hash);
    });
  });

  describe('publish', async () => {
    let multiEditProc: MultiEditProcessor;

    beforeEach(async () => {
      const editActionColumnSelection = exampleEditActionColumnSelection();
      const edit: Edit = {
        name: 'Select: brand, type, serviceIntervals, isElectric, height, width, length, engine, repairedByWorkshop',
        action: editActionColumnSelection,
        _hash: '',
      } as Edit;

      const editInsertTree = {
        [`${cakeKey}Edits`]: {
          _data: [edit],
          _type: 'edits',
        } as EditsTable,
      };

      const [{ [`${cakeKey}EditsRef`]: editRef }] = await db.insert(
        Route.fromFlat(`/${cakeKey}Edits`),
        editInsertTree,
        { skipHistory: true },
      );

      const multiEdit: MultiEdit = hip<MultiEdit>({
        previous: null,
        edit: editRef!,
        _hash: '',
      });

      multiEditProc = await MultiEditProcessor.fromMultiEdit(
        db,
        cakeKey,
        cakeRef,
        multiEdit,
      );
    });

    it('should publish the MultiEditProcessor changes to the Db', async () => {
      const editSetValue = exampleEditActionSetValue();

      const edit: Edit = hip<Edit>({
        name: 'Set: serviceIntervals = [15000,30000,45000,60000]',
        action: editSetValue,
        _hash: '',
      });

      await multiEditProc.edit(edit);

      const multiEdit = { ...multiEditProc.multiEdit };

      const multiEditProcPublished = await multiEditProc.publish();

      expect(multiEditProcPublished.cakeRef).toBeDefined();

      const writtenCakeRef = multiEditProcPublished.cakeRef;

      //Check Data updated
      const { cell: writtenCarGeneral } = await db.get(
        Route.fromFlat(
          `/${cakeKey}@${writtenCakeRef}/carGeneralLayer/carGeneral/serviceIntervals`,
        ),
        {},
      );

      expect(writtenCarGeneral.length).toBe(8);
      expect(
        writtenCarGeneral
          .map((c) => c.value)
          .every((serviceIntervals: any) =>
            equals([15000, 30000, 45000, 60000], serviceIntervals),
          ),
      ).toBe(true);

      //Check MultiEdit saved
      const { cell: multiEdits } = await db.get(
        Route.fromFlat(`${cakeKey}MultiEdits`),
        multiEdit._hash,
      );

      expect(multiEdits.length).toBe(1);
      expect(multiEdits[0].row).toEqual(multiEdit);

      //Check Head updated
      const { cell: insertCells } = await db.get(
        Route.fromFlat(`${cakeKey}InsertHistory/${cakeKey}Ref`),
        {},
      );

      expect(insertCells.length).toBe(1);
      expect(insertCells[0].value === writtenCakeRef).toBe(true);
    });
  });

  describe('fromEditHistory', () => {
    let editHistory: EditHistory;
    let edit: Edit;
    let multiEdit: MultiEdit;

    beforeEach(async () => {
      const editActionColumnSelection = exampleEditActionColumnSelection();
      edit = hip<Edit>({
        name: 'Select: brand, type, serviceIntervals, isElectric, height, width, length, engine, repairedByWorkshop',
        action: editActionColumnSelection,
        _hash: '',
      });

      const { [cakeKey + 'EditsRef']: editRef } = (
        await db.addEdit(cakeKey, edit)
      )[0] as any;

      multiEdit = hip<MultiEdit>({
        previous: null,
        edit: editRef!,
        _hash: '',
      });

      const { [cakeKey + 'MultiEditsRef']: multiEditRef } = (
        await db.addMultiEdit(cakeKey, multiEdit)
      )[0] as any;

      editHistory = hip<EditHistory>({
        timeId: timeId(),
        dataRef: cakeRef,
        multiEditRef: multiEditRef!,
        previous: [],
        _hash: '',
      });

      await db.addEditHistory(cakeKey, editHistory);
    });

    it('should be defined', async () => {
      const proc = await MultiEditProcessor.fromEditHistory(
        db,
        cakeKey,
        editHistory,
      );
      expect(proc).toBeDefined();
      expect(proc.join).toBeDefined();

      expect(proc.multiEdit).toBeDefined();
      expect(proc.multiEdit.edit).toBe(edit._hash);

      expect(proc.join.rows.length).toBeGreaterThan(0);
    });
  });

  describe('fromMultiEdit', async () => {
    describe('Single Edit', async () => {
      it('ColumnSelection', async () => {
        const editActionColumnSelection = exampleEditActionColumnSelection();
        const edit: Edit = {
          name: 'Select: brand, type, serviceIntervals, isElectric, height, width, length, engine, repairedByWorkshop',
          action: editActionColumnSelection,
          _hash: '',
        } as Edit;

        const editInsertTree = {
          [`${cakeKey}Edits`]: {
            _data: [edit],
            _type: 'edits',
          } as EditsTable,
        };

        const [{ [`${cakeKey}EditsRef`]: editRef }] = await db.insert(
          Route.fromFlat(`/${cakeKey}Edits`),
          editInsertTree,
          { skipHistory: true },
        );

        const multiEdit: MultiEdit = {
          previous: null,
          edit: editRef!,
          _hash: '',
        } as MultiEdit;

        const proc = await MultiEditProcessor.fromMultiEdit(
          db,
          cakeKey,
          cakeRef,
          multiEdit,
        );

        expect(proc.join.rows.length).toBeGreaterThan(0);
      });

      it('RowFilter', async () => {
        const editActionRowFilter = exampleEditActionRowFilter();

        const edit: Edit = {
          name: 'Filter: isElectric = true, length > 4000',
          action: editActionRowFilter,
          _hash: '',
        } as Edit;
        const editInsertTree = {
          [`${cakeKey}Edits`]: {
            _data: [edit],
            _type: 'edits',
          } as EditsTable,
        };

        const [{ [`${cakeKey}EditsRef`]: editRef }] = await db.insert(
          Route.fromFlat(`/${cakeKey}Edits`),
          editInsertTree,
          { skipHistory: true },
        );

        const multiEdit: MultiEdit = {
          previous: null,
          edit: editRef!,
          _hash: '',
        } as MultiEdit;

        const proc = await MultiEditProcessor.fromMultiEdit(
          db,
          cakeKey,
          cakeRef,
          multiEdit,
        );

        expect(proc.join.rows.length).toBeGreaterThan(0);
      });

      it('RowSort', async () => {
        const editActionRowSort = exampleEditActionRowSort();

        const edit: Edit = {
          name: 'Sort: brand ASC',
          action: editActionRowSort,
          _hash: '',
        } as Edit;

        const editInsertTree = {
          [`${cakeKey}Edits`]: {
            _data: [edit],
            _type: 'edits',
          } as EditsTable,
        };

        const [{ [`${cakeKey}EditsRef`]: editRef }] = await db.insert(
          Route.fromFlat(`/${cakeKey}Edits`),
          editInsertTree,
          { skipHistory: true },
        );

        const multiEdit: MultiEdit = hip<MultiEdit>({
          previous: null,
          edit: editRef!,
          _hash: '',
        });

        const proc = await MultiEditProcessor.fromMultiEdit(
          db,
          cakeKey,
          cakeRef,
          multiEdit,
        );

        expect(proc.join.rows.length).toBeGreaterThan(0);
      });

      it('SetValue', async () => {
        const editActionSetValue = exampleEditActionSetValue();

        const edit: Edit = {
          name: 'Set: serviceIntervals = [15000,30000,45000,60000]',
          action: editActionSetValue,
          _hash: '',
        } as Edit;

        const editInsertTree = {
          [`${cakeKey}Edits`]: {
            _data: [edit],
            _type: 'edits',
          } as EditsTable,
        };

        const [{ [`${cakeKey}EditsRef`]: editRef }] = await db.insert(
          Route.fromFlat(`/${cakeKey}Edits`),
          editInsertTree,
          { skipHistory: true },
        );

        const multiEdit: MultiEdit = {
          previous: null,
          edit: editRef!,
          _hash: '',
        } as MultiEdit;

        const proc = await MultiEditProcessor.fromMultiEdit(
          db,
          cakeKey,
          cakeRef,
          multiEdit,
        );

        expect(proc.join.rows.length).toBeGreaterThan(0);
      });

      it('SetValue Referenced & Insert', async () => {
        const edit: Edit = {
          name: 'Set: length = 4800',
          action: exampleEditSetValueReferenced(),
          _hash: '',
        } as Edit;

        const editInsertTree = {
          [`${cakeKey}Edits`]: {
            _data: [edit],
            _type: 'edits',
          } as EditsTable,
        };

        const [{ [`${cakeKey}EditsRef`]: editRef }] = await db.insert(
          Route.fromFlat(`/${cakeKey}Edits`),
          editInsertTree,
          { skipHistory: true },
        );

        const multiEdit: MultiEdit = {
          previous: null,
          edit: editRef!,
          _hash: '',
        } as MultiEdit;

        const proc = await MultiEditProcessor.fromMultiEdit(
          db,
          cakeKey,
          cakeRef,
          multiEdit,
        );

        expect(proc.join.rows.flat().every((c) => c.includes(4800))).toBe(true);

        const inserts = proc.join.insert();
        expect(inserts.length).toBe(1);

        const insertResults: InsertHistoryRow<any>[] = [];
        for (const insert of inserts) {
          insertResults.push(...(await db.insert(insert.route, insert.tree)));
        }

        expect(insertResults).toBeDefined();
        expect(insertResults.length).toBe(1);

        const writtenCakeRef = insertResults[0][`${cakeKey}Ref`] as string;
        const { rljson: writtenData } = await db.get(
          Route.fromFlat(
            `/${cakeKey}@${writtenCakeRef}/carTechnicalLayer/carTechnical/carDimensions/length`,
          ),
          {
            carTechnicalLayer: {
              carTechnical: {
                carDimensions: {
                  length: 4800,
                },
              },
            },
          },
        );

        expect(writtenData['carDimensions']._data.length).toBe(6);
        expect(
          writtenData['carDimensions']._data.every(
            (d: any) => d['length'] === 4800,
          ),
        ).toBe(true);
      });

      it('PutComponent (new document, Switch A)', async () => {
        // No prior edit -> _join is still null -> exercises Switch A,
        // which seeds a minimal one-column Join via a single targeted
        // db.get() (not db.join(), which would be O(every slice)) and
        // then calls Join.putComponent() on it. The example puts a
        // brand-new sliceId (VIN99), so this proves INSERT, not update.
        const editActionPutComponent = exampleEditActionPutComponent();
        const putSliceId = editActionPutComponent.data.sliceId; // VIN99

        const edit: Edit = {
          name: 'Put: carGeneral component for a new document',
          action: editActionPutComponent,
          _hash: '',
        } as Edit;

        const editInsertTree = {
          [`${cakeKey}Edits`]: {
            _data: [edit],
            _type: 'edits',
          } as EditsTable,
        };

        const [{ [`${cakeKey}EditsRef`]: editRef }] = await db.insert(
          Route.fromFlat(`/${cakeKey}Edits`),
          editInsertTree,
          { skipHistory: true },
        );

        const multiEdit: MultiEdit = {
          previous: null,
          edit: editRef!,
          _hash: '',
        } as MultiEdit;

        const proc = await MultiEditProcessor.fromMultiEdit(
          db,
          cakeKey,
          cakeRef,
          multiEdit,
        );

        const inserts = proc.join.insert();
        expect(inserts.length).toBe(1);

        const { route, tree } = inserts[0];
        expect(route.flat).toBe('/carCake/carGeneralLayer/carGeneral');

        const cakeRow = (tree as any).carCake._data[0];
        const layerRow = cakeRow.layers.carGeneralLayer._data[0];
        // Compare content only -- inserting the Edit row itself already
        // computed real hashes for the (previously '') placeholders.
        expect(
          rmhsh(layerRow.add[putSliceId].carGeneral._data[0]),
        ).toEqual(rmhsh(editActionPutComponent.data.component));

        // Publish and verify the new document round-trips through
        // db.insert(). Switch A already persisted the extended slice set
        // during _process, so both read paths below resolve VIN99.
        const insertResults = await db.insert(route, tree);
        const writtenCakeRef = insertResults[0][`${cakeKey}Ref`] as string;

        // (a) sliceId-filtered db.get returns the new component.
        const { cell: brandCells } = await db.get(
          Route.fromFlat(
            `/${cakeKey}@${writtenCakeRef}/carGeneralLayer/carGeneral/brand`,
          ),
          {},
          undefined,
          [putSliceId],
        );
        expect(brandCells.length).toBe(1);
        expect(brandCells[0].value).toBe(
          editActionPutComponent.data.component.brand,
        );

        // (b) the sliceId-driven db.join (reconstruction engine) sees it.
        const reconSelection = new ColumnSelection([
          {
            key: 'brand',
            route: `${cakeKey}/carGeneralLayer/carGeneral/brand`,
            alias: 'brand',
            titleLong: '',
            titleShort: '',
            type: 'jsonValue',
            _hash: '',
          },
        ]);
        const reconJoin = await db.join(
          reconSelection,
          cakeKey,
          writtenCakeRef,
        );
        expect(reconJoin.rowIndices).toContain(putSliceId);
        // cakeRef here is carCake._data[0] (8 base slices VIN1..VIN8),
        // so the new document brings the total to 9.
        expect(reconJoin.rowIndices.length).toBe(9);
      });
    });
    describe('Multiple Edits', async () => {
      let multiEditProc: MultiEditProcessor;

      beforeEach(async () => {
        const editColumnSelection: Edit = exampleEditColumnSelection();

        const editRowFilter: Edit = {
          name: 'Filter: isElectric = true, length > 4000',
          action: exampleEditActionRowFilter(),
          _hash: '',
        } as Edit;

        const editRowSort: Edit = {
          name: 'Sort: brand ASC',
          action: exampleEditActionRowSort(),
          _hash: '',
        } as Edit;

        const editSetValue: Edit = {
          name: 'Set: serviceIntervals = [15000,30000,45000,60000]',
          action: exampleEditActionSetValue(),
          _hash: '',
        } as Edit;

        const editColumnSelectionSomeColumns: Edit = {
          name: 'Select: brand, type, serviceIntervals, isElectric, length',
          action: exampleEditActionColumnSelectionOnlySomeColumns(),
          _hash: '',
        } as Edit;

        const editInsertTrees: Record<TableKey, EditsTable>[] = [
          {
            [`${cakeKey}Edits`]: {
              _data: [editColumnSelection],
              _type: 'edits',
            } as EditsTable,
          },
          {
            [`${cakeKey}Edits`]: {
              _data: [editRowFilter],
              _type: 'edits',
            } as EditsTable,
          },
          {
            [`${cakeKey}Edits`]: {
              _data: [editRowSort],
              _type: 'edits',
            } as EditsTable,
          },
          {
            [`${cakeKey}Edits`]: {
              _data: [editSetValue],
              _type: 'edits',
            } as EditsTable,
          },
          {
            [`${cakeKey}Edits`]: {
              _data: [editColumnSelectionSomeColumns],
              _type: 'edits',
            } as EditsTable,
          },
        ];

        const editRefs: string[] = [];

        for (const editInsertTree of editInsertTrees) {
          //Insert Edit

          const results = await db.insert(
            Route.fromFlat(`/${cakeKey}Edits`),
            editInsertTree,
            { skipHistory: true },
          );

          editRefs.push(results[0][`${cakeKey}EditsRef`]!);
        }

        //Create MultiEdit chain

        let previousMultiEditRef: string | null = null;
        let multiEdit: MultiEdit;

        for (const editRef of editRefs) {
          multiEdit = {
            previous: previousMultiEditRef,
            edit: editRef,
            _hash: '',
          } as MultiEdit;

          const multiEditInsertTree: Record<TableKey, MultiEditsTable> = {
            [`${cakeKey}MultiEdits`]: {
              _data: [multiEdit],
              _type: 'multiEdits',
            } as MultiEditsTable,
          };

          const results = await db.insert(
            Route.fromFlat(`/${cakeKey}MultiEdits`),
            multiEditInsertTree,
            {
              skipHistory: true,
            },
          );

          previousMultiEditRef = results[0][`${cakeKey}MultiEditsRef`]!; //Update previous ref
        }

        multiEditProc = await MultiEditProcessor.fromMultiEdit(
          db,
          cakeKey,
          cakeRef,
          multiEdit!,
        );
      });

      it('should process a MultiEdit with multiple Edits', async () => {
        const result = multiEditProc.join.rows;

        expect(result.length).toBeGreaterThan(0);

        //Check sorted order by brand
        expect(result.flatMap((r) => r[0])).toEqual([
          'Audi',
          'Audi',
          'BMW',
          'Tesla',
          'Tesla',
        ]);

        //Check filtered values
        // isElectric == true
        expect(
          result.flatMap((r) => r[3]).every((isElectric) => isElectric == true),
        ).toBe(true);

        // length > 4000
        const lengths = result.flatMap((r) => r[4]);

        expect(lengths.every((length) => length > 4000)).toBe(true);

        //Check set values
        expect(
          result
            .map((r) => r[2])
            .every((serviceIntervals) =>
              equals([15000, 30000, 45000, 60000], serviceIntervals),
            ),
        ).toBe(true);
      });

      it('should insert resulting Join into Db', async () => {
        const join = multiEditProc.join;
        const inserts = join.insert();

        expect(inserts.length).toBe(1);

        const insertResults: InsertHistoryRow<any>[] = [];
        for (const insert of inserts) {
          insertResults.push(
            ...(await db.insert(insert.route, insert.tree, {
              skipHistory: true,
            })),
          );
        }

        expect(insertResults).toBeDefined();
        expect(insertResults.length).toBe(1);

        const writtenCakeRef = insertResults[0][`${cakeKey}Ref`] as string;
        const {
          rljson: {
            carGeneral: { _data: writtenCarGeneral },
          },
        } = await db.get(
          Route.fromFlat(
            `/${cakeKey}@${writtenCakeRef}/carGeneralLayer/carGeneral/serviceIntervals`,
          ),
          {},
        );

        expect(writtenCarGeneral.length).toBe(5);
      });
    });
  });

  describe('PutComponent acceptance (round-trip)', () => {
    it(
      'publishes via MultiEditManager, reconstructs via ' +
        'applyEditHistory, and the component round-trips with an ' +
        'identical hash',
      async () => {
        const manager = new MultiEditManager(cakeKey, db);

        // Edit #1: an ordinary selection edit, purely to give the
        // second edit's EditHistory a `previous` to chain onto -- so
        // resolving it exercises MultiEditProcessor.applyEditHistory()'s
        // incremental ("has previous") path below, not just a
        // from-scratch replay.
        const selectionEdit: Edit = hip<Edit>({
          name: 'Select some columns',
          action: exampleEditActionColumnSelection(),
          _hash: '',
        });
        await manager.edit(selectionEdit, cakeRef);
        const firstEditHistoryRef = manager.head!.editHistoryRef;

        // Edit #2: the putComponent edit -- a representative "document"
        // (nested object + array fields), carried whole in one edit.
        const editActionPutComponent = exampleEditActionPutComponent();
        const putComponentEdit: Edit = hip<Edit>({
          name: editActionPutComponent.name,
          action: editActionPutComponent,
          _hash: '',
        });

        await manager.edit(putComponentEdit);
        const secondEditHistoryRef = manager.head!.editHistoryRef;
        expect(secondEditHistoryRef).not.toBe(firstEditHistoryRef);

        const published = await manager.publish();
        const writtenCakeRef = published.cakeRef;

        // Reconstruct from scratch: a FRESH manager (empty processor
        // cache) rebuilds the head purely from persisted
        // Edit/MultiEdit/EditHistory rows. secondEditHistoryRef.previous
        // = [firstEditHistoryRef], so MultiEditManager.editHistoryRef()
        // recurses through MultiEditProcessor.applyEditHistory().
        const freshManager = new MultiEditManager(cakeKey, db);
        const reconstructed = await freshManager.editHistoryRef(
          secondEditHistoryRef,
        );

        const inserts = reconstructed.join.insert();
        expect(inserts.length).toBe(1);

        const { tree } = inserts[0];
        const reconstructedLayerRow = (tree as any).carCake._data[0].layers
          .carGeneralLayer._data[0];
        // Compare content only, same reasoning as the other PutComponent
        // cases: the Edit row's own insert already computed real hashes.
        expect(
          rmhsh(
            reconstructedLayerRow.add[editActionPutComponent.data.sliceId]
              .carGeneral._data[0],
          ),
        ).toEqual(rmhsh(editActionPutComponent.data.component));

        // The published Db state carries the SAME content-addressed
        // component: re-hashing an untouched copy of the original
        // object client-side yields the same hash as the row that
        // actually landed in carGeneral.
        const expectedHash = hip(
          JSON.parse(JSON.stringify(editActionPutComponent.data.component)),
        )._hash as string;

        const dump = await db.core.dumpTable('carGeneral');
        const stored = (dump.carGeneral._data as any[]).find(
          (c) => c._hash === expectedHash,
        );

        expect(stored).toBeDefined();
        expect(stored._hash).toBe(expectedHash);
        expect(stored.brand).toBe(editActionPutComponent.data.component.brand);
        expect(stored.serviceIntervals).toEqual(
          editActionPutComponent.data.component.serviceIntervals,
        );
        expect(stored.units.energy).toEqual(
          editActionPutComponent.data.component.units.energy,
        );
        expect(stored.meta.pressText).toEqual(
          editActionPutComponent.data.component.meta.pressText,
        );

        // And the NEW document is fully readable through BOTH consumer
        // read paths on the published cake:
        const putSliceId = editActionPutComponent.data.sliceId; // VIN99

        // (a) sliceId-filtered db.get.
        const { cell: brandCells } = await db.get(
          Route.fromFlat(
            `/${cakeKey}@${writtenCakeRef}/carGeneralLayer/carGeneral/brand`,
          ),
          {},
          undefined,
          [putSliceId],
        );
        expect(brandCells.length).toBe(1);
        expect(brandCells[0].value).toBe(
          editActionPutComponent.data.component.brand,
        );

        // (b) the sliceId-driven db.join -- the exact engine
        // applyEditHistory rebuilds joins with. Without the sliceIds
        // maintenance this MISSES the new document entirely; with it,
        // VIN99 is present (10 existing + 1 new) carrying its component.
        const reconSelection = new ColumnSelection([
          {
            key: 'brand',
            route: `${cakeKey}/carGeneralLayer/carGeneral/brand`,
            alias: 'brand',
            titleLong: '',
            titleShort: '',
            type: 'jsonValue',
            _hash: '',
          },
        ]);
        const publishedJoin = await db.join(
          reconSelection,
          cakeKey,
          writtenCakeRef,
        );
        expect(publishedJoin.rowIndices).toContain(putSliceId);
        // cakeRef here is carCake._data[0] (8 base slices VIN1..VIN8),
        // so the new document brings the total to 9.
        expect(publishedJoin.rowIndices.length).toBe(9);
        expect(publishedJoin.row(putSliceId)[0].value.cell[0].value).toBe(
          editActionPutComponent.data.component.brand,
        );
      },
    );
  });
});
