// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';
import { IoMem } from '@rljson/io';
import {
  createEditHistoryTableCfg,
  createEditTableCfg,
  createMultiEditTableCfg,
  Edit,
  EditHistory,
  MultiEdit,
  Route,
  timeId,
} from '@rljson/rljson';

import { beforeEach, describe, expect, it, vi } from 'vitest';

import { Db } from '../../src/db';
import {
  exampleEditActionColumnSelection,
  exampleEditActionSetValue,
} from '../../src/edit/edit-action';
import { MultiEditManager } from '../../src/edit/multi-edit-manager';
import { staticExample } from '../../src/example-static/example-static';

describe('MultiEditManager', () => {
  let db: Db;
  let multiEditManager: MultiEditManager;

  let editHistory: EditHistory;
  let edit: Edit;
  let multiEdit: MultiEdit;

  const cakeKey = 'carCake';
  const cakeRef = staticExample().carCake._data[2]._hash as string;

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

    //Instantiate MultiEditManager
    multiEditManager = new MultiEditManager(cakeKey, db);

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
  });

  describe('Init', () => {
    it('should be defined', async () => {
      multiEditManager.init();
      expect(multiEditManager).toBeDefined();
    });
    it('should register an observer on EditHistory', async () => {
      multiEditManager.init();

      // Add EditHistory to trigger observer
      const editHistoryRef = await vi.waitFor(async () => {
        const { [cakeKey + 'EditHistoryRef']: ref } = (
          await db.addEditHistory(cakeKey, editHistory)
        )[0] as any;
        return ref as string;
      });

      const processorAdded = await vi.waitUntil(
        () => {
          return multiEditManager.processors.has(editHistoryRef);
        },
        { timeout: 2000, interval: 100 },
      );

      expect(processorAdded).toBe(true);
    });
  });

  describe('tearDown', () => {
    it('should be defined', async () => {
      multiEditManager.init();
      multiEditManager.tearDown();

      expect(multiEditManager).toBeDefined();
      expect(multiEditManager.isListening).toBe(false);
    });
  });

  describe('Edit', () => {
    let initialEditHistoryRef: string;
    beforeEach(async () => {
      multiEditManager.init();

      // Add initial EditHistory to set head
      const { [cakeKey + 'EditHistoryRef']: initialEditHistoryHash } = (
        await db.addEditHistory(cakeKey, editHistory)
      )[0] as any;
      initialEditHistoryRef = initialEditHistoryHash as string;

      // Wait for the processor to be added
      await vi.waitUntil(
        () => {
          return multiEditManager.processors.has(
            initialEditHistoryRef as string,
          );
        },
        { timeout: 2000, interval: 100 },
      );
    });

    it('should throw an error if no head MultiEditProcessor is available', async () => {
      await expect(
        new MultiEditManager(cakeKey, db).edit(
          hip<Edit>({
            name: 'Test Edit',
            action: exampleEditActionColumnSelection(),
            _hash: '',
          }),
        ),
      ).rejects.toThrowError('No head MultiEditProcessor available.');
    });

    it('should perform an edit and update the head MultiEditProcessor', async () => {
      const callback = vi.fn();
      multiEditManager.listenToHeadChanges(callback);

      const newEdit = hip<Edit>({
        name: 'Another Test Edit',
        action: exampleEditActionColumnSelection(),
        _hash: '',
      });

      // Perform the edit
      await multiEditManager.edit(newEdit);

      expect(multiEditManager.head).toBeDefined();
      expect(multiEditManager.head?.editHistoryRef).not.toBe(
        initialEditHistoryRef,
      );

      // Verify that the new edit is stored in the database
      const { cell: newEditAdded } = await db.get(
        Route.fromFlat(`${cakeKey}Edits`),
        newEdit._hash,
      );

      expect(newEditAdded).toBeDefined();
      expect(newEditAdded.length).toBe(1);
      expect(newEditAdded[0].row as Edit).toEqual(newEdit);

      // Verify that the callback was called with the new head editHistoryRef
      expect(callback).toHaveBeenCalled();
      expect(callback).toHaveBeenCalledWith(
        multiEditManager.head?.editHistoryRef,
      );
    });

    it('should perform an edit even if head is not provided', async () => {
      const newMultiEditManager = new MultiEditManager(cakeKey, db);
      newMultiEditManager.init();

      const callback = vi.fn();
      newMultiEditManager.listenToHeadChanges(callback);

      const newEdit = hip<Edit>({
        name: 'Another Test Edit',
        action: exampleEditActionColumnSelection(),
        _hash: '',
      });

      const { [cakeKey + 'EditsRef']: newEditRef } = (
        await db.addEdit(cakeKey, newEdit)
      )[0] as any;

      // Verify that the new edit is stored in the database
      const { cell: newEditAdded } = await db.get(
        Route.fromFlat(`${cakeKey}Edits`),
        newEditRef,
      );

      expect(newEditAdded).toBeDefined();
      expect(newEditAdded.length).toBe(1);

      // Perform the edit
      await newMultiEditManager.edit(newEdit, cakeRef);

      expect(newMultiEditManager.head).toBeDefined();
      expect(newMultiEditManager.head?.editHistoryRef).not.toBe(
        initialEditHistoryRef,
      );

      expect(newEditAdded).toBeDefined();
      expect(newEditAdded.length).toBe(1);
      expect(newEditAdded[0].row as Edit).toEqual(newEdit);

      // Verify that the callback was called with the new head editHistoryRef
      expect(callback).toHaveBeenCalled();
      expect(callback).toHaveBeenCalledWith(
        newMultiEditManager.head?.editHistoryRef,
      );
    });
  });

  describe('Publish', () => {
    beforeEach(async () => {
      multiEditManager.init();

      // Add initial EditHistory to set head
      const { [cakeKey + 'EditHistoryRef']: initialEditHistoryHash } = (
        await db.addEditHistory(cakeKey, editHistory)
      )[0] as any;

      // Wait for the processor to be added
      await vi.waitUntil(
        () => {
          return multiEditManager.processors.has(
            initialEditHistoryHash as string,
          );
        },
        { timeout: 2000, interval: 100 },
      );
    });

    it('should throw an error if no head MultiEditProcessor is available', async () => {
      await expect(
        new MultiEditManager(cakeKey, db).publish(),
      ).rejects.toThrowError('No head MultiEditProcessor available.');
    });

    it('should publish the current head MultiEditProcessor', async () => {
      const setValueEdit = hip<Edit>({
        name: 'Set: Service Intervals to [15000, 30000, 45000, 60000]',
        action: exampleEditActionSetValue(),
        _hash: '',
      });

      await multiEditManager.edit(setValueEdit);

      expect(multiEditManager.head).toBeDefined();
      expect(multiEditManager.processors.size).toBe(2);

      const publishResult = await multiEditManager.publish();
      const writtenCakeRef = publishResult.cakeRef;

      expect(writtenCakeRef).toBeDefined();

      // Verify that the data has been written correctly
      const { cell: publishedCake } = await db.get(
        Route.fromFlat(cakeKey),
        writtenCakeRef,
      );

      expect(publishedCake).toBeDefined();
      expect(publishedCake.length).toBe(1);

      const { cell: publishedCars } = await db.get(
        Route.fromFlat(
          `${cakeKey}@${writtenCakeRef}/carGeneralLayer/carGeneral/serviceIntervals`,
        ),
        {},
      );

      expect(publishedCars).toBeDefined();
      expect(publishedCars.length).toBe(12);
      expect(
        publishedCars
          .map((c) => c.value)
          .every(
            (si) =>
              JSON.stringify(si) ===
              JSON.stringify([15000, 30000, 45000, 60000]),
          ),
      ).toBe(true);
    });
  });

  describe('Join', () => {
    beforeEach(async () => {
      multiEditManager.init();

      // Add initial EditHistory to set head
      const { [cakeKey + 'EditHistoryRef']: initialEditHistoryHash } = (
        await db.addEditHistory(cakeKey, editHistory)
      )[0] as any;

      // Wait for the processor to be added
      await vi.waitUntil(
        () => {
          return multiEditManager.processors.has(
            initialEditHistoryHash as string,
          );
        },
        { timeout: 2000, interval: 100 },
      );
    });

    it('should throw an error if no head MultiEditProcessor is available', async () => {
      expect(() => new MultiEditManager(cakeKey, db).join).toThrowError(
        'No head MultiEditProcessor available.',
      );
    });

    it('should return a join w/ resulting data', async () => {
      const setValueEdit = hip<Edit>({
        name: 'Set: Service Intervals to [15000, 30000, 45000, 60000]',
        action: exampleEditActionSetValue(),
        _hash: '',
      });

      await multiEditManager.edit(setValueEdit);

      const join = await multiEditManager.join;

      expect(join).toBeDefined();
      expect(join.rows).toBeDefined();
      expect(join.rows.length).toBe(12);
      expect(
        join.rows
          .map((r) => r[2]) // serviceIntervals column
          .every(
            (si) =>
              JSON.stringify(si) ===
              JSON.stringify([15000, 30000, 45000, 60000]),
          ),
      ).toBe(true);
    });
  });
});
