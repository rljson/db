// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { IoMem } from '@rljson/io';
import { equals } from '@rljson/json';
import {
  createEditHistoryTableCfg,
  createEditTableCfg,
  createMultiEditTableCfg,
  Edit,
  Insert,
  InsertHistoryRow,
  MultiEdit,
  Route,
} from '@rljson/rljson';

import { beforeEach, describe, expect, it } from 'vitest';

import { Db } from '../../src/db';
import { exampleEditColumnSelection } from '../../src/edit/edit';
import {
  exampleEditActionColumnSelection,
  exampleEditActionColumnSelectionOnlySomeColumns,
  exampleEditActionRowFilter,
  exampleEditActionRowSort,
  exampleEditActionSetValue,
  exampleEditSetValueReferenced,
} from '../../src/edit/edit-action';
import { MultiEditProcessor } from '../../src/edit/multi-edit-processor';
import { staticExample } from '../../src/example-static/example-static';

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
      const editColumnSelection: Edit = {
        name: 'Test Edit',
        action: editActionColumnSelection,
        _hash: '',
      } as Edit;

      const editInsert: Insert<Edit> = {
        route: `${cakeKey}Edits`,
        value: editColumnSelection,
        command: 'add',
      };

      const [{ [`${cakeKey}EditsRef`]: editRef }] = await db.insert(
        editInsert,
        {
          skipHistory: true,
        },
      );

      const multiEdit: MultiEdit = {
        previous: null,
        edit: editRef!,
        _hash: '',
      } as MultiEdit;

      const proc = await MultiEditProcessor.fromModel(
        db,
        cakeKey,
        cakeRef,
        multiEdit,
      );

      expect(proc).toBeDefined();
      expect(proc.join).toBeDefined();
    });
  });

  describe('fromModel', async () => {
    describe('Single Edit', async () => {
      it('ColumnSelection', async () => {
        const editActionColumnSelection = exampleEditActionColumnSelection();
        const editColumnSelection: Edit = {
          name: 'Select: brand, type, serviceIntervals, isElectric, height, width, length, engine, repairedByWorkshop',
          action: editActionColumnSelection,
          _hash: '',
        } as Edit;

        const editInsert: Insert<Edit> = {
          route: `${cakeKey}Edits`,
          value: editColumnSelection,
          command: 'add',
        };

        const [{ [`${cakeKey}EditsRef`]: editRef }] = await db.insert(
          editInsert,
          {
            skipHistory: true,
          },
        );

        const multiEdit = {
          previous: null,
          edit: editRef!,
          _hash: '',
        } as MultiEdit;

        const proc = await MultiEditProcessor.fromModel(
          db,
          cakeKey,
          cakeRef,
          multiEdit,
        );

        expect(proc.join.rows.length).toBeGreaterThan(0);
      });

      it('RowFilter', async () => {
        const editActionRowFilter = exampleEditActionRowFilter();

        const editRowFilter: Edit = {
          name: 'Filter: isElectric = true, length > 4000',
          action: editActionRowFilter,
          _hash: '',
        } as Edit;

        const editInsert: Insert<Edit> = {
          route: `${cakeKey}Edits`,
          value: editRowFilter,
          command: 'add',
        };

        const [{ [`${cakeKey}EditsRef`]: editRef }] = await db.insert(
          editInsert,
          {
            skipHistory: true,
          },
        );

        const multiEdit = {
          previous: null,
          edit: editRef!,
          _hash: '',
        } as MultiEdit;

        const proc = await MultiEditProcessor.fromModel(
          db,
          cakeKey,
          cakeRef,
          multiEdit,
        );

        expect(proc.join.rows.length).toBeGreaterThan(0);
      });

      it('RowSort', async () => {
        const editActionRowSort = exampleEditActionRowSort();

        const editRowSort: Edit = {
          name: 'Sort: brand ASC',
          action: editActionRowSort,
          _hash: '',
        } as Edit;

        const editInsert: Insert<Edit> = {
          route: `${cakeKey}Edits`,
          value: editRowSort,
          command: 'add',
        };

        const [{ [`${cakeKey}EditsRef`]: editRef }] = await db.insert(
          editInsert,
          {
            skipHistory: true,
          },
        );

        const multiEdit = {
          previous: null,
          edit: editRef!,
          _hash: '',
        } as MultiEdit;

        const proc = await MultiEditProcessor.fromModel(
          db,
          cakeKey,
          cakeRef,
          multiEdit,
        );

        expect(proc.join.rows.length).toBeGreaterThan(0);
      });

      it('SetValue', async () => {
        const editActionSetValue = exampleEditActionSetValue();

        const editSetValue: Edit = {
          name: 'Set: serviceIntervals = [15000,30000,45000,60000]',
          action: editActionSetValue,
          _hash: '',
        } as Edit;

        const editInsert: Insert<Edit> = {
          route: `${cakeKey}Edits`,
          value: editSetValue,
          command: 'add',
        };

        const [{ [`${cakeKey}EditsRef`]: editRef }] = await db.insert(
          editInsert,
          {
            skipHistory: true,
          },
        );

        const multiEdit = {
          previous: null,
          edit: editRef!,
          _hash: '',
        } as MultiEdit;

        const proc = await MultiEditProcessor.fromModel(
          db,
          cakeKey,
          cakeRef,
          multiEdit,
        );

        expect(proc.join.rows.length).toBeGreaterThan(0);
      });

      it('SetValue Referenced & Insert', async () => {
        const editSetValueReferenced: Edit = {
          name: 'Set: length = 4800',
          action: exampleEditSetValueReferenced(),
          _hash: '',
        } as Edit;

        const editInsert: Insert<Edit> = {
          route: `${cakeKey}Edits`,
          value: editSetValueReferenced,
          command: 'add',
        };

        const [{ [`${cakeKey}EditsRef`]: editRef }] = await db.insert(
          editInsert,
          {
            skipHistory: true,
          },
        );

        const multiEdit = {
          previous: null,
          edit: editRef!,
          _hash: '',
        } as MultiEdit;

        const proc = await MultiEditProcessor.fromModel(
          db,
          cakeKey,
          cakeRef,
          multiEdit,
        );

        expect(proc.join.rows.every((c) => equals(c, [4800]))).toBe(true);

        const inserts = proc.join.insert();

        const insertResults: InsertHistoryRow<any>[] = [];
        for (const insert of inserts) {
          insertResults.push(...(await db.insert(insert)));
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

        const editInserts: Insert<Edit>[] = [
          {
            route: `${cakeKey}Edits`,
            value: editColumnSelection,
            command: 'add',
          },
          {
            route: `${cakeKey}Edits`,
            value: editRowFilter,
            command: 'add',
          },
          {
            route: `${cakeKey}Edits`,
            value: editRowSort,
            command: 'add',
          },
          {
            route: `${cakeKey}Edits`,
            value: editSetValue,
            command: 'add',
          },
          {
            route: `${cakeKey}Edits`,
            value: editColumnSelectionSomeColumns,
            command: 'add',
          },
        ];

        const editRefs: string[] = [];

        for (const insert of editInserts) {
          //Insert Edit
          const results = await db.insert(insert, {
            skipHistory: true,
          });
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

          const multiEditInsert: Insert<MultiEdit> = {
            route: `${cakeKey}MultiEdits`,
            value: multiEdit,
            command: 'add',
          };

          const results = await db.insert(multiEditInsert, {
            skipHistory: true,
          });

          previousMultiEditRef = results[0][`${cakeKey}MultiEditsRef`]!; //Update previous ref
        }

        multiEditProc = await MultiEditProcessor.fromModel(
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
        expect(result.map((r) => r[0])).toEqual([
          'Audi',
          'Audi',
          'BMW',
          'Tesla',
          'Tesla',
        ]);

        //Check filtered values
        // isElectric == true
        expect(
          result.map((r) => r[3]).every((isElectric) => isElectric == true),
        ).toBe(true);

        // length > 4000
        const lengths = result
          .map((r) => r[4])
          .flatMap((l: { ref: string; value: number }[]) =>
            l.map((li) => (li as any)._value),
          );

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
          insertResults.push(...(await db.insert(insert)));
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

        expect(writtenCarGeneral.length).toBe(join.rows.length);
      });
    });
  });
});
