// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip, rmhsh } from '@rljson/hash';
import { Edit, EditHistory, MultiEdit, Route } from '@rljson/rljson';

import { Db } from '../db.ts';
import { RowFilter } from '../join/filter/row-filter.ts';
import { Join, JoinRowsHashed } from '../join/join.ts';
import {
  ColumnInfo,
  ColumnSelection,
} from '../join/selection/column-selection.ts';
import { SetValue } from '../join/set-value/set-value.ts';
import { RowSort, RowSortType } from '../join/sort/row-sort.ts';

import {
  EditColumnSelection,
  EditRowFilter,
  EditRowSort,
  EditSetValue,
} from './edit.ts';

export type MultiEditColumnSelection = ColumnSelection;
export type MultiEditRowHashed = JoinRowsHashed;
export type MultiEditRows = any[][];

export class MultiEditProcessor {
  private _multiEdit: MultiEdit | null = null;
  private _edits: Edit[] = [];
  private _join: Join | null = null;

  constructor(
    private readonly _db: Db,
    private readonly _cakeKey: string,
    private readonly _cakeRef: string,
  ) {
    // Initialization code can go here
  }

  //...........................................................................
  /**
   * Create MultiEditProcessor from EditHistory
   * @param db - Db instance
   * @param cakeKey - Cake key
   * @param editHistory - EditHistory
   * @returns MultiEditProcessor
   */
  static async fromEditHistory(
    db: Db,
    cakeKey: string,
    editHistory: EditHistory,
  ): Promise<MultiEditProcessor> {
    /* v8 ignore if -- @preserve */
    if (!editHistory || !editHistory.multiEditRef) {
      throw new Error('MultiEditProcessor: Invalid EditHistory provided.');
    }

    const cakeRef = editHistory.dataRef;
    const multiEdits = await db.getMultiEdits(
      cakeKey,
      editHistory.multiEditRef,
    );

    /* v8 ignore if -- @preserve */
    if (!multiEdits || multiEdits.length === 0) {
      throw new Error(
        `MultiEditProcessor: MultiEdit not found for ref ${editHistory.multiEditRef}`,
      );
    }

    /* v8 ignore if -- @preserve */
    if (multiEdits.length > 1) {
      throw new Error(
        `MultiEditProcessor: Multiple MultiEdits found for ref ${editHistory.multiEditRef}`,
      );
    }

    const multiEdit = multiEdits[0];

    return MultiEditProcessor.fromMultiEdit(db, cakeKey, cakeRef, multiEdit);
  }

  //...........................................................................
  /**
   * Create MultiEditProcessor from MultiEdit
   * @param db - Db instance
   * @param cakeKey - Cake key
   * @param cakeRef - Cake ref
   * @param multiEdit - MultiEdit
   * @returns MultiEditProcessor
   */
  static async fromMultiEdit(
    db: Db,
    cakeKey: string,
    cakeRef: string,
    multiEdit: MultiEdit,
  ): Promise<MultiEditProcessor> {
    const processor = new MultiEditProcessor(db, cakeKey, cakeRef);
    await processor._resolve(multiEdit);
    await processor._processAll();
    return processor;
  }

  get join(): Join {
    /* v8 ignore if -- @preserve */
    if (!this._join) {
      throw new Error('MultiEditProcessor: Join not processed yet.');
    }
    return this._join;
  }

  get multiEdit(): MultiEdit {
    /* v8 ignore if -- @preserve */
    if (!this._multiEdit) {
      throw new Error('MultiEditProcessor: MultiEdit not resolved yet.');
    }
    return this._multiEdit;
  }

  get cakeRef(): string {
    return this._cakeRef;
  }

  //...........................................................................
  /**
   * Apply an Edit to the MultiEditProcessor
   * @param edit - Edit to apply
   * @returns MultiEditProcessor
   */
  async edit(edit: Edit): Promise<MultiEditProcessor> {
    this._edits.push(edit);
    this._join = await this._process(edit);
    /* v8 ignore next -- @preserve */
    this._multiEdit = hip<MultiEdit>({
      _hash: '',
      edit: edit._hash,
      previous: this.multiEdit ? this.multiEdit._hash : null,
    });
    return this;
  }

  async applyEditHistory(
    editHistory: EditHistory,
  ): Promise<MultiEditProcessor> {
    const multiEdits = await this._db.getMultiEdits(
      this._cakeKey,
      editHistory.multiEditRef,
    );

    /* v8 ignore if -- @preserve */
    if (!multiEdits || multiEdits.length === 0) {
      throw new Error(
        `MultiEditProcessor: MultiEdit not found for ref ${editHistory.multiEditRef}`,
      );
    }

    /* v8 ignore if -- @preserve */
    if (multiEdits.length > 1) {
      throw new Error(
        `MultiEditProcessor: Multiple MultiEdits found for ref ${editHistory.multiEditRef}`,
      );
    }

    const multiEdit = multiEdits[0];

    await this._resolve(multiEdit);
    await this._processAll();

    return this;
  }

  //...........................................................................
  /**
   * Publish the MultiEditProcessor. Inserts the resulting Join as new data,
   * updates the head revision, and saves the resulting MultiEdit.
   * @param options - Publish options
   * @returns MultiEditProcessor
   */
  async publish(options?: {
    skipHeadUpdate?: boolean;
    skipSaveMultiEdit?: boolean;
  }): Promise<MultiEditProcessor> {
    const inserts = this.join.insert();

    /* v8 ignore if -- @preserve */
    if (inserts.length === 0) {
      throw new Error('MultiEditProcessor: No inserts to publish.');
    }

    /* v8 ignore if -- @preserve */
    if (inserts.length > 1) {
      throw new Error(
        'MultiEditProcessor: Multiple inserts not supported yet.',
      );
    }

    const insert = inserts[0];
    const inserteds = await this._db.insert(insert.route, insert.tree);

    /* v8 ignore if -- @preserve */
    if (inserteds.length === 0) {
      throw new Error('MultiEditProcessor: No rows inserted.');
    }
    /* v8 ignore if -- @preserve */
    if (inserteds.length > 1) {
      throw new Error(
        'MultiEditProcessor: Multiple inserted rows not supported yet.',
      );
    }

    const inserted = inserteds[0];
    const writtenCakeRef = (inserted as any)[this._cakeKey + 'Ref'] as string;

    /* v8 ignore else -- @preserve */
    if (!options?.skipHeadUpdate) {
      await this._db.addHeadRevision(this._cakeKey, writtenCakeRef);
    }

    /* v8 ignore else -- @preserve */
    if (!options?.skipSaveMultiEdit) {
      await this._db.addMultiEdit(this._cakeKey, this._multiEdit!);
    }

    return new MultiEditProcessor(this._db, this._cakeKey, writtenCakeRef);
  }

  //...........................................................................
  /**
   * Clone the MultiEditProcessor
   * @returns Cloned MultiEditProcessor
   */
  clone(): MultiEditProcessor {
    const clone = new MultiEditProcessor(
      this._db,
      this._cakeKey,
      this._cakeRef,
    );
    clone._multiEdit = this._multiEdit;
    clone._edits = [...this._edits];
    clone._join = this._join;
    return clone;
  }

  //...........................................................................
  /**
   * Resolve MultiEdit chain recursively
   * @param multiEdit - MultiEdit to resolve
   * @returns Promise<void>
   */
  private async _resolve(multiEdit: MultiEdit): Promise<void> {
    this._multiEdit = multiEdit;

    const edits = await this._db.getEdits(this._cakeKey, multiEdit.edit);

    /* v8 ignore if -- @preserve */
    if (edits.length === 0) {
      throw new Error(
        `MultiEditProcessor: Edit not found for ref ${multiEdit.edit}`,
      );
    }

    /* v8 ignore if -- @preserve */
    if (edits.length > 1) {
      throw new Error(
        `MultiEditProcessor: Multiple Edits found for ref ${multiEdit.edit}`,
      );
    }

    const edit = edits[0];

    this._edits.push(edit);

    if (multiEdit.previous) {
      const previousMultiEdits = await this._db.getMultiEdits(
        this._cakeKey,
        multiEdit.previous!,
      );

      const previousMultiEdit = previousMultiEdits[0];

      return this._resolve(previousMultiEdit);
    }
  }

  //...........................................................................
  /**
   * Process all Edits in the MultiEditProcessor
   * @returns Resulting Join
   */
  private async _processAll(): Promise<Join> {
    for (let i = this._edits.length - 1; i >= 0; i--) {
      const edit = this._edits[i];
      this._join = await this._process(edit);
    }

    return this._join!;
  }

  //...........................................................................
  /**
   * Process a single Edit and update the Join
   * @param edit - Edit to process
   * @returns Resulting Join
   */
  private async _process(edit: Edit): Promise<Join> {
    const action = edit.action;
    if (!this._join) {
      switch (action.type) {
        case 'selection':
          const editColInfos = (edit as EditColumnSelection).action.data
            .columns as ColumnInfo[];
          const editColSelection = new ColumnSelection(editColInfos);
          this._join = await this._db.join(
            editColSelection,
            this._cakeKey,
            this._cakeRef,
          );
          break;
        case 'setValue':
          const editSetValue = (edit as EditSetValue).action.data as SetValue;
          const editSetValueKey = Route.fromFlat(editSetValue.route).segment()
            .tableKey;
          const editSetValueColumnInfo: ColumnInfo = {
            key: editSetValueKey,
            route: editSetValue.route,
            alias: editSetValueKey,
            titleLong: '',
            titleShort: '',
            type: 'jsonValue',
            _hash: '',
          };
          const editSetValueColSelection = new ColumnSelection([
            editSetValueColumnInfo,
          ]);
          this._join = (
            await this._db.join(
              editSetValueColSelection,
              this._cakeKey,
              this._cakeRef,
            )
          ).setValue(editSetValue);
          break;
        case 'sort':
          const editRowSort = rmhsh(edit as EditRowSort).action.data;
          const editRowSortColumnInfos: ColumnInfo[] = [];
          for (const routeStr of Object.keys(editRowSort)) {
            const route = Route.fromFlat(routeStr);
            const tableKey = route.segment().tableKey;
            const columnInfo: ColumnInfo = {
              key: tableKey,
              route: routeStr,
              alias: tableKey,
              titleLong: '',
              titleShort: '',
              type: 'jsonValue',
              _hash: '',
            };
            editRowSortColumnInfos.push(columnInfo);
          }
          const editRowSortColSelection = new ColumnSelection(
            editRowSortColumnInfos,
          );
          this._join = (
            await this._db.join(
              editRowSortColSelection,
              this._cakeKey,
              this._cakeRef,
            )
          ).sort(new RowSort(editRowSort));
          break;
        case 'filter':
          const editRowFilter = (edit as EditRowFilter).action.data;
          const editRowFilterColumnInfos: ColumnInfo[] = [];
          for (const colFilter of editRowFilter.columnFilters) {
            const route = Route.fromFlat(colFilter.column);
            const tableKey = route.segment().tableKey;
            const columnInfo: ColumnInfo = {
              key: tableKey,
              route: colFilter.column,
              alias: tableKey,
              titleLong: '',
              titleShort: '',
              type: 'jsonValue',
              _hash: '',
            };
            editRowFilterColumnInfos.push(columnInfo);
          }
          const editRowFilterColSelection = new ColumnSelection(
            editRowFilterColumnInfos,
          );
          this._join = (
            await this._db.join(
              editRowFilterColSelection,
              this._cakeKey,
              this._cakeRef,
            )
          ).filter(editRowFilter);
          break;
        /* v8 ignore next -- @preserve */
        default:
          break;
      }
    } else {
      switch (action.type) {
        case 'selection':
          const editColInfos = (edit as EditColumnSelection).action.data
            .columns as ColumnInfo[];
          const editColSelection = new ColumnSelection(editColInfos);
          this._join = this._join.select(editColSelection);
          break;
        case 'setValue':
          const editSetValue = rmhsh(
            (edit as EditSetValue).action.data as SetValue,
          );
          this._join = this._join.setValue(editSetValue);
          break;
        case 'sort':
          const editRowSort = rmhsh(
            (edit as EditRowSort).action.data,
          ) as RowSortType;
          this._join = this._join.sort(new RowSort(editRowSort));
          break;
        case 'filter':
          const editRowFilter = rmhsh(
            (edit as EditRowFilter).action.data,
          ) as RowFilter;
          this._join = this._join.filter(editRowFilter);
          break;
        /* v8 ignore next -- @preserve */
        default:
          break;
      }
    }

    return this._join!;
  }
}
