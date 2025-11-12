// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { rmhsh } from '@rljson/hash';
import { Json } from '@rljson/json';
import { Route } from '@rljson/rljson';

import { Db } from '../db.ts';
import { Join, JoinRowsHashed } from '../join/join.ts';
import {
  ColumnInfo,
  ColumnSelection,
} from '../join/selection/column-selection.ts';
import { RowSort } from '../join/sort/row-sort.ts';

import {
  EditActionColumnSelection,
  EditActionRowFilter,
  EditActionRowSort,
  EditActionSetValue,
} from './edit-action.ts';
import { Edit } from './edit.ts';
import { MultiEdit } from './multi-edit.ts';

export type MultiEditColumnSelection = ColumnSelection;
export type MultiEditRowHashed = JoinRowsHashed;
export type MultiEditRows = any[][];

export class MultiEditProcessor {
  private _edits: Edit[] = [];
  private _join: Join | null = null;

  constructor(
    private readonly db: Db,
    private readonly cakeKey: string,
    private readonly cakeRef: string,
  ) {
    // Initialization code can go here
  }

  static async fromModel(
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

  private async _resolve(multiEdit: MultiEdit): Promise<void> {
    const edit = await this.edit(multiEdit.edit);
    this._edits.push(edit);

    if (multiEdit.previous) {
      const previousMultiEdit = await this.multiEdit(multiEdit.previous!);
      return this._resolve(previousMultiEdit);
    }
  }

  private async _processAll(): Promise<Join> {
    for (let i = this._edits.length - 1; i >= 0; i--) {
      const edit = this._edits[i];
      this._join = await this._process(edit);
    }

    return this._join!;
  }

  private async _process(edit: Edit): Promise<Join> {
    const action = edit.action;
    if (!this._join) {
      switch (action.type) {
        case 'selection':
          const editColInfos =
            action.data as EditActionColumnSelection as ColumnInfo[];
          const editColSelection = new ColumnSelection(editColInfos);
          this._join = await this.db.join(
            editColSelection,
            this.cakeKey,
            this.cakeRef,
          );
          break;
        case 'setValue':
          const editSetValue = action.data as EditActionSetValue;
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
            await this.db.join(
              editSetValueColSelection,
              this.cakeKey,
              this.cakeRef,
            )
          ).setValue(editSetValue);
          break;
        case 'sort':
          const editRowSort = action.data as EditActionRowSort;
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
            await this.db.join(
              editRowSortColSelection,
              this.cakeKey,
              this.cakeRef,
            )
          ).sort(new RowSort(editRowSort));
          break;
        case 'filter':
          const editRowFilter = action.data as EditActionRowFilter;
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
            await this.db.join(
              editRowFilterColSelection,
              this.cakeKey,
              this.cakeRef,
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
          const editColInfos =
            action.data as EditActionColumnSelection as ColumnInfo[];
          const editColSelection = new ColumnSelection(editColInfos);
          this._join = this._join.select(editColSelection);
          break;
        case 'setValue':
          const editSetValue = rmhsh(action.data as Json) as EditActionSetValue;
          this._join = this._join.setValue(editSetValue);
          break;
        case 'sort':
          const editRowSort = rmhsh(action.data as Json) as EditActionRowSort;
          this._join = this._join.sort(new RowSort(editRowSort));
          break;
        case 'filter':
          const editRowFilter = rmhsh(
            action.data as Json,
          ) as EditActionRowFilter;
          this._join = this._join.filter(editRowFilter);
          break;
        /* v8 ignore next -- @preserve */
        default:
          break;
      }
    }

    return this._join!;
  }

  private async multiEdit(multiEditRef: string): Promise<MultiEdit> {
    const multiEditTableKey = `${this.cakeKey}MultiEdits`;
    const {
      [multiEditTableKey]: { _data: multiEdit },
    } = await this.db.core.readRows(multiEditTableKey, { _hash: multiEditRef });

    return multiEdit[0] as MultiEdit;
  }

  private async edit(editRef: string): Promise<Edit> {
    const editTableKey = `${this.cakeKey}Edits`;
    const {
      [editTableKey]: { _data: edits },
    } = await this.db.core.readRows(editTableKey, { _hash: editRef });

    return rmhsh(edits[0]) as Edit;
  }
}
