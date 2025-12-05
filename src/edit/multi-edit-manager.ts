// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import {
  Edit,
  EditHistory,
  InsertHistoryRow,
  Route,
  timeId,
} from '@rljson/rljson';

import { Db } from '../db.ts';

import { MultiEditProcessor } from './multi-edit-processor.ts';

export class MultiEditManager {
  _head: { editHistoryRef: string; processor: MultiEditProcessor } | null =
    null;
  _headListener: ((editHistoryRef: string) => Promise<void>)[] = [];
  _processors: Map<string, MultiEditProcessor> = new Map();

  constructor(private readonly _cakeKey: string, private readonly _db: Db) {}

  init() {
    const editHistoryKey = `${this._cakeKey}EditHistory`;
    this._db.registerObserver(
      Route.fromFlat(editHistoryKey),
      (ins: InsertHistoryRow<string>) => {
        const editHistoryRef = (ins as any)[editHistoryKey + 'Ref'] as string;
        return this._addProcessorFromEditHistoryRef(editHistoryRef);
      },
    );
  }

  async edit(edit: Edit) {
    if (!this.head) {
      throw new Error('No head MultiEditProcessor available.');
    }

    // Create new MultiEditProcessor by applying the edit to the current head
    const multiEditProc = await this.head.processor.edit(edit);

    // Store the new Edit
    const { [this._cakeKey + 'EditsRef']: editRef } = (
      await this._db.addEdit(this._cakeKey, edit)
    )[0] as any;
    /* v8 ignore next -- @preserve */
    if (!editRef) {
      throw new Error('MultiEditManager: Failed to create EditRef.');
    }

    // Create and store the new MultiEdit and EditHistory
    const { [this._cakeKey + 'MultiEditsRef']: multiEditRef } = (
      await this._db.addMultiEdit(this._cakeKey, multiEditProc.multiEdit)
    )[0] as any;
    /* v8 ignore next -- @preserve */
    if (!multiEditRef) {
      throw new Error('MultiEditManager: Failed to create MultiEditRef.');
    }

    // Create and store the new EditHistory pointing to the new MultiEdit
    const { [this._cakeKey + 'EditHistoryRef']: editHistoryRef } = (
      await this._db.addEditHistory(this._cakeKey, {
        _hash: '',
        dataRef: multiEditProc.cakeRef,
        multiEditRef: multiEditRef,
        timeId: timeId(),
        previous: [this.head.editHistoryRef],
      } as EditHistory)
    )[0] as any;
    /* v8 ignore next -- @preserve */
    if (!editHistoryRef) {
      throw new Error('MultiEditManager: Failed to create EditHistoryRef.');
    }

    // Update internal state
    this._processors.set(editHistoryRef, multiEditProc);
    this._head = {
      editHistoryRef,
      processor: multiEditProc,
    };

    // Notify listeners about head change
    this._notifyHeadListener(editHistoryRef);
  }

  async publish() {
    if (!this.head) {
      throw new Error('No head MultiEditProcessor available.');
    }
    return await this.head.processor.publish();
  }

  listenToHeadChanges(callback: (editHistoryRef: string) => Promise<void>) {
    this._headListener.push(callback);
  }

  private _notifyHeadListener(editHistoryRef: string) {
    /* v8 ignore next -- @preserve */
    Promise.all(this._headListener.map((cb) => cb(editHistoryRef))).catch(
      (err) => {
        console.error(
          `Error notifying head observers for editHistoryRef ${editHistoryRef}:`,
          err,
        );
      },
    );
  }

  private _addProcessorFromEditHistoryRef(
    editHistoryRef: string,
  ): Promise<void> {
    return new Promise((resolve, reject) => {
      this._db
        .getEditHistories(this._cakeKey, editHistoryRef)
        .then((editHistories) => {
          /* v8 ignore if -- @preserve */
          if (editHistories.length === 0) {
            reject(
              new Error(`EditHistory with ref ${editHistoryRef} not found.`),
            );
          }
          /* v8 ignore if -- @preserve */
          if (editHistories.length > 1) {
            reject(
              new Error(
                `Multiple EditHistories with ref ${editHistoryRef} found.`,
              ),
            );
          }

          const editHistory = editHistories[0];

          /* v8 ignore next -- @preserve */
          MultiEditProcessor.fromEditHistory(
            this._db,
            this._cakeKey,
            editHistory,
          )
            .then((processor) => {
              this._processors.set(editHistoryRef, processor);
              this._head = {
                editHistoryRef,
                processor,
              };
              this._notifyHeadListener(editHistoryRef);
              resolve();
            })
            .catch((err) => {
              reject(err);
            });
        });
    });
  }

  get processors() {
    return this._processors;
  }

  get head() {
    return this._head;
  }

  get join() {
    if (!this.head) {
      throw new Error('No head MultiEditProcessor available.');
    }
    return this.head.processor.join;
  }
}
