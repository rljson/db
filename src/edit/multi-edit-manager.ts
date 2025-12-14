// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import {
  Edit,
  EditHistory,
  InsertHistoryRow,
  MultiEdit,
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
        return this.editHistoryRef(editHistoryRef);
      },
    );
  }

  async edit(edit: Edit, cakeRef?: string) {
    /* v8 ignore next -- @preserve */
    if (!this.head && !cakeRef) {
      throw new Error(
        'No head MultiEditProcessor available. Provide a cakeRef.',
      );
    }
    /* v8 ignore next -- @preserve */
    if (this.head && cakeRef) {
      throw new Error(
        'Head MultiEditProcessor already exists. Do not provide a cakeRef.',
      );
    }

    // Store the new Edit
    await this._persistEdit(edit);

    let multiEditProc: MultiEditProcessor;
    if (!this.head) {
      const multiEdit: MultiEdit = {
        _hash: '',
        edit: edit._hash,
        previous: null,
      };
      multiEditProc = await MultiEditProcessor.fromMultiEdit(
        this._db,
        this._cakeKey,
        cakeRef!,
        multiEdit,
      );
    } else {
      multiEditProc = await this.head.processor.edit(edit);
    }

    // Store the new MultiEdit
    const multiEditRef = await this._persistMultiEdit(multiEditProc.multiEdit);

    // Create and store the new EditHistory pointing to the new MultiEdit
    const editHistoryRef = await this._persistEditHistory({
      _hash: '',
      dataRef: multiEditProc.cakeRef,
      multiEditRef: await multiEditRef,
      timeId: timeId(),
      previous:
        !!this.head && this.head.editHistoryRef
          ? [this.head.editHistoryRef]
          : null,
    } as EditHistory);

    // Update internal state
    this._processors.set(editHistoryRef, multiEditProc);
    this._head = {
      editHistoryRef,
      processor: multiEditProc,
    };

    // Notify listeners about head change
    await this._notifyHeadListener(editHistoryRef);
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
    return Promise.all(
      this._headListener.map((cb) => cb(editHistoryRef)),
    ).catch((err) => {
      console.error(
        `Error notifying head observers for editHistoryRef ${editHistoryRef}:`,
        err,
      );
    });
  }

  async editHistoryRef(editHistoryRef: string): Promise<MultiEditProcessor> {
    const editHistories = await this._db.getEditHistories(
      this._cakeKey,
      editHistoryRef,
    );

    /* v8 ignore if -- @preserve */
    if (editHistories.length === 0) {
      throw new Error(`EditHistory with ref ${editHistoryRef} not found.`);
    }

    /* v8 ignore if -- @preserve */
    if (editHistories.length > 1) {
      throw new Error(
        `Multiple EditHistories with ref ${editHistoryRef} found.`,
      );
    }

    const editHistory = editHistories[0];

    // Check if processor already exists
    if (this._processors.has(editHistoryRef)) {
      const processor = this._processors.get(editHistoryRef)!;
      this._head = {
        editHistoryRef,
        processor,
      };
      await this._notifyHeadListener(editHistoryRef);
      return processor;
    }

    // Handle case with previous edit history
    if (editHistory.previous && editHistory.previous.length > 0) {
      /* v8 ignore if -- @preserve */
      if (editHistory.previous.length > 1) {
        throw new Error(
          `EditHistory with ref ${editHistoryRef} has multiple previous refs. Not supported.`,
        );
      }

      const previousEditHistoryRef = editHistory.previous[0];
      const previousProcessor = await this.editHistoryRef(
        previousEditHistoryRef,
      );
      const previousProcessorCloned = previousProcessor.clone();

      const processor = await previousProcessorCloned.applyEditHistory(
        editHistory,
      );

      this._processors.set(editHistoryRef, processor);
      this._head = {
        editHistoryRef,
        processor,
      };
      await this._notifyHeadListener(editHistoryRef);
      return processor;
    }

    // Handle case without previous edit history (base case)
    const processor = await MultiEditProcessor.fromEditHistory(
      this._db,
      this._cakeKey,
      editHistory,
    );

    this._processors.set(editHistoryRef, processor);
    this._head = {
      editHistoryRef,
      processor,
    };
    await this._notifyHeadListener(editHistoryRef);
    return processor;
  }

  private async _persistEdit(edit: Edit): Promise<string> {
    // Store the new Edit
    const { [this._cakeKey + 'EditsRef']: editRef } = (
      await this._db.addEdit(this._cakeKey, edit)
    )[0] as any;
    /* v8 ignore next -- @preserve */
    if (!editRef) {
      throw new Error('MultiEditManager: Failed to create EditRef.');
    }
    return editRef;
  }

  private async _persistMultiEdit(multiEdit: MultiEdit): Promise<string> {
    // Create and store the new MultiEdit
    const { [this._cakeKey + 'MultiEditsRef']: multiEditRef } = (
      await this._db.addMultiEdit(this._cakeKey, multiEdit)
    )[0] as any;
    /* v8 ignore next -- @preserve */
    if (!multiEditRef) {
      throw new Error('MultiEditManager: Failed to create MultiEditRef.');
    }
    return multiEditRef;
  }

  private async _persistEditHistory(editHistory: EditHistory): Promise<string> {
    // Create and store the new EditHistory pointing to the new MultiEdit
    const { [this._cakeKey + 'EditHistoryRef']: editHistoryRef } = (
      await this._db.addEditHistory(this._cakeKey, editHistory)
    )[0] as any;
    /* v8 ignore next -- @preserve */
    if (!editHistoryRef) {
      throw new Error('MultiEditManager: Failed to create EditHistoryRef.');
    }
    return editHistoryRef;
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
