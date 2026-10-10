// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Json } from '@rljson/json';
import {
  createEditHistoryTableCfg,
  createEditTableCfg,
  createMultiEditTableCfg,
  Edit,
  EditAction,
  EditHistory,
  MultiEdit,
  timeId,
} from '@rljson/rljson';

import { Db } from '../db.ts';

// .............................................................................
/** One entry of an edit chain, as read back. */
export interface EditChainEntry {
  /** The `EditHistory` ref — the entry's identity. */
  head: string;

  /** The entry's `timeId`. An identity, not an order. */
  timeId: string;

  /** What the entry refers to, e.g. the ref of a tree. */
  dataRef: string;

  /** The entries this one was made from. Empty for a root. */
  previous: string[];

  /** The action recorded with the entry. */
  action: EditAction;
}

// .............................................................................
/** What {@link EditChainManager.append} records. */
export interface EditChainAppendOptions {
  /** What the entry refers to, e.g. the ref of a tree. */
  dataRef: string;

  /**
   * The entries this one was made from: none for a root, one for an ordinary
   * edit, two or more for a merge.
   */
  previous: string[];

  /** What happened. Stored as the entry's `Edit`. */
  action: { name: string; type: string; data: Json };

  /**
   * The entry's `timeId`. Minted when omitted.
   *
   * Pass one to make the entry a function of its content: two nodes that
   * append the same data, previous and timeId write the same rows, and the
   * entry gets the same ref on both.
   */
  timeId?: string;
}

// .............................................................................
/**
 * An append-only chain of edits that is agnostic of what the edits contain.
 *
 * Each entry is three rows in the `${key}Edits`, `${key}MultiEdits` and
 * `${key}EditHistory` tables. An entry may have any number of `previous`
 * entries, so a merge of two lineages is an ordinary entry.
 *
 * Unlike {@link MultiEditManager}, it applies nothing to a cake and keeps no
 * head: which entry a caller stands on is the caller's state. Nothing here
 * orders entries by `timeId`.
 *
 * The `dataRef` column refers to the `${key}` table itself — the trees or
 * cake table the edits are about — so that table has to exist before the
 * first {@link EditChainManager.append}.
 */
export class EditChainManager {
  constructor(
    private readonly _key: string,
    private readonly _db: Db,
  ) {}

  // ...........................................................................
  /**
   * Creates the three tables, or extends them if they exist.
   */
  async init(): Promise<void> {
    await this._db.core.createTable(createEditTableCfg(this._key));
    await this._db.core.createTable(createMultiEditTableCfg(this._key));
    await this._db.core.createTable(createEditHistoryTableCfg(this._key));
  }

  // ...........................................................................
  /**
   * Appends one entry.
   * @param opts - The entry to write; see {@link EditChainAppendOptions}
   * @returns The ref of the new entry and its `timeId`
   */
  async append(
    opts: EditChainAppendOptions,
  ): Promise<{ head: string; timeId: string }> {
    const action: EditAction = { ...opts.action, _hash: '' };
    const edit: Edit = { name: opts.action.name, action, _hash: '' };
    const editRef = this._refOf(
      await this._db.addEdit(this._key, edit),
      'Edits',
    );

    const multiEdit: MultiEdit = { previous: null, edit: editRef, _hash: '' };
    const multiEditRef = this._refOf(
      await this._db.addMultiEdit(this._key, multiEdit),
      'MultiEdits',
    );

    const entryTimeId = opts.timeId ?? timeId();
    const history: EditHistory = {
      timeId: entryTimeId,
      multiEditRef,
      dataRef: opts.dataRef,
      previous: [...opts.previous],
      _hash: '',
    };
    const head = this._refOf(
      await this._db.addEditHistory(this._key, history),
      'EditHistory',
    );

    return { head, timeId: entryTimeId };
  }

  // ...........................................................................
  /**
   * Reads one entry back.
   * @param head - The `EditHistory` ref of the entry
   * @returns The entry, or `undefined` when one of its rows cannot be read
   */
  async entry(head: string): Promise<EditChainEntry | undefined> {
    return (await this.entries([head])).get(head);
  }

  // ...........................................................................
  /**
   * Reads many entries in three batched reads, one per table.
   *
   * An entry whose rows cannot all be read is absent from the result, so a
   * caller that needs every entry compares the result's size with what it
   * asked for. A failing read rejects.
   * @param heads - The `EditHistory` refs of the entries
   * @returns The entries that could be read, keyed by their ref
   */
  async entries(heads: string[]): Promise<Map<string, EditChainEntry>> {
    const result = new Map<string, EditChainEntry>();
    if (heads.length === 0) return result;

    const histories = (await this._db.core.readRowsByHashes(
      `${this._key}EditHistory`,
      heads,
    )) as Map<string, EditHistory>;

    const multiEdits = (await this._db.core.readRowsByHashes(
      `${this._key}MultiEdits`,
      [...histories.values()].map((h) => h.multiEditRef),
    )) as Map<string, MultiEdit>;

    const edits = (await this._db.core.readRowsByHashes(
      `${this._key}Edits`,
      [...multiEdits.values()].map((m) => m.edit),
    )) as Map<string, Edit>;

    for (const head of heads) {
      const history = histories.get(head);
      const multiEdit = history && multiEdits.get(history.multiEditRef);
      const edit = multiEdit && edits.get(multiEdit.edit);
      if (!history || !edit) continue;
      result.set(head, {
        head,
        timeId: history.timeId,
        dataRef: history.dataRef,
        previous: [...(history.previous ?? [])],
        action: edit.action,
      });
    }

    return result;
  }

  // ...........................................................................
  /**
   * Pulls the generated ref out of an insert result.
   * @param result - What an `add*` call of the db returned
   * @param suffix - The table suffix, e.g. `Edits`
   * @returns The ref of the inserted row
   */
  private _refOf(result: unknown, suffix: string): string {
    const rows = result as Array<Record<string, string>>;
    const ref = rows?.[0]?.[`${this._key}${suffix}Ref`];
    /* v8 ignore next -- @preserve a successful insert always returns its ref */
    if (!ref) throw new Error(`EditChainManager: no ref for ${suffix}`);
    return ref;
  }
}
