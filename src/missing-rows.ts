// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Io } from '@rljson/io';
import { Rljson, TableCfg } from '@rljson/rljson';

// .............................................................................
/** A row a store lacks. */
export interface MissingRow {
  /** The table it belongs in. Absent when not even the ref's row was found. */
  table?: string;

  /** Its hash. */
  hash: string;
}

// .............................................................................
/** Options of {@link MissingRows}. */
export interface MissingRowsOptions {
  /**
   * How many row hashes are remembered as complete before the memory is
   * cleared and filled again. Default 1 000 000.
   */
  maxRemembered?: number;
}

/** A row as read from the store. */
type Row = Record<string, unknown>;

/** Content types whose rows are looked up first when a ref is located. */
const headTypes = new Set(['editHistory', 'insertHistory']);

/** Content types that never hold a ref's data. */
const metaTypes = new Set(['tableCfgs', 'revisions']);

// #############################################################################
/**
 * Finds the rows under a ref that a store lacks, reading only that store.
 *
 * A ref names a row by its hash: as it is, or — when that row is not found —
 * by the part after its last `:`, so a ref may carry a prefix such as a
 * collection name. The row is looked up in every table of the store, those
 * of type `editHistory` and `insertHistory` first. From there every row it
 * references is followed, in every table:
 *
 * - the reference columns a table's config declares (`ColumnCfg.ref`), with a
 *   hash, a list of hashes, or objects with a `ref`
 * - `previous` of edit histories and multi edits, `children` of trees
 * - `base`, the slice ids row and the components a layer adds
 * - the slice ids row and the layers of a cake, `base` of slice ids
 * - the row an insert history row names (`<table>Ref` → `<table>`)
 *
 * Blobs are not followed: no column says it holds a blob id.
 *
 * A subtree found complete is remembered by its rows' hashes, so the next
 * ref walks only the rows it adds.
 */
export class MissingRows {
  private readonly _complete = new Set<string>();
  private readonly _maxRemembered: number;

  // ...........................................................................
  /**
   * Creates the walk over one store.
   * @param _io - The store. Pass a local one: every row is read from it.
   * @param options - Bounds.
   */
  constructor(
    private readonly _io: Io,
    options: MissingRowsOptions = {},
  ) {
    this._maxRemembered = options.maxRemembered ?? 1_000_000;
  }

  // ...........................................................................
  /**
   * Whether the store holds every row under a ref.
   * @param ref - The ref.
   * @returns True when nothing is missing.
   */
  async holds(ref: string): Promise<boolean> {
    return (await this.of(ref, 1)).length === 0;
  }

  // ...........................................................................
  /**
   * The rows under a ref the store lacks.
   * @param ref - The ref.
   * @param limit - Stop after this many. Default: find them all.
   * @returns The missing rows, empty when the store holds the ref completely.
   */
  async of(ref: string, limit = Infinity): Promise<MissingRow[]> {
    const candidates = MissingRows._candidates(ref);
    if (candidates.some((hash) => this._complete.has(hash))) return [];

    const cfgs = await this._tableCfgs();
    const root = await this._locate(candidates, cfgs);
    if (root === undefined) return [{ hash: ref }];

    const missing: MissingRow[] = [];
    const visited = new Set<string>();
    let frontier = new Map<string, Set<string>>([
      [root.table, new Set([root.hash])],
    ]);

    while (frontier.size > 0) {
      const next = new Map<string, Set<string>>();
      const follow = (table: string, hash: string) => {
        if (this._complete.has(hash) || visited.has(hash)) return;
        const hashes = next.get(table) ?? new Set<string>();
        hashes.add(hash);
        next.set(table, hashes);
      };

      for (const [table, hashes] of frontier) {
        const todo = [...hashes].filter((hash) => !visited.has(hash));
        for (const hash of todo) visited.add(hash);
        const rows = await this._read(table, todo);
        for (const hash of todo) {
          const row = rows.get(hash);
          if (row === undefined) {
            missing.push({ table, hash });
            if (missing.length >= limit) return missing;
            continue;
          }
          for (const [childTable, childHash] of MissingRows._refsOf(
            table,
            row,
            cfgs.get(table),
          )) {
            follow(childTable, childHash);
          }
        }
      }
      frontier = next;
    }

    if (missing.length === 0) this._remember(visited);
    return missing;
  }

  // ######################
  // Private
  // ######################

  // ...........................................................................
  /**
   * The hashes a ref may name: itself, then the part after its last `:`.
   * @param ref - The ref.
   * @returns One or two hashes.
   */
  private static _candidates(ref: string): string[] {
    const at = ref.lastIndexOf(':');
    const suffix = at < 0 ? '' : ref.slice(at + 1);
    return suffix.length > 0 && suffix !== ref ? [ref, suffix] : [ref];
  }

  // ...........................................................................
  /**
   * The table configs of the store, the newest version of each table — the
   * one with the most columns.
   * @returns Table key → config.
   */
  private async _tableCfgs(): Promise<Map<string, TableCfg>> {
    const result = new Map<string, TableCfg>();
    for (const cfg of await this._io.rawTableCfgs()) {
      const known = result.get(cfg.key);
      if (!known || cfg.columns.length > known.columns.length) {
        result.set(cfg.key, cfg);
      }
    }
    return result;
  }

  // ...........................................................................
  /**
   * Finds the row a ref names.
   * @param candidates - The hashes the ref may name, in order.
   * @param cfgs - The table configs.
   * @returns Where the row is, or undefined.
   */
  private async _locate(
    candidates: string[],
    cfgs: Map<string, TableCfg>,
  ): Promise<{ table: string; hash: string } | undefined> {
    const tables = [...cfgs.values()]
      .filter((cfg) => !metaTypes.has(cfg.type))
      .sort((a, b) => Number(headTypes.has(b.type)) - Number(headTypes.has(a.type)))
      .map((cfg) => cfg.key);

    for (const hash of candidates) {
      for (const table of tables) {
        const rows = await this._read(table, [hash]);
        if (rows.has(hash)) return { table, hash };
      }
    }
    return undefined;
  }

  // ...........................................................................
  /**
   * Reads rows of one table by their hashes, from the store itself: no cache,
   * so a row the store lost is missing again. A table the store lacks, or one
   * it cannot read, has none of them.
   * @param table - The table.
   * @param hashes - The hashes.
   * @returns Hash → row, for the rows found.
   */
  private async _read(table: string, hashes: string[]): Promise<Map<string, Row>> {
    const rows = new Map<string, Row>();
    if (hashes.length === 0 || !(await this._io.tableExists(table))) return rows;
    const collect = (result: Rljson) => {
      const data = (result[table] as { _data?: Row[] } | undefined)?._data;
      for (const found of data ?? []) rows.set(found['_hash'] as string, found);
    };
    try {
      if (this._io.readRowsByHashes) {
        collect(await this._io.readRowsByHashes({ table, hashes }));
      } else {
        for (const hash of hashes) {
          collect(await this._io.readRows({ table, where: { _hash: hash } }));
        }
      }
    } catch {
      // Unreadable: every row of it counts as missing.
    }
    return rows;
  }

  // ...........................................................................
  /**
   * Every row a row references.
   * @param table - The row's table.
   * @param row - The row.
   * @param cfg - The table's config, if the store has one.
   * @returns Pairs of table and hash.
   */
  private static _refsOf(
    table: string,
    row: Row,
    cfg: TableCfg | undefined,
  ): Array<[string, string]> {
    const refs: Array<[string, string]> = [];
    const add = (target: unknown, hash: unknown) => {
      if (typeof target !== 'string' || target.length === 0) return;
      if (typeof hash !== 'string' || hash.length === 0) return;
      refs.push([target, hash]);
    };
    const addEach = (target: unknown, value: unknown) => {
      if (Array.isArray(value)) {
        for (const item of value) addEach(target, item);
      } else if (value !== null && typeof value === 'object') {
        add(target, (value as { ref?: unknown }).ref);
      } else {
        add(target, value);
      }
    };

    switch (cfg?.type) {
      case 'insertHistory': {
        // Its declared reference names a multi edits table; the value is the
        // hash of a row of the table the history belongs to.
        const base = table.slice(0, -'InsertHistory'.length);
        add(base, row[`${base}Ref`]);
        return refs;
      }
      case 'trees':
        addEach(table, row['children']);
        break;
      case 'editHistory':
      case 'multiEdits':
        addEach(table, row['previous']);
        break;
      case 'layers':
        add(table, row['base']);
        add(row['sliceIdsTable'], row['sliceIdsTableRow']);
        for (const [sliceId, ref] of Object.entries((row['add'] ?? {}) as Row)) {
          // Hashing puts a `_hash` into the nested object, too.
          if (!sliceId.startsWith('_')) add(row['componentsTable'], ref);
        }
        break;
      case 'cakes':
        add(row['sliceIdsTable'], row['sliceIdsRow']);
        for (const [layerTable, ref] of Object.entries(
          (row['layers'] ?? {}) as Row,
        )) {
          if (!layerTable.startsWith('_')) add(layerTable, ref);
        }
        break;
      case 'sliceIds':
        add(table, row['base']);
        break;
    }

    for (const column of cfg?.columns ?? []) {
      if (column.ref) addEach(column.ref.tableKey, row[column.key]);
    }
    return refs;
  }

  // ...........................................................................
  /**
   * Remembers rows whose subtrees are complete.
   * @param hashes - Their hashes.
   */
  private _remember(hashes: Set<string>): void {
    if (this._complete.size + hashes.size > this._maxRemembered) {
      this._complete.clear();
    }
    for (const hash of hashes) this._complete.add(hash);
  }
}
