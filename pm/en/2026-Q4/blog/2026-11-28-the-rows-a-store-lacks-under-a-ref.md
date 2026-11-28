<!--
@license
Copyright (c) rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# The rows a store lacks under a ref

## Motivation

- A ref names a state of a route: a tree root, an edit history entry, an
  insert history entry, or a `collection:head` string. Under it hang the
  rows that state is made of.
- Whether a store holds that state completely, and which rows it lacks if
  not, nothing answered generically. Each kind of ref had its own idea.
- A node that holds a state completely can serve it to others; one that
  lacks rows knows what to ask for. Both need the same walk.
- And the build failed on Windows: its last step used `cp` and `mkdir`,
  which `cmd.exe` does not know in that form.

## Strategy

- One walk for every kind of ref, by row hash, through the table configs
  of the store.
- Find the ref's row in any table, then follow every reference it makes.
- Remember subtrees found complete, so a second question is cheap.
- Read through the Io directly, not through a cache that could hide what
  the store really holds.

## MissingRows

- `new MissingRows(io, { maxRemembered })`: `holds(ref)` says whether every
  row under `ref` is in the store, `of(ref, limit)` returns the hashes of
  the rows that are not, up to `limit`.
- The ref is looked up as a whole, else by the part after its last `:`
  (so `collection:<hash>` works); edit and insert history tables first.
- Missing tables and unreadable rows count as missing, never as an error.

## Following references

- Declared references: `ColumnCfg.ref` of every column, in the table config
  with the most columns when a table has several.
- Structural ones: `previous` of history entries, `children` of trees,
  layers, cakes and their slice ids, and the insert history as a special
  case.
- Keys starting with `_` are skipped: a nested `_hash` is no reference.

## Remembering what is complete

- A subtree found complete is remembered by its hash, up to
  `maxRemembered` entries, the oldest first out. Rows are content-addressed,
  so a complete subtree stays complete.
- Rows are read by their hashes in one call where the Io offers
  `readRowsByHashes`, otherwise one by one.

## A build that runs on Windows

- `scripts/copy-readme-to-dist.js` replaces `cp README* dist && mkdir
  dist/src && cp ...`, which stopped in `cmd.exe` after vite and tsc had
  passed. The `dist` is the same as before.

## Result

- One answer to "does this store hold this state?", for every kind of ref.
- 570 tests, coverage at 100 %; the build passes in `cmd.exe` and in Git
  Bash.

## Open Points

- The walk trusts the table configs: a reference a config does not declare
  is not followed.
- Large states are walked row by row on the first question; only the memo
  makes the next one cheap.
