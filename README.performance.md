# @rljson/db — Performance Optimization Concept

Branch: `performance-optimization`
Status: **CONCEPT — awaiting approval, no implementation yet**
Scope: `@rljson/db` (this package). Cross-repo opportunities in `@rljson/io` are listed separately in §7 and are optional.

---

## 1. Executive Summary

@rljson/db mimics a DBMS on top of the `@rljson/io` data layer. The analysis of all hot paths
(`Db.get`/`_get`, `Db.insert`/`_insert`, `Db.join`, the controllers, the Join pipeline and the
Multi-Edit system) found **four systemic performance paradigms** that dominate runtime, plus a
catalogue of 24 concrete findings:

| # | Systemic problem | Where it hits hardest |
| --- | --- | --- |
| S1 | **N+1 / sequential I/O** — independent Io reads and writes are awaited one-by-one in loops instead of being batched or parallelized | `Db._get` child recursion, `Db.join`, `TreeController.get`, `_insert`, `MultiEditManager.edit` |
| S2 | **Redundant recomputation** — the same data is re-fetched, re-parsed, re-hashed or re-built although it is immutable (content-addressed!) | `Join.rows` getter, `getChildRefs`, cache keys via SHA, `Core.tableCfg`, controller re-creation |
| S3 | **Accidental quadratic/cubic algorithms** — full scans or full re-builds inside loops | `Join.value()` inside filters (O(R²C²)), `Join.select` (O(R²)), `detectDagBranch` (O(N) per insert ⇒ O(N²) cumulative), `ComponentController._getByWhere` (dumpTable per ref) |
| S4 | **Unexploited immutability** — content-addressed rows can never change, yet nothing caches them; the query cache is unbounded, never invalidated, and its keys are SHA-hashed deep clones | `Db._cache`, every `readRow(table, hash)` |

The single most important insight: **every `Io.readRows` call is expensive** (in `IoMem` it is a
full table scan *plus* a re-sort *plus* a SHA re-hash of the result set — file/network backends are
worse). The db layer currently emits **O(rows × depth)** such calls per query and **O(sliceIds ×
columns)** per join. Reducing *call count* is worth more than any micro-optimization.

**Estimated overall effect after implementing Phases 1–3** (order-of-magnitude estimates, to be
validated by the Phase-0 benchmark suite):

| Scenario | Today (complexity) | After | Estimated speedup |
| --- | --- | --- | --- |
| `Db.join` — 100 sliceIds × 10 columns | ~1000 sequential full `Db.get` route resolutions | ~2–4 batched/parallel gets + local column extraction | **50–500×** |
| Join `filter()` on 1 000 rows × 10 cols | O(R²·C²) with route re-parsing per cell | O(R·C) on a pre-materialized value matrix | **100–1000×** |
| Multi-edit chain replay (20 edits) | full join re-query per edit + full replay | cached processors + incremental joins | **10–100×** |
| `Db.get` on nested route, 100 parent rows | 100 sequential child sub-queries/level | parallel + batched child resolution | **10–50×** (wall clock) |
| Tree expansion, 1 000 nodes | 1 000 sequential `readRow` calls (each O(table) on IoMem) | 1 table read + in-memory expansion | **100×+** |
| `Db.insert` with 10 000-row InsertHistory | full history dump + scan per insert | incremental tip tracking (O(1) amortized) | **10–100×** on the history/conflict step |
| Repeated identical reads (immutable rows) | full Io round-trip every time | content-addressed row cache hit | **∞** (cache hit ≈ 0 cost) |

Risk is manageable because the package has 100 % test coverage: behavior-preserving rewrites are
verified by the existing suite, and a benchmark suite (Phase 0) makes every claim measurable.

---

## 2. Cost Model: What an Io Call Actually Costs

All db-layer reasoning must start from the real cost of the backend primitives
(verified in `@rljson/io` `io-mem.ts`):

- `readRows({table, where})` — **full linear scan** of the table (no hash index, not even for
  `_hash` lookups), then `sortTableDataAndUpdateHash()` on the *result*: a sort **plus SHA-256
  re-hash per call**. Cost ≈ O(|table|) + O(|result|·log|result|) + hashing.
- `write({data})` — per row an O(|table|) linear `find` dedup, then full-table sort + re-hash +
  global hash update. Bulk import of N rows ⇒ **O(N²)**.
- `dumpTable` — full copy of the table.
- `tableExists` / `contentType` / `rawTableCfgs` — cheap-ish but still async round-trips; on
  network-backed Ios each one is a wire round-trip.

Consequence: **the primary optimization currency is "number of Io calls", the secondary one is
"bytes touched per call".** All findings below are weighted accordingly.

---

## 3. Findings Catalogue

Each finding: **[ID] Location — Problem → Fix → Estimated effect / Effort (S/M/L) / Risk**

### 3.1 Query path — `Db.get` / `Db._get` (db.ts)

**[F1] `_get` fetches children per parent row, sequentially — the N+1 query problem**
`db.ts:553-877`: `for (let i = 0; i < nodeRowsFiltered.length; i++) { ... await this._get(childrenRoute, ...) }`.
Every parent row triggers its own recursive child query; each of those is ≥1 full-scan `readRows`.
→ **Fix (2 stages):**
  1. *Parallelize*: collect the per-row `_get` promises and `Promise.all` them (results are
     independent; the per-row post-processing stays as-is).
  2. *Batch*: resolve all `childrenRefs` of **all** rows in one child query (single `_get` with the
     union of refs), then distribute results back to parents via the existing
     `resolvedChildrenHashSet` logic. Requires the row-cache [F20] or a where-with-array convention.
→ **Effect:** wall-clock ÷ rowCount for stage 1 (10–100× for wide nodes); stage 2 additionally
   divides Io scan work by rowCount. **Effort:** stage 1 = S, stage 2 = M. **Risk:** low (stage 1), medium (stage 2 — ordering/dedup semantics covered by tests).

**[F2] Cache keys are SHA hashes of deep-cloned params**
`db.ts:196-212`: `cacheHash = hsh(rmhsh(params))._hash` — `rmhsh` deep-clones the whole
where/filter structure, `hsh` SHA-hashes it — *on every cacheable `_get` invocation, including every
recursion level*.
→ **Fix:** plain deterministic string key: `` `${route.flat}|${JSON.stringify(where)}|${JSON.stringify(filter)}|${sliceIds}|${routeAccumulator}|${optsBits}` ``.
`JSON.stringify` is ~10–50× faster than deep-clone + SHA and collision-safety is not needed for a
private cache key.
→ **Effect:** 5–20× on cache-key overhead, which today can rival the actual fetch on cache hits. **Effort:** S. **Risk:** none (key format is internal).

**[F3] Cache is written even when not cacheable, is unbounded, and is never invalidated**
`db.ts:471` and `db.ts:940` call `this._cache.set(cacheHash, result)` **unconditionally** — when
`cacheable === false`, `cacheHash` is `''`, so results are stored under the `''` key (dead memory,
one stale slot). The cache also has no size bound and no invalidation on `insert`.
→ **Fix:** (a) guard both `set` calls with `if (cacheable)`; (b) bounded LRU (e.g. 500 entries,
Map-based, O(1)); (c) invalidate per-table on insert: keep a `table → Set<cacheKey>` index and drop
affected entries in `_insert`/`insertTrees`.
→ **Effect:** removes a latent memory leak + stale-data hazard; enables *safe* aggressive caching
elsewhere. **Effort:** S–M. **Risk:** low.

**[F4] `getChildRefs` re-executes the query that `_get` just ran**
`db.ts:547-550` calls `nodeController.getChildRefs(nodeRow._hash)` per row; every controller
implementation (`component-controller.ts:131-196`, `cake-controller.ts:103-122`,
`tree-controller.ts:361-387`, `layer-controller.ts:306-330`) **re-fetches the row via
`this.get(where)`** although `_get` already holds the row object. ComponentController additionally
re-fetches `tableCfg` via `Core.tableCfg` (which itself fetches **all** raw table configs, [F13]).
→ **Fix:** add `getChildRefsFromRow(row: Json): ControllerChildProperty[]` (pure, synchronous — all
implementations only inspect the row object + column configs already cached on the controller).
`_get` passes the row it already has. Keep the old signature as a wrapper for API compat.
→ **Effect:** eliminates one full query per parent row per level — ≥2× on every nested `get`;
far more on IoMem where each query is a table scan. **Effort:** S–M. **Risk:** low.

**[F5] `isolatePropertyKeyFromRoute` runs twice per `get` and probes tables sequentially**
`db.ts:127` runs it, then `indexedControllers` (`db.ts:1585`) runs it **again**; each run awaits
`io.tableExists` per segment sequentially.
→ **Fix:** run once in `get`, pass the isolated route into `indexedControllers`; batch the
`tableExists` probes with `Promise.all`; memoize `tableExists` results (tables are only ever
created, never dropped, during a session — invalidate on `createTable`).
→ **Effect:** removes 1–2 io round-trips × segments per `get`. **Effort:** S. **Risk:** low.

**[F6] Controllers are re-created and re-initialized on every `Db.get`/`insert`**
`indexedControllers` (`db.ts:1580-1606`) builds fresh controllers each call; each `init()` performs
`contentType` + `tableCfg` (+ ref-column resolution with recursive `tableCfg` fetches in
`ComponentController.init`). The architecture docs promise "Controller Memoization" — it does not exist.
→ **Fix:** controller instance cache on `Db`, keyed `tableKey|refsBase`. Controllers are stateless
after init (all fields are config-derived), so reuse is safe; invalidate on table creation/extension.
→ **Effect:** removes 2–6 io calls per table per operation; big win for high-frequency small ops
(edits, inserts, connector traffic). **Effort:** S–M. **Risk:** low–medium (must audit controllers for per-call mutable state; `CakeController._refs.base` deletion in `insert` (`cake-controller.ts:136`) needs cleanup).

**[F7] Per-level `merge` + `makeUnique` full-structure traversals**
`db.ts:883`: `makeUnique(merge(...nodeChildrenArray))` deep-merges all child rljsons per level and
then re-traverses the merged structure (`object-traversal`) to dedup `_data` arrays.
→ **Fix:** accumulate children per tableKey in a `Map<tableKey, Map<hash, row>>` while iterating
(dedup for free), materialize once at the end. Avoids deep merge + traversal entirely.
→ **Effect:** O(total children) instead of O(children × tables × depth); 2–10× on wide multi-table
results. **Effort:** S–M. **Risk:** low.

**[F8] Cell path arrays are rebuilt with spreads at every recursion level**
`db.ts:911-931` (and the layer/cake/component branches): every cell object and its `path` array is
copied per level ⇒ O(cells × depth²) allocations.
→ **Fix:** build paths root-relative once at the leaf using the `routeAccumulator` depth (known),
or use a prefix-chain (linked-list) flattened once at the top of the recursion.
→ **Effect:** allocation-bound; 2–5× on cell-heavy deep queries. **Effort:** M. **Risk:** medium (path format is observable in tests/goldens — must stay identical).

### 3.2 Join engine (db.ts `join`, join/)

**[F9] `Db.join` executes sliceIds × columns sequential full route queries — the dominant join cost**
`db.ts:1009-1034`: nested `for (sliceId) { for (columnInfo) { await this.get(columnRoute, cakeRef, undefined, [sliceId]) } }`.
100 slices × 10 columns = **1 000 sequential full route resolutions**, each walking
cake→layer→component with all the costs above.
→ **Fix (3 stages):**
  1. *Dedupe*: columns share route prefixes — group `columnSelection.columns` by
     `route.flatWithoutRefs` minus property key (typically 1–3 unique component tables). One `get`
     per unique table route per sliceId, then extract per-column values locally from the returned
     container (`isolatePropertyFromComponents` logic, local, no Io).
  2. *Parallelize*: `Promise.all` across sliceIds (and across unique routes).
  3. *Single-pass*: fetch the whole cake→layers→components subtree **once** without sliceId filter
     (one `get`), then slice locally per sliceId using the `ControllerChildProperty.sliceIds`
     already returned by `LayerController.getChildRefs`. Turns S×C queries into **O(unique layer
     tables)** queries.
→ **Effect:** stage 1+2 ≈ 10–50×; stage 3 ≈ 100×+ for large slice sets. This is the highest-value
fix in the package. **Effort:** stage 1+2 = M, stage 3 = L. **Risk:** medium — semantics (per-slice layer overrides) covered by join/edit test suites.

**[F10] `Join.rows` getter is rebuilt from scratch on every access — and `Join.value()` calls it per cell**
`join.ts:523-554`: the getter does, for every row × column, a `dataColumns.find(...)` whose callback
**parses `Route.fromFlat(colInfo.route)` freshly per candidate**. `Join.value(row, col)`
(`join.ts:380-382`) evaluates `this.rows[row][column]` — i.e. **the entire matrix is rebuilt for
every single cell probe**. `RowFilterProcessor._filterColumnAnd/Or` call `join.value` per remaining
row per column ⇒ **O(R² × C²) with route parsing in the inner loop**.
→ **Fix:** (a) materialize the rows matrix **once** per process step and cache it on the process
entry (invalidated naturally because each step produces new `data`); (b) pre-compute a
`route→columnIndex` map instead of `find` + parse (routes are already hashed in
`ColumnSelection.routeHashes`); (c) `Join.value` reads from the cached matrix.
→ **Effect:** filters/sorts drop from O(R²C²·parse) to O(R·C); 100–1000× on 1 000-row joins.
**Effort:** M. **Risk:** low (pure caching, results identical).

**[F11] `Join.select` — `Object.entries(this.data)[i]` inside the row loop**
`join.ts:231-232`: rebuilds the full entries array on **every iteration** ⇒ O(R²).
→ **Fix:** `for (const [sliceId, row] of Object.entries(this.data))`.
→ **Effect:** O(R²)→O(R); noticeable ≥1 000 rows. **Effort:** trivial. **Risk:** none.

**[F12] Row hashes are recomputed with SHA for every row on every process step**
`join.ts:170-177`, `join.ts:239-244`, `_hashedRows`: every `select`/`setValue` re-hashes **all**
rows (flatMap of all cell values → `Hash.default.calcHash`), even untouched ones; every process step
also keeps a **full copy** of the data map (O(steps × rows) memory).
→ **Fix:** (a) compute rowHash lazily on first access (getter) — most steps never read it;
(b) `setValue` re-hashes only rows whose cells actually changed; (c) keep per-step data as
copy-on-write (only changed sliceIds re-referenced).
→ **Effect:** removes SHA hashing from the hot path of every edit step: 5–20× per `setValue`/
`select` on large joins; memory O(changed) instead of O(all). **Effort:** M. **Risk:** low–medium (rowHash consumers must tolerate lazy evaluation).

**[F13] `setValue` re-parses routes and re-isolates trees per cell**
`join.ts:114`: `Route.fromFlat(setValue.route)` inside the per-column loop (per sliceId);
`isolate`/`inject` walk the tree per cell.
→ **Fix:** parse the route once before the loop; short-circuit columns whose route doesn't match
before entering the per-cell work (cheap string compare on `flatWithoutRefs`).
→ **Effect:** 2–10× per setValue on wide joins. **Effort:** S. **Risk:** none.

**[F14] `RowFilterProcessor._filterColumnAnd` pushes duplicate row indices**
`row-filter-processor.ts:203-211`: a row with multiple matching cell values is pushed once **per
matching value**; duplicates then multiply the work of every subsequent column pass (they are only
deduped at the very end via `Set`).
→ **Fix:** `break` after the first match.
→ **Effect:** removes a potential exponential blow-up on multi-value cells; also a latent
correctness hazard. **Effort:** trivial. **Risk:** none.

**[F15] `RowSort` comparator allocates arrays per comparison**
`row-sort.ts:111-170`: each of the O(R log R) comparisons re-derives insert/base values through
4 conditional array wraps.
→ **Fix:** precompute a sort-key array (one pass, R × sortCols), sort indices by keys.
→ **Effect:** 5–20× on large sorts. **Effort:** S. **Risk:** low.

### 3.3 Controllers

**[F16] `ComponentController._getByWhere` with reference columns: full table dump + async row filter per ref-clause**
`component-controller.ts:230-242`: for **each** resolved reference where-clause it calls
`dumpTable` (full copy!) and then linear-scans with `await this.filterRow(row, ...)` — an async
function per row (promise machinery per row) doing nested `equals`.
→ **Fix:** (a) dump the table **once** per `_getByWhere` (not per clause) — or better, use
`readRows` per clause and only fall back to scanning for array-valued columns; (b) make `filterRow`
synchronous (it does no I/O — same for Cake/SliceId/Tree implementations; keep the interface async
but stop awaiting per-row: evaluate sync core); (c) index the clauses by column and test all clauses
in one scan pass.
→ **Effect:** refs-in-where queries go from O(clauses × table) + promise overhead to O(table);
5–50×. **Effort:** M. **Risk:** low.

**[F17] `_readRowsWithReferences` issues sequential queries for array values**
`component-controller.ts:463-487`: splits array-valued where into N clauses, awaits each
`readRows` **sequentially**, then deep-merges.
→ **Fix:** `Promise.all` the clause reads; concat `_data` + hash-dedup instead of `merge()`.
→ **Effect:** wall-clock ÷ N per array-ref lookup. **Effort:** S. **Risk:** none.

**[F18] `TreeController.get` expands children one Io call per node, sequentially**
`tree-controller.ts:144-159`: recursive descent, one `readRow` (full table scan on IoMem!) per
node, awaited sequentially. A 1 000-node tree = 1 000 sequential table scans.
→ **Fix:** level-wise BFS batching: collect all child hashes of the current frontier, fetch them in
one pass — with the row cache [F20] backed by a **single `dumpTable`** when the frontier exceeds a
threshold (e.g. >8 nodes), building a local `hash→row` index. Recursion depth guard stays.
→ **Effect:** O(depth) or O(1) Io calls instead of O(nodes): 100×+ for big trees; directly speeds
`fs-agent` scans. **Effort:** M. **Risk:** medium (ordering of `_data` output must stay: children-before-parent — preserved by explicit post-order assembly).

**[F19] `LayerController.resolveBaseLayer` — recursive, uncached, called per row**
`layer-controller.ts:181-304` + `getChildRefs:306-330` + `filterRow:332-344`: every call re-fetches
the base-layer chain (readRow per level, new `SliceIdController` per level, recursive). `filterRow`
calls it **per row** inside table scans [F16].
→ **Fix:** memoize resolved layers by layer `_hash` on the controller (immutable content ⇒ safe
forever); share one `SliceIdController` per sliceIds table; same for
`SliceIdController.resolveBaseSliceIds` and `Db._resolveSliceIds` (`db.ts:1041-1065` creates a new
controller per call).
→ **Effect:** layer-heavy queries (i.e. every cake/join path) save O(chainLength) Io calls per row;
5–50× on layered data. **Effort:** S–M. **Risk:** low.

### 3.4 Read layer / caching (cross-cutting)

**[F20] No content-addressed row cache despite full immutability — the biggest unexploited invariant**
Rows are identified by content hash; **a (table, _hash) → row mapping can never become stale.**
Yet every `readRow(table, hash)` goes to Io (full scan on IoMem).
→ **Fix:** `RowStore` on `Core`: bounded LRU `Map<'table|hash', row>` consulted by
`readRow`/`_getByHash`; populated by every read **and every write/import** (write-through: rows we
just wrote never need re-reading — the Connector/insert paths read back their own writes today).
Add **request coalescing**: concurrent identical reads share one in-flight promise.
→ **Effect:** hash-addressed reads (route refs, tree children, layer bases, join columns) become
memory lookups after first touch; combined with F18/F9 this converts most Io traffic to O(1).
**Effort:** M. **Risk:** low — the invariant is architectural; only `where`-queries (non-hash) must bypass it.

**[F21] `Core.tableCfg` fetches ALL table configs per call; `Core.tables()` dumps the whole DB**
`core.ts:117-121`: `rawTableCfgs()` + linear `find` per call — and it's called per controller init,
per `getChildRefs` [F4]. `core.ts:101-103`: `tables()` = `dump()` (full DB!) — any caller wanting
table names pays a full dump.
→ **Fix:** cache `rawTableCfgs` on Core (invalidate on `createTable`/`createOrExtendTable`);
implement `tables()` via `rawTableCfgs` keys instead of `dump`.
→ **Effect:** removes the most frequent redundant Io call in the codebase. **Effort:** S. **Risk:** low.

### 3.5 Insert path

**[F22] `Core.import` runs full BaseValidator on every write — including every 1-row internal write**
`core.ts:76-98`: every controller `insert`, every `_writeInsertHistory`, every edit/multiEdit/
editHistory persist constructs a `Validate` + `BaseValidator` and validates the payload. For
internally-constructed single rows this is pure overhead (hashing + structural walks).
→ **Fix:** `import(data, {validate?: boolean})` defaulting to `true`; internal single-row writes
from controllers/history pass `validate: false` (they build the structures themselves). Public
`db.core.import` behavior unchanged.
→ **Effect:** 2–5× on the write path (validator cost ≈ or > write cost for small payloads).
**Effort:** S. **Risk:** low — internal payload shapes are fixed by the controllers.

**[F23] `detectDagBranch` dumps and scans the full InsertHistory after EVERY insert**
`db.ts:1676-1695` + `1618-1651`: each `_writeInsertHistory` triggers `dumpTable(history)` + full
tip-scan ⇒ O(N) per insert, **O(N²) cumulative**; with 10k history rows every insert drags 10k rows
through memory.
→ **Fix:** incremental tip tracking: maintain `Map<table, Set<timeId>>` of current tips on `Db`
(lazy-initialized with one scan, then updated in O(|previous|) per insert: remove referenced
parents, add new timeId). `detectDagBranch` reads the set. Persisted state not needed — it's a
session-level accelerator with the existing full scan as cold-start.
→ **Effect:** insert-path conflict detection O(N)→O(1) amortized; 10–100× on the history step for
long-running nodes (directly relevant to Connector/heartbeat traffic). **Effort:** M. **Risk:** medium (must stay consistent when history rows arrive via `core.import` bypass — hook the tip-map update into `_writeInsertHistory` and invalidate the lazy map on external imports to the history table).

**[F24] `_insert` awaits component/layer/slice writes sequentially; `MultiEditManager.edit` persists 3 records sequentially**
`db.ts:1278-1338` (per-sliceId component inserts), `db.ts:1350-1418` (per-component inserts),
`multi-edit-manager.ts:59-106` (`_persistEdit` → `_persistMultiEdit` → processor → `_persistMultiEdit`
→ `_persistEditHistory`, all awaited in series; the first `_persistMultiEdit` result is even
discarded for the head-exists path).
→ **Fix:** `Promise.all` independent component/slice writes (their refs don't depend on each
other); in the manager, persist edit+multiEdit concurrently where the multiEdit hash doesn't depend
on the persisted edit's *write* (it's a content hash, computable locally); drop the redundant
persist.
→ **Effect:** 2–4× on multi-row inserts and per-edit latency. **Effort:** S–M. **Risk:** low–medium (IoMem write is not concurrency-hostile — single-threaded event loop — but write-order-dependent hashes must be audited; keep sequential where `previous` chains require it).

### 3.6 Multi-Edit replay

**[F25] Edit-chain replay re-executes every edit from scratch, re-querying the join base each time**
`multi-edit-processor.ts:301-308` (`_processAll` loops all edits), `:146-179` (`applyEditHistory`
re-resolves + replays the full chain on a *cloned* processor that already holds the join state).
`MultiEditManager` caches processors per `editHistoryRef` (good) but the chain walk
(`editHistoryRef()`) is recursive-sequential per hop with 2 queries per hop (`getEditHistories`,
`getMultiEdits` — each a `readRows`).
→ **Fix:** (a) in `applyEditHistory`, replay only edits **newer** than the clone's last applied
edit (the clone's `_edits` tail is known — currently it re-pushes and re-processes everything);
(b) batch the chain resolution: `getInsertHistory`-style single dump of the multiEdits/edits tables
into a local map, walk in memory (the chains are content-addressed — perfect for the row cache
[F20]); (c) note F9/F10 make each replayed step itself cheap.
→ **Effect:** head updates after N edits go from O(N × join-query) to O(1 × incremental step);
10–100× for long edit sessions (the interactive editing path!). **Effort:** M. **Risk:** medium (replay semantics guarded by multi-edit test suite).

### 3.7 Minor / hygiene

- **[F26]** `Notify.notify` / `_insert` build `Route.fromFlat(result.route)` per result — pass the
  route object along instead. (S)
- **[F27]** `getEditHistories` sorts with `getTimeIdTimestamp` re-parsed per comparison — precompute
  keys. (S)
- **[F28]** `mergeTrees` JSON.stringify/parse for path grouping — use a cheap join(' ') key. (S)
- **[F29]** `Join.componentRoutes`/`layerRoutes`/`cakeRoute` getters re-parse all routes per access —
  compute once in the constructor of `ColumnSelection`. (S)

---

## 4. Target Paradigms (the "after" architecture)

1. **Batch-first I/O discipline.** Any loop that awaits an Io call is a bug unless order matters.
   Standard utilities: `promiseAllInBatches`, level-wise BFS for trees, union-fetch + local
   distribution for children.
2. **RowStore: content-addressed read layer.** One bounded write-through LRU on `Core` for
   `(table, hash) → row`, with request coalescing. Everything hash-addressed flows through it.
3. **Materialize once, index, reuse.** The Join keeps an indexed value matrix per process step;
   `ColumnSelection` owns `route→index` maps; controllers are cached on `Db`; table configs are
   cached on `Core`.
4. **Incremental over recompute.** DAG tips, multi-edit replay, join row hashes: maintain deltas,
   not full rebuilds.
5. **Measured, not believed.** A vitest `bench` suite (Phase 0) pins baselines for the 6 scenarios
   in §1; every phase must show its multiplier in CI-runnable benchmarks.

---

## 5. Implementation Plan

| Phase | Content | Findings | Effort | Expected gain |
| --- | --- | --- | --- | --- |
| **0** | Benchmark suite (`test/bench/*.bench.ts` via vitest bench; scenarios from §1 using `example-static` mass data) + baseline numbers committed to the concept | — | 0.5–1 d | measurement basis |
| **1** | **Quick wins, zero-risk:** F2, F3a/b, F5, F11, F13, F14, F15, F17, F21, F22, F26–F29 | 13 items | 1–2 d | 2–10× broad |
| **2** | **Join engine:** F9 (stages 1+2), F10, F12; **Filters:** matrix materialization | 4 items | 2–4 d | 50–500× joins/edits |
| **3** | **Read layer & query path:** F20 RowStore, F1 (stage 1→2), F4, F6, F7, F16, F18, F19 | 8 items | 3–5 d | 10–100× nested gets & trees |
| **4** | **Insert path & multi-edit:** F23, F24, F25, F3c (cache invalidation) | 4 items | 2–3 d | 10–100× edit sessions |
| **5** | *(Optional, separate approval — cross-repo)* §7 Io extensions; F9 stage 3, F1 stage 2 full batching | — | 3–5 d | further 5–50× |

Each phase = one or more PR-sized commits on `performance-optimization` (≤5 files per commit per
CLAUDE.md), each passing `pnpm test` at 100 % coverage, docs updated first. Benchmarks re-run and
deltas recorded in this file per phase. Merge to `main` only when all phases through 4 are complete
and validated (per your instruction).

### Compatibility guarantees (verified against `src/index.ts` and the test suite)

**Public package API (exports of `index.ts`): fully preserved.**
`Db`, `Connector`, `MultiEditManager`, `Join`, `inject`, `isolate`, `makeUnique`, `mergeTrees`,
the example exports and all re-exported types keep their exact signatures and observable behavior.
All changes to these are either internal (caching, batching, parallelization) or **additive**:

- `Core.import(data)` → `Core.import(data, options?: { validate?: boolean })` — optional param,
  default `true` = today's behavior.
- `Controller.getChildRefsFromRow(row)` — new method; existing `getChildRefs(where)` stays and
  keeps its semantics (used directly by tests, 5 call sites).
- `Db.cache` stays typed `Map<string, Container>` and `setCache(Map)` stays — the LRU bound (F3b)
  is implemented *on top of* a Map (insertion-order eviction), so `cache.size`, `cache.get`,
  `setCache(new Map())` (asserted in `db.spec.ts:68-86, 605-620`) keep working.

**Semi-public methods called directly by the test suite — signatures and results preserved:**
`detectDagBranch` (11 call sites — F23 only accelerates it internally, same `Conflict|null`),
`filterRow` (6 — stays `Promise<boolean>`; only the *per-row awaiting inside scans* is removed),
`getChildRefs` (5), `buildTreeFromTrees`/`buildCellsFromTree`, `resolveBaseLayer`, `Join.rows`
(26 call sites — F10 caches the matrix; identical values), `Join.value`, `_get`/`_insert`
(signatures unchanged).

**Output shapes are pinned by goldens** (`test/goldens/`: view, view-with-data, column-selection):
Container `rljson`/`tree`/`cell` structures — including cell `path` arrays (F8) and `_data`
ordering (F18 tree expansion) — must reproduce byte-identical goldens. Any diff in
`pnpm updateGoldens` output = regression, not an acceptable change.

**Known, deliberate test updates (bug-fix fallout — exactly two):**
1. `db.spec.ts:606` asserts `cache.size === 7` after a nested get. That count includes entries
   written by the unconditional `cache.set` even for non-cacheable calls (the `''`-key bug, F3a).
   Fixing F3a lowers the count; the assertion is updated *in the same commit* with an explanation.
2. Any future test that relied on stale cache after insert would be affected by F3c invalidation —
   none found in the current suite; the documented `notify → clearCache` pattern remains valid.

F14 (duplicate filter indices) changes **no** observable result — the final `Set` dedup already
masked it; only wasted work is removed.

**Test strategy per phase:** every commit runs the full suite at 100 % coverage (repo rule).
Phase 0 additionally locks behavior with benchmark fixtures reusing `example-static` mass data, so
performance changes are measured against the same data the correctness suite uses. New code paths
(RowStore, tip tracking, matrix cache) get their own unit tests to hold the 100 % bar.

---

## 6. Estimated Combined Effects (summary)

Multipliers compose across layers (call-count reduction × per-call reduction × parallelism):

| Workload | Phase 1 | + Phase 2 | + Phase 3 | + Phase 4 |
| --- | --- | --- | --- | --- |
| Wide join (100×10) | 2–3× | **50–200×** | 100–500× | — |
| Interactive edit step (setValue on head) | 2–5× | 20–100× | 30–150× | **50–300×** |
| Nested `Db.get` (3 levels, 100 rows) | 2–3× | — | **20–80×** | — |
| Tree read 1 000 nodes | ~1× | — | **100×+** | — |
| Insert w/ 10k history | 2–4× | — | 3–8× | **20–100×** |
| Bulk import 10k rows | 2–5× (F22) | — | — | — (io-bound, see §7) |

These are complexity-class-derived estimates; Phase 0 baselines turn them into measured numbers.

---

## 7. Cross-Repo Opportunities (out of scope here, listed for the record)

In `@rljson/io` (would multiply all of the above; requires separate tickets in the io repo, bottom-up publish order applies):

- **IO-1:** Hash index per table in `IoMem` (`Map<_hash, row>`) — `readRows({_hash})` O(1) instead
  of full scan.
- **IO-2:** Stop `sortTableDataAndUpdateHash` on **read** results (re-hashing reads is pure waste);
  hash lazily or on write only.
- **IO-3:** `write` dedup via the hash index instead of `Array.find` per row (bulk import O(N²)→O(N)).
- **IO-4:** Batch read API: `readRowsByHashes(table, hashes[])` (or array-value `where` = IN
  semantics) — makes db-layer batching first-class.
- **IO-5:** `tableKeys(): Promise<string[]>` so `Core.tables()` needn't `dump()`.

---

## 8. Appendix: Finding × File Map

| File | Findings |
| --- | --- |
| `src/db.ts` | F1–F9, F22–F24, F26, F27 |
| `src/core.ts` | F20, F21, F22 |
| `src/join/join.ts` | F10–F13, F29 |
| `src/join/filter/row-filter-processor.ts` | F10, F14 |
| `src/join/sort/row-sort.ts` | F15 |
| `src/join/selection/column-selection.ts` | F10b, F29 |
| `src/controller/component-controller.ts` | F4, F16, F17 |
| `src/controller/tree-controller.ts` | F4, F18 |
| `src/controller/layer-controller.ts` | F4, F19 |
| `src/controller/slice-id-controller.ts` | F19 |
| `src/controller/cake-controller.ts` | F4, F6 |
| `src/edit/multi-edit-processor.ts` | F25 |
| `src/edit/multi-edit-manager.ts` | F24, F25 |
| `src/tools/merge-trees.ts` | F28 |
| `src/tools/make-unique.ts` | F7 |
| `src/notify.ts` | F26 |
