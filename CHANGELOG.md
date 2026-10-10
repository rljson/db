# Changelog

## [0.1.1]

### The hub's order reaches the client

A stamping server (`@rljson/server` ≥ 0.0.76, `ServerOptions.stamp`) gives
every ref it relays a `RefStamp` — `(domain, epoch, hub, n)`, compared with
`compareRefStamp` and read from no clock. The Connector carries it without
interpreting it:

- **Added** `RefArrivalInfo.stamp`: a listener's third argument holds the stamp
  an announcement or a bootstrap arrived with.
- **Added** `send(ref, { stamp })`: a ref forwarded by a bridge, or announced
  again, keeps the stamp it already has instead of getting a second one.
- **Added** `onStamp(cb)`: a hub relays to everyone but the sender, so the
  sender is told its stamp on `${route}:stamp`. This is the only way it learns
  it.
- A malformed stamp, from the wire or from a caller, is dropped, never passed
  on. Against a server that does not stamp, nothing changes.
- **Exported** the types `RefArrivalInfo`, `StampCallback`, `RefStamp` and
  `StampPayload`.

### `EditChainManager`

**Added** an append-only edit chain that knows nothing about what its entries
contain. An entry names its data (`dataRef`), what it was made from
(`previous`: none for a root, two or more for a merge) and an action. It keeps
no head and orders nothing by `timeId`: where a caller stands is the caller's
state. `entries()` reads many entries in three batched reads and leaves out any
whose rows cannot all be read.

### Dependencies

`@rljson/rljson` 0.0.86, `@rljson/io` 0.0.85.

## [0.0.48]

### A gap-fill storm can no longer feed itself

A gap-fill answer is processed ref by ref through `_processIncoming`, so any ref
in it that jumps ANOTHER sender's sequence opens a second gap and asks again.
Each answer carries the hub's whole matching ref log, so each round trip is
expensive on the hub: on 2026-09-29 the cloud EventHub served **858 of them in
18.2 seconds** and died of `FATAL ERROR: Reached heap limit`, heap at 990 MB,
inside socket.io's outbound packet encoder — 124-203 kB of `JSON.stringify` per
answer, built synchronously, faster than the collector could keep up.

This is a different fault from the recursion closed in 0.0.47 (#60). That one was
one ref reopening its own gap until the call stack blew; this one is many senders'
refs opening each other's gaps, and it terminates — expensively.

- **Added** `Connector.gapFillMinIntervalMs` (250 ms) and a rate limit in front
  of the request.

### Why a rate limit and not a suppression

The tempting fix is to mark replayed refs the way a bootstrap is marked and skip
gap detection for them. **That loses refs.** A gap-fill answer is filtered by one
`afterSeq` across every sender in the log, so it routinely carries refs from
senders whose own gaps it does not fill. Skipping detection would move those
senders' high-water marks past refs that were never delivered, and nothing would
ever ask for them again.

So the rate is bounded instead of the detection. A storm collapses to a couple of
requests; every gap is still detected, and the next live ref re-opens any request
the window dropped. Nothing is permanently suppressed — the property that makes
this safe to put in the convergence path, and the reason the tests assert it
explicitly.

Pairs with `@rljson/server` 0.0.70, which bounds the size of each answer. Either
change alone survives the storm; together the storm costs a couple of 25 kB
messages instead of 858 × 150 kB.

## [Unreleased]

### Added

- **`stateBeaconEvent(route)`** (ONE-446): the name of the event a hub's
  state beacon is sent on (`${route}:state`). Defined next to the Connector
  because the Connector deliberately never subscribes to it; imported by
  `@rljson/server` (which sends it) and `@rljson/fs-agent` (which reads it),
  so the name exists once instead of in both.

### Changed

- **`@rljson/io` pinned to 0.0.79.** Its two fixes — an error that survives the
  peer boundary so a benign miss stays benign, and a readable that rejects with
  nothing no longer breaking the read — reach every package that reads through
  `db`. Pinned here first because `db` declares `io` itself: lifting it only in
  a consumer would leave `db` running against an `io` it never declared.

### Fixed

- **Gap-fill no longer recurses on a message the hub never received**
  (ONE-446). The Connector asked for a gap before recording the ref that
  revealed it; over a synchronous socket the answer arrived inside that
  request, carried the same ref again and reopened the same gap — until
  `Maximum call stack size exceeded` swallowed the ref (1–16 times per CI run
  of `@rljson/fs-agent`). The request now goes out last, and gap-fill entries
  with the connector's own origin are skipped like on the live channel.

- **Tree INSERT Double-Root Issue**: Fixed bug where `treeFromObject` was creating an automatic root node wrapper even for already-isolated subtrees during INSERT operations. This caused a double-root structure (auto-root wrapping user-root, both with id='root') that prevented proper tree navigation. The fix adds a `skipRootCreation` parameter to `treeFromObject` call in `db.ts` line 1365, which is set to `true` to prevent the extra wrapper when inserting tree data.
  - Impact: Tree INSERT operations now work correctly without requiring `isolate()` calls
  - Tests: All 361 tests passing, including previously failing "insert on tree simple branch" and "insert new child on branch"

## [0.0.1]

Initial commit.
