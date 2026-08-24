// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Socket } from '@rljson/io';
import {
  AckPayload,
  ClientId,
  Conflict,
  ConflictCallback,
  ConnectorPayload,
  GapFillRequest,
  GapFillResponse,
  clientId as generateClientId,
  Route,
  SyncConfig,
  SyncEventNames,
  syncEvents,
  timeId,
} from '@rljson/rljson';

import { Db } from '../db.ts';

export type { ConnectorPayload } from '@rljson/rljson';
/**
 * Invoked for each deduplicated incoming ref. `predecessorRefs` carries the
 * causal predecessor *content refs* (shared identity across clients) when
 * `causalOrdering` is enabled, so the receiver can record correct ancestry in
 * its own InsertHistory. Empty/undefined for roots or when causal ordering is off.
 */
/**
 * Whether this advertisement is the newest thing its sender has said.
 *
 * A ref is a content hash: it identifies a STATE, and the same state can
 * legitimately recur, so nothing in the ref answers "is this news?". The
 * per-sender sequence does, and cheaply — without walking the ancestry DAG,
 * merging anything, or trusting the sender's view of the world.
 *
 * `false` means this sender has already advertised something later; the
 * payload is a re-advertisement or a straggler. It does NOT mean the content
 * is wrong, only that acting destructively on it is unsafe.
 *
 * `true` when the sequence is unavailable (`causalOrdering` or
 * `includeClientIdentity` off), because "unknown" must not silently become
 * "stale" and change behaviour for deployments carrying no metadata.
 */
export interface RefArrivalInfo {
  /** Predecessor content refs the sender declared, when causal ordering is on. */
  predecessorRefs?: string[];
  /** See {@link RefArrivalInfo}. `true` when it cannot be determined. */
  isNewestFromSender: boolean;
}

export type ConnectorCallback = (
  ref: string,
  predecessorRefs?: string[],
  info?: RefArrivalInfo,
) => Promise<any>;

export class Connector {
  private _origin: string;
  private _callbacks: ConnectorCallback[] = [];
  private _conflictCallbacks: ConflictCallback[] = [];
  private _missedRef: string | null = null;
  private _missedPredecessorRefs: string[] | undefined = undefined;
  private _missedInfo: RefArrivalInfo | undefined = undefined;
  private _lastSentRef: string | null = null;

  private _isListening: boolean = false;

  // Two-generation dedup sets — bounded memory
  private _sentRefsCurrent: Set<string> = new Set();
  private _sentRefsPrevious: Set<string> = new Set();
  private _receivedRefsCurrent: Set<string> = new Set();
  private _receivedRefsPrevious: Set<string> = new Set();
  private readonly _maxDedup: number;

  // Sync protocol state
  private readonly _syncConfig: SyncConfig | undefined;
  private readonly _clientId: ClientId | undefined;
  private readonly _events: SyncEventNames;
  private _seq: number = 0;
  // Predecessor *content refs* (not timeIds) attached to the next send. Refs are
  // the only identity shared across clients, so the receiver can map them to its
  // own local ancestry. Auto-populated from the InsertHistoryRow on db inserts.
  private _lastPredecessors: string[] = [];
  private _peerSeqs: Map<ClientId, number> = new Map();

  constructor(
    private readonly _db: Db,
    private readonly _route: Route,
    private readonly _socket: Socket,
    syncConfig?: SyncConfig,
    clientIdentity?: ClientId,
  ) {
    this._origin = timeId();
    this._syncConfig = syncConfig;
    this._events = syncEvents(this._route.flat);

    // Resolve client identity
    if (clientIdentity) {
      this._clientId = clientIdentity;
    } else if (syncConfig?.includeClientIdentity) {
      this._clientId = generateClientId();
    }

    this._maxDedup = syncConfig?.maxDedupSetSize ?? 10_000;

    this._init();
  }

  // ...........................................................................
  /**
   * Sends a ref to the server via the socket.
   * Enriches the payload based on SyncConfig flags.
   * @param ref - The ref to send
   */
  send(ref: string) {
    if (this._hasSentRef(ref) || this._hasReceivedRef(ref)) return;

    this._addSentRef(ref);

    // Do NOT clear _missedRef here. The bootstrap ref must survive
    // until listen() is called so the callback receives the server's
    // latest state. Previously, clearing _missedRef caused a race:
    // syncToDb's send() would discard the bootstrap, and the
    // subsequent listen() in syncFromDb would get nothing — leaving
    // the client permanently stuck.

    const payload: ConnectorPayload = {
      o: this._origin,
      r: ref,
    };

    if (this._syncConfig?.includeClientIdentity && this._clientId) {
      payload.c = this._clientId;
      payload.t = Date.now();
    }

    if (this._syncConfig?.causalOrdering) {
      payload.seq = ++this._seq;
      if (this._lastPredecessors.length > 0) {
        payload.p = [...this._lastPredecessors];
      }
    }

    this._lastSentRef = ref;
    this.socket.emit(this._events.ref, payload);
  }

  // ...........................................................................
  /**
   * Sends a ref and waits for server acknowledgment.
   * Only meaningful when `syncConfig.requireAck` is `true`.
   * @param ref - The ref to send
   * @returns A promise that resolves with the AckPayload
   */
  async sendWithAck(ref: string): Promise<AckPayload> {
    const timeoutMs = this._syncConfig?.ackTimeoutMs ?? 10_000;

    return new Promise<AckPayload>((resolve, reject) => {
      const timeout = setTimeout(() => {
        this._socket.off(this._events.ack, handler);
        reject(new Error(`ACK timeout for ref ${ref} after ${timeoutMs}ms`));
      }, timeoutMs);

      const handler = (ack: AckPayload) => {
        if (ack.r === ref) {
          clearTimeout(timeout);
          this._socket.off(this._events.ack, handler);
          resolve(ack);
        }
      };

      // Register listener BEFORE send so synchronous ACK is not lost
      this._socket.on(this._events.ack, handler);
      this.send(ref);
    });
  }

  // ...........................................................................
  /**
   * Sets the causal predecessors (content refs) attached to the next send.
   * Normally auto-populated from the InsertHistoryRow; exposed for tests/manual
   * control.
   * @param predecessors - The predecessor content refs
   */
  setPredecessors(predecessors: string[]) {
    this._lastPredecessors = predecessors;
  }

  // ...........................................................................
  /**
   * Registers a callback for incoming refs on this route.
   *
   * Incoming refs are processed through the full sync pipeline:
   * origin filtering, dedup, gap detection, and ACK.
   *
   * @param callback - The callback to invoke with each deduplicated incoming ref
   */
  listen(callback: ConnectorCallback) {
    this._callbacks.push(callback);

    // Replay ref that arrived before any callback was registered.
    // This fixes the bootstrap race: the server sends the latest ref
    // immediately on connect, but the Connector may be constructed
    // before listen() is called. Without replay, that initial ref is
    // added to the dedup set (so it won't fire again) but no callback
    // ever sees it.
    if (this._missedRef !== null) {
      const ref = this._missedRef;
      const predecessorRefs = this._missedPredecessorRefs;
      const info = this._missedInfo;
      this._missedRef = null;
      this._missedPredecessorRefs = undefined;
      this._missedInfo = undefined;
      /* v8 ignore next -- @preserve */
      Promise.resolve(callback(ref, predecessorRefs, info)).catch(
        console.error,
      );
    }
  }

  // ...........................................................................
  /**
   * Registers a callback that fires when a DAG conflict is detected.
   *
   * A conflict occurs when the InsertHistory for this route's table
   * has multiple "tips" (leaf nodes), indicating concurrent writes
   * from different clients that have not yet been merged.
   *
   * Detection-only: the callback receives a `Conflict` object
   * describing the branches. Resolution is left to upper layers.
   * @param callback - Invoked with the detected Conflict
   */
  onConflict(callback: ConflictCallback) {
    this._conflictCallbacks.push(callback);
  }

  // ...........................................................................
  /**
   * Removes a ref from the received-dedup set so a later re-advertisement of
   * the same ref (e.g. the server's bootstrap heartbeat, which only re-sends
   * the *latest* ref) is delivered to listeners again instead of being
   * silently deduplicated.
   *
   * A ref is added to the dedup set the instant it is *received* — before the
   * listener callback (which materializes it) has a chance to succeed. If that
   * apply step fails terminally, the ref is stuck in the dedup set forever and
   * the heartbeat can never heal it. Consumers whose apply failed call this so
   * recovery becomes eventually-consistent rather than permanent loss.
   * @param ref - The ref to allow re-delivery for.
   */
  invalidateReceived(ref: string): void {
    this._receivedRefsCurrent.delete(ref);
    this._receivedRefsPrevious.delete(ref);
  }

  // ...........................................................................
  /**
   * Removes a ref from BOTH dedup sets, so this connector may send it again.
   *
   * {@link send} drops a ref it has already sent OR already received, which
   * assumes a ref describes a state reached once and never returned to. That
   * holds for an append-only stream and fails for content-addressed state: a
   * folder that goes A → B → A re-derives A's exact ref, and the return trip
   * is discarded as a duplicate. A file created and then deleted within one
   * session is precisely that shape, and its deletion reached no peer at all.
   *
   * Callers use this when they move OFF a ref: the ref no longer describes
   * their state, so a later return to it is real news rather than an echo.
   * Refs that still describe the current state stay deduped, so ordinary
   * bounce-back suppression is untouched.
   * @param ref - The ref to allow re-sending for.
   */
  invalidateSent(ref: string): void {
    this._sentRefsCurrent.delete(ref);
    this._sentRefsPrevious.delete(ref);
    this.invalidateReceived(ref);
  }

  // ...........................................................................
  /**
   * Returns the current sequence number.
   * Only meaningful when `causalOrdering` is enabled.
   */
  get seq(): number {
    return this._seq;
  }

  // ...........................................................................
  /**
   * Returns the stable client identity.
   * Only available when `includeClientIdentity` is enabled.
   */
  get clientIdentity(): ClientId | undefined {
    return this._clientId;
  }

  // ...........................................................................
  /**
   * Returns the sync configuration, if any.
   */
  get syncConfig(): SyncConfig | undefined {
    return this._syncConfig;
  }

  // ...........................................................................
  /**
   * Returns the typed event names for this connector's route.
   */
  get events(): SyncEventNames {
    return this._events;
  }

  // ######################
  // Private
  // ######################

  private _init() {
    this._registerSocketObserver();
    this._registerBootstrapHandler();
    this._registerDbObserver();
    this._registerConflictObserver();

    if (this._syncConfig?.causalOrdering) {
      this._registerGapFillHandler();
    }

    this._isListening = true;
  }

  public tearDown() {
    this._socket.removeAllListeners(this._events.ref);
    this._socket.removeAllListeners(this._events.bootstrap);

    if (this._syncConfig?.causalOrdering) {
      this._socket.removeAllListeners(this._events.gapFillRes);
    }

    if (this._syncConfig?.requireAck) {
      this._socket.removeAllListeners(this._events.ack);
    }

    this._db.unregisterAllObservers(this._route);
    this._db.unregisterAllConflictObservers(this._route);

    this._isListening = false;
  }

  // ...........................................................................
  // Two-generation dedup helpers
  // ...........................................................................

  private _hasSentRef(ref: string): boolean {
    return this._sentRefsCurrent.has(ref) || this._sentRefsPrevious.has(ref);
  }

  private _addSentRef(ref: string): void {
    this._sentRefsCurrent.add(ref);
    if (this._sentRefsCurrent.size >= this._maxDedup) {
      this._sentRefsPrevious = this._sentRefsCurrent;
      this._sentRefsCurrent = new Set();
    }
  }

  private _hasReceivedRef(ref: string): boolean {
    return (
      this._receivedRefsCurrent.has(ref) || this._receivedRefsPrevious.has(ref)
    );
  }

  private _addReceivedRef(ref: string): void {
    this._receivedRefsCurrent.add(ref);
    if (this._receivedRefsCurrent.size >= this._maxDedup) {
      this._receivedRefsPrevious = this._receivedRefsCurrent;
      this._receivedRefsCurrent = new Set();
    }
  }

  private _notifyCallbacks(
    ref: string,
    predecessorRefs?: string[],
    info?: RefArrivalInfo,
  ) {
    if (this._callbacks.length === 0) {
      // No callbacks registered yet — store for replay on first listen().
      //
      // The slot holds ONE ref, so a second arrival before listen() replaces
      // the first — deliberately, since the newer ref describes the newer
      // state and replaying a superseded one would regress it. But the
      // replaced ref was already marked received the instant it arrived, and
      // nothing ever un-marks it: it is neither delivered nor retired, so it
      // stays "already received" for the life of the connector.
      //
      // Refs are content hashes, so that is not merely a lost notification —
      // it silently blocks the state itself. A peer that later puts the data
      // back into exactly that state re-derives that exact ref, and the
      // advertisement is dropped here before any listener sees it. Deleting a
      // file created earlier in the session is precisely that shape, and it
      // is why a THIRD peer changes the outcome: with two connectors only one
      // bootstrap ref lands in this window, with three there is a second one
      // to replace it.
      //
      // Hand the replaced ref back, so a later return to its state is real
      // news again. Its own delivery is still (correctly) skipped.
      if (this._missedRef !== null && this._missedRef !== ref) {
        this.invalidateReceived(this._missedRef);
      }
      this._missedRef = ref;
      this._missedPredecessorRefs = predecessorRefs;
      this._missedInfo = info;
      return;
    }
    /* v8 ignore next -- @preserve */
    Promise.all(
      this._callbacks.map((cb) => cb(ref, predecessorRefs, info)),
    ).catch((err) => {
      console.error(`Error notifying connector callbacks for ref ${ref}:`, err);
    });
  }

  /**
   * Handles a ref arriving from the wire.
   * @param payload - The incoming payload.
   * @param fromBootstrap - Whether it arrived on the bootstrap channel. A
   *   bootstrap is the server ANNOUNCING current state, not a link in a
   *   per-sender chain, so gap detection is skipped for it — a late joiner
   *   would otherwise see its first announcement as a gap and ask the server
   *   to replay history that it never missed. Staleness is still computed:
   *   that is the whole point of the sequence being there.
   */
  private _processIncoming(payload: ConnectorPayload, fromBootstrap = false) {
    const ref = payload.r;
    /* v8 ignore next -- @preserve */
    if (this._hasReceivedRef(ref)) {
      return;
    }

    // Gap detection
    // Is this the newest thing this sender has said? Answered here because
    // this is where the per-sender sequence already lives — the same number
    // gap detection keys on. Unknown (no sequence, no sender id) answers
    // `true`: "unknown" must not silently become "stale".
    let isNewestFromSender = true;
    if (this._syncConfig?.causalOrdering && payload.seq != null && payload.c) {
      isNewestFromSender = payload.seq > (this._peerSeqs.get(payload.c) ?? 0);
    }

    if (
      !fromBootstrap &&
      this._syncConfig?.causalOrdering &&
      payload.seq != null &&
      payload.c
    ) {
      const lastSeq = this._peerSeqs.get(payload.c) ?? 0;
      if (payload.seq > lastSeq + 1) {
        // Gap detected — request fill
        const gapReq: GapFillRequest = {
          route: this._route.flat,
          afterSeq: lastSeq,
        };
        this._socket.emit(this._events.gapFillReq, gapReq);
      }
      this._peerSeqs.set(payload.c, payload.seq);
    }

    // A bootstrap skips gap detection but must still move the high-water mark,
    // or every repeat of the same announcement would read as new again.
    if (fromBootstrap && payload.seq != null && payload.c) {
      const lastSeq = this._peerSeqs.get(payload.c) ?? 0;
      if (payload.seq > lastSeq) this._peerSeqs.set(payload.c, payload.seq);
    }

    this._addReceivedRef(ref);
    // `payload.p` carries the sender's predecessor content refs (shared
    // identity) so the receiver can record correct local ancestry.
    this._notifyCallbacks(ref, payload.p, {
      predecessorRefs: payload.p,
      isNewestFromSender,
    });

    // Send individual client ACK if required
    if (this._syncConfig?.requireAck) {
      this._socket.emit(this._events.ackClient, { r: ref });
    }
  }

  private _registerSocketObserver() {
    this.socket.on(this._events.ref, (p: ConnectorPayload) => {
      if (p.o === this._origin) {
        return;
      }

      this._processIncoming(p);
    });
  }

  private _registerGapFillHandler() {
    this._socket.on(this._events.gapFillRes, (res: GapFillResponse) => {
      for (const p of res.refs) {
        this._processIncoming(p);
      }
    });
  }

  /**
   * Listens for bootstrap messages from the server.
   * The server sends the latest ref on connect and optionally via heartbeat.
   * _processIncoming handles dedup so already-seen refs are filtered out.
   */
  private _registerBootstrapHandler() {
    this._socket.on(this._events.bootstrap, (p: ConnectorPayload) => {
      this._processIncoming(p, true);
    });
  }

  private _registerConflictObserver() {
    this._db.registerConflictObserver(this._route, (conflict: Conflict) => {
      for (const cb of this._conflictCallbacks) {
        cb(conflict);
      }
    });
  }

  private _registerDbObserver() {
    this._db.registerObserver(this._route, async (ins) => {
      const tableKey = this.route.root.tableKey;
      const ref = (ins as any)[tableKey + 'Ref'] as string;
      /* v8 ignore next -- @preserve */
      if (this._hasSentRef(ref)) {
        return;
      }

      // Auto-populate predecessors from the InsertHistoryRow, translating each
      // local predecessor timeId into its shared *content ref*. timeIds are
      // per-db (not shared across clients); the content ref is the only stable
      // cross-client identity, so the wire carries refs.
      if (this._syncConfig?.causalOrdering && ins.previous?.length) {
        const refs: string[] = [];
        for (const timeId of ins.previous) {
          const predRef = await this._db.getRefOfTimeId(tableKey, timeId);
          /* v8 ignore next -- @preserve a stored predecessor always has a ref */
          if (predRef) {
            refs.push(predRef);
          }
        }
        this._lastPredecessors = refs;
      }

      this.send(ref);
    });
  }

  get socket() {
    return this._socket;
  }

  get route() {
    return this._route;
  }

  get origin() {
    return this._origin;
  }

  get isListening() {
    return this._isListening;
  }

  get lastSentRef(): string | null {
    return this._lastSentRef;
  }
}
