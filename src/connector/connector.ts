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
export type ConnectorCallback = (
  ref: string,
  predecessorRefs?: string[],
) => Promise<any>;

export class Connector {
  private _origin: string;
  private _callbacks: ConnectorCallback[] = [];
  private _conflictCallbacks: ConflictCallback[] = [];
  private _missedRef: string | null = null;
  private _missedPredecessorRefs: string[] | undefined = undefined;
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
      this._missedRef = null;
      this._missedPredecessorRefs = undefined;
      /* v8 ignore next -- @preserve */
      Promise.resolve(
        predecessorRefs ? callback(ref, predecessorRefs) : callback(ref),
      ).catch(console.error);
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

  private _notifyCallbacks(ref: string, predecessorRefs?: string[]) {
    if (this._callbacks.length === 0) {
      // No callbacks registered yet — store for replay on first listen()
      this._missedRef = ref;
      this._missedPredecessorRefs = predecessorRefs;
      return;
    }
    /* v8 ignore next -- @preserve */
    Promise.all(
      this._callbacks.map((cb) =>
        predecessorRefs ? cb(ref, predecessorRefs) : cb(ref),
      ),
    ).catch((err) => {
      console.error(`Error notifying connector callbacks for ref ${ref}:`, err);
    });
  }

  private _processIncoming(payload: ConnectorPayload) {
    const ref = payload.r;
    /* v8 ignore next -- @preserve */
    if (this._hasReceivedRef(ref)) {
      return;
    }

    // Gap detection
    if (this._syncConfig?.causalOrdering && payload.seq != null && payload.c) {
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

    this._addReceivedRef(ref);
    // `payload.p` carries the sender's predecessor content refs (shared
    // identity) so the receiver can record correct local ancestry.
    this._notifyCallbacks(ref, payload.p);

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
      this._processIncoming(p);
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
