// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Socket } from '@rljson/io';
import {
  AckPayload,
  ClientId,
  clientId as generateClientId,
  ConnectorPayload,
  GapFillRequest,
  GapFillResponse,
  InsertHistoryTimeId,
  Route,
  SyncConfig,
  SyncEventNames,
  syncEvents,
  timeId,
} from '@rljson/rljson';

import { Db } from '../db.ts';

export type { ConnectorPayload } from '@rljson/rljson';
export type ConnectorCallback = (ref: string) => Promise<any>;

export class Connector {
  private _origin: string;
  private _callbacks: ConnectorCallback[] = [];

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
  private _lastPredecessors: InsertHistoryTimeId[] = [];
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
   * Sets the causal predecessors for the next send.
   * @param predecessors - The InsertHistory timeIds of causal predecessors
   */
  setPredecessors(predecessors: InsertHistoryTimeId[]) {
    this._lastPredecessors = predecessors;
  }

  // ...........................................................................
  /**
   * Registers a listener for incoming refs on this route.
   * The callback receives the raw ref string (not the full payload).
   *
   * **⚠️ Bypasses dedup, origin filtering, and gap detection.**
   * Prefer {@link onIncomingRef} for safe, deduplicated delivery.
   *
   * @param callback - The callback to invoke with each incoming ref
   */
  listen(callback: (editHistoryRef: string) => Promise<void>) {
    this._socket.on(this._events.ref, async (payload: ConnectorPayload) => {
      /* v8 ignore next -- @preserve */
      try {
        await callback(payload.r);
      } catch (error) {
        console.error('Error in connector listener callback:', error);
      }
    });
  }

  // ...........................................................................
  /**
   * Registers a callback for incoming refs that are processed through the
   * full sync pipeline: origin filtering, dedup, gap detection, and ACK.
   *
   * This is the recommended way to receive incoming refs. Unlike
   * {@link listen}, this method benefits from all sync protocol protections.
   *
   * @param callback - The callback to invoke with each deduplicated incoming ref
   */
  onIncomingRef(callback: ConnectorCallback) {
    this._callbacks.push(callback);
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
    this._registerDbObserver();

    if (this._syncConfig?.causalOrdering) {
      this._registerGapFillHandler();
    }

    this._isListening = true;
  }

  public teardown() {
    this._socket.removeAllListeners(this._events.ref);

    if (this._syncConfig?.causalOrdering) {
      this._socket.removeAllListeners(this._events.gapFillRes);
    }

    if (this._syncConfig?.requireAck) {
      this._socket.removeAllListeners(this._events.ack);
    }

    this._db.unregisterAllObservers(this._route);

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

  private _notifyCallbacks(ref: string) {
    /* v8 ignore next -- @preserve */
    Promise.all(this._callbacks.map((cb) => cb(ref))).catch((err) => {
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
    this._notifyCallbacks(ref);

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

  private _registerDbObserver() {
    this._db.registerObserver(this._route, (ins) => {
      return new Promise<void>((resolve) => {
        const ref = (ins as any)[this.route.root.tableKey + 'Ref'] as string;
        /* v8 ignore next -- @preserve */
        if (this._hasSentRef(ref)) {
          resolve();
          return;
        }

        // Auto-populate predecessors from InsertHistoryRow
        if (this._syncConfig?.causalOrdering && ins.previous?.length) {
          this._lastPredecessors = [...ins.previous];
        }

        this.send(ref);
        resolve();
      });
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
}
