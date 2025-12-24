// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Socket } from '@rljson/io';
import { Route, timeId } from '@rljson/rljson';

import { Db } from '../db.ts';

export type ConnectorPayload = { o: string; r: string };
export type ConnectorCallback = (ref: string) => Promise<any>;

export class Connector {
  private _origin: string;
  private _callbacks: ConnectorCallback[] = [];

  private _isListening: boolean = false;

  private _sentRefs: Set<string> = new Set();
  private _receivedRefs: Set<string> = new Set();

  constructor(
    private readonly _db: Db,
    private readonly _route: Route,
    private readonly _socket: Socket,
  ) {
    this._origin = timeId();

    this._init();
  }

  send(ref: string) {
    if (this._sentRefs.has(ref) || this._receivedRefs.has(ref)) return;

    this._sentRefs.add(ref);

    this.socket.emit(this.route.flat, {
      o: this._origin,
      r: ref,
    } as ConnectorPayload);
  }

  listen(callback: (editHistoryRef: string) => Promise<void>) {
    this._socket.on(this._route.flat, async (payload: ConnectorPayload) => {
      /* v8 ignore next -- @preserve */
      try {
        await callback(payload.r);
      } catch (error) {
        console.error('Error in connector listener callback:', error);
      }
    });
  }

  private _init() {
    this._registerSocketObserver();
    this._registerDbObserver();

    this._isListening = true;
  }

  public teardown() {
    this._socket.removeAllListeners(this._route.flat);
    this._db.unregisterAllObservers(this._route);

    this._isListening = false;
  }

  private _notifyCallbacks(ref: string) {
    /* v8 ignore next -- @preserve */
    Promise.all(this._callbacks.map((cb) => cb(ref))).catch((err) => {
      console.error(`Error notifying connector callbacks for ref ${ref}:`, err);
    });
  }

  private _registerSocketObserver() {
    this.socket.on(this.route.flat, (p: ConnectorPayload) => {
      if (p.o === this._origin) {
        return;
      }

      const ref = p.r;
      /* v8 ignore next -- @preserve */
      if (this._receivedRefs.has(ref)) {
        return;
      }

      this._receivedRefs.add(p.r);

      this._notifyCallbacks(p.r);
    });
  }

  private _registerDbObserver() {
    this._db.registerObserver(this._route, (ins) => {
      return new Promise<void>((resolve) => {
        const ref = (ins as any)[this.route.root.tableKey + 'Ref'] as string;
        /* v8 ignore next -- @preserve */
        if (this._sentRefs.has(ref)) {
          resolve();
          return;
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
