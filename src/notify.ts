// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { InsertHistoryRow, Route } from '@rljson/rljson';

import { DbEvents } from './events/db-events.ts';

export type NotifyCallback<N extends string> = (
  InsertHistoryRow: InsertHistoryRow<N>,
) => Promise<any>;

// ...........................................................................
/**
 * Notification system to manage callbacks for specific routes and notify them with edit protocol rows.
 *
 * The callbacks are insert observers of a {@link DbEvents}: `notify` starts
 * them in the order they were registered, does not wait for them and only
 * logs their errors.
 */
export class Notify {
  private _callbacks: Map<string, NotifyCallback<any>[]> = new Map();
  private _detachers: Map<string, (() => void)[]> = new Map();

  // ...........................................................................
  /**
   * Constructor
   * @param events - The event model the callbacks are registered with
   */
  constructor(readonly events: DbEvents = new DbEvents()) {}

  // ...........................................................................
  /**
   * Registers a callback for a specific route.
   * @param route   The route to register the callback for.
   * @param callback  The callback function to be invoked when a notification is sent.
   */
  register(route: Route, callback: NotifyCallback<any>) {
    this._callbacks.set(route.flat, [
      ...(this._callbacks.get(route.flat) || []),
      callback,
    ]);

    const detach = this.events.afterInsert(
      (event) => callback(event.insertHistoryRow),
      { route, name: callback.name },
    );
    this._detachers.set(route.flat, [
      ...(this._detachers.get(route.flat) || []),
      detach,
    ]);
  }

  // ...........................................................................
  /**
   * Unregisters a callback for a specific route.
   * @param route   The route to unregister the callback from.
   * @param callback  The callback function to be removed.
   */
  unregister(route: Route, callback: NotifyCallback<any>) {
    const callbacks = this._callbacks.get(route.flat);
    /*v8 ignore else -- @preserve */
    if (callbacks) {
      const detachers = this._detachers.get(route.flat)!;
      callbacks.forEach((cb, i) => {
        if (cb === callback) detachers[i]();
      });
      this._detachers.set(
        route.flat,
        detachers.filter((_, i) => callbacks[i] !== callback),
      );
      this._callbacks.set(
        route.flat,
        callbacks.filter((cb) => cb !== callback),
      );
    }
  }

  // ...........................................................................
  /**
   * Unregisters all callbacks for a specific route.
   * @param route   The route to unregister all callbacks from.
   */
  unregisterAll(route: Route) {
    for (const detach of this._detachers.get(route.flat) || []) detach();
    this._detachers.delete(route.flat);
    this._callbacks.delete(route.flat);
  }

  // ...........................................................................
  /**
   * Notifies all registered callbacks for a specific route with the provided edit protocol row.
   * @param route   The route to notify callbacks for.
   * @param insertHistoryRow  The edit protocol row to pass to the callbacks.
   */
  notify<N extends string>(
    route: Route,
    insertHistoryRow: InsertHistoryRow<N>,
  ) {
    this.events.emitInsert(route, insertHistoryRow);
  }

  // ...........................................................................
  /**
   * Returns the current map of registered callbacks.
   * @returns A map where keys are route strings and values are arrays of callback functions.
   */
  get callbacks() {
    return this._callbacks;
  }

  // ...........................................................................
  /**
   * Retrieves the list of callbacks registered for a specific route.
   * @param route   The route to get callbacks for.
   * @returns An array of callback functions registered for the specified route.
   */
  getCallBacksForRoute(route: Route) {
    return this._callbacks.get(route.flat) || [];
  }
}
