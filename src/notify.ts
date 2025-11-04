// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { InsertHistoryRow, Route } from '@rljson/rljson';

type NotifyCallback<N extends string> = (
  InsertHistoryRow: InsertHistoryRow<N>,
) => void;

// ...........................................................................
/**
 * Notification system to manage callbacks for specific routes and notify them with edit protocol rows.
 */
export class Notify {
  private _callbacks: Map<string, NotifyCallback<any>[]> = new Map();

  // ...........................................................................
  constructor() {}

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
  }

  // ...........................................................................
  /**
   * Unregisters a callback for a specific route.
   * @param route   The route to unregister the callback from.
   * @param callback  The callback function to be removed.
   */
  unregister(route: Route, callback: NotifyCallback<any>) {
    const callbacks = this._callbacks.get(route.flat);
    if (callbacks) {
      this._callbacks.set(
        route.flat,
        callbacks.filter((cb) => cb !== callback),
      );
    }
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
    const callbacks = this._callbacks.get(route.flat);
    if (callbacks) {
      for (const cb of callbacks) {
        cb(insertHistoryRow);
      }
    }
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
