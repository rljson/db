// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import type {
  DbEvents,
  DbHookDecision,
  DbHookOptions,
  DbReadEvent,
  DbRowFilter,
  DbWriteEvent,
} from './db-events.ts';

// #############################################################################
/** What a {@link MockHook} decides */
export interface MockHookOptions extends DbHookOptions {
  /** Refuses every read with this reason */
  readonly refuseReads?: string;

  /** Refuses every write with this reason */
  readonly refuseWrites?: string;

  /** Keeps only the rows this filter keeps when reading */
  readonly filterReads?: DbRowFilter;

  /** Keeps only the rows this filter keeps when writing */
  readonly filterWrites?: DbRowFilter;
}

// #############################################################################
/**
 * A hook for tests: hangs itself before reads and writes, records every
 * event it sees and refuses or filters as configured. Without options it
 * allows everything and only records.
 */
export class MockHook {
  /**
   * Constructor
   * @param options - What the hook decides, see {@link MockHookOptions}
   */
  constructor(readonly options: MockHookOptions = {}) {}

  // ...........................................................................
  /**
   * Returns a mock hook attached to an event model
   * @param events - The event model
   * @param options - What the hook decides
   */
  static example(events: DbEvents, options: MockHookOptions = {}): MockHook {
    return new MockHook({ name: 'mockHook', ...options }).attach(events);
  }

  /** The read events the hook has seen */
  readonly reads: DbReadEvent[] = [];

  /** The write events the hook has seen */
  readonly writes: DbWriteEvent[] = [];

  // ...........................................................................
  /**
   * Hangs the hook into an event model
   * @param events - The event model
   * @returns The hook itself
   */
  attach(events: DbEvents): this {
    this.detach();
    const { refuseReads, refuseWrites, filterReads, filterWrites, ...hook } =
      this.options;

    this._detachers = [
      events.beforeRead((event) => {
        this.reads.push(event);
        return _decide(refuseReads, filterReads);
      }, hook),
      events.beforeWrite((event) => {
        this.writes.push(event);
        return _decide(refuseWrites, filterWrites);
      }, hook),
    ];
    return this;
  }

  // ...........................................................................
  /** Removes the hook from the event model it is attached to */
  detach(): void {
    for (const detach of this._detachers) detach();
    this._detachers = [];
  }

  private _detachers: (() => void)[] = [];
}

// .............................................................................
const _decide = (
  refuse: string | undefined,
  filter: DbRowFilter | undefined,
): DbHookDecision => {
  if (refuse !== undefined) return { refuse };
  if (filter) return { filter };
  return undefined;
};
