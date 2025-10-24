// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import {
  BehaviorSubject,
  combineLatest,
  filter,
  map,
  Observable,
  shareReplay,
} from 'rxjs';

import { ViewCache } from '../view/view-cache.ts';
import { ViewEditedMultiple } from '../view/view-edited-multiple.ts';
import { ViewWithData } from '../view/view-with-data.ts';
import { View } from '../view/view.ts';

import { MultiEditColumnEstimator } from './multi-edit-column-estimator.ts';
import { MultiEditResolved } from './multi-edit-resolved.ts';

/**
 * Takes a master view observable, a multi edit observable and creates
 * a new observable that delivers the master view with the multi edit applied.
 */
export class MultiEditMixer {
  constructor(
    public readonly masterView$: Observable<View>,
    public readonly multiEdit$: Observable<MultiEditResolved>,
    public readonly cacheSize = 5,
  ) {
    this.editedView$ = this._initEditedView();
    this._cache = new ViewCache(this.cacheSize);
  }

  /**
   * The edited view.
   * Changes each time the master view or the multi edit changes.
   */
  editedView$: Observable<View>;

  static get example(): MultiEditMixer {
    return new MultiEditMixer(
      new BehaviorSubject<View>(ViewWithData.example()),
      new BehaviorSubject<MultiEditResolved>(MultiEditResolved.empty),
    );
  }

  // ######################
  // Private
  // ######################

  private _cache: ViewCache;

  private _initEditedView(): Observable<View> {
    return combineLatest([this.masterView$, this.multiEdit$]).pipe(
      // Do not process when master view has missing columns
      filter(([view, multiEdit]) => {
        return !this._hasMissingColumns(view, multiEdit);
      }),

      // Whenever the master view or the multi edit changes,
      // apply the edits to the master view and deliver the result.
      map(([masterView, multiEdit]) => {
        const result = new ViewEditedMultiple(
          masterView,
          multiEdit,
          this._cache,
        );
        return result;
      }),

      // Always share the last value with new subscribers
      shareReplay({ bufferSize: 1, refCount: true }),
    );
  }

  // ...........................................................................
  private _hasMissingColumns(
    view: View,
    multiEdit: MultiEditResolved,
  ): boolean {
    const requiredColumns = new MultiEditColumnEstimator(multiEdit)
      .columnSelection;
    const availableColumns = view.columnSelection;
    const difference = requiredColumns.addedColumns(availableColumns);
    return difference.length > 0;
  }
}
