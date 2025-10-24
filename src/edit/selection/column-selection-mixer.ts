// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { BehaviorSubject, combineLatest, map, Observable } from 'rxjs';

import { MultiEditColumnEstimator } from '../edit/multi-edit-column-estimator.ts';

import { ColumnSelection } from './column-selection.ts';

/**
 * Mixes column selections from multi edits and column selections from views
 * into one master column selection.
 */
export class ColumnSelectionMixer {
  /**
   * Constructor
   * @param viewColumnSelections$ The column selections from all views
   * @param multiEditColumnSelection$  The column selections from all multi edits
   */
  constructor(
    public viewColumnSelections$: Observable<ColumnSelection[]>,
    public multiEditColumnSelection$: Observable<ColumnSelection>,
  ) {
    this._columnSelection$ = this._initColumnSelection();
  }

  /**
   * The resulting master column selection
   */
  get columnSelection$(): Observable<ColumnSelection> {
    return this._columnSelection$;
  }

  /**
   * Example instance for testing
   */
  static get example(): ColumnSelectionMixer {
    const viewColumnSelections = new BehaviorSubject<ColumnSelection[]>([
      ColumnSelection.example(),

      new ColumnSelection([
        {
          key: 'doors',
          alias: 'intCol',
          route: 'carGeneral/doors',
          type: 'number',
          titleLong: 'Int values',
          titleShort: 'Ints',
        },
      ]),
    ]);

    const multiEditColumnSelection = new BehaviorSubject<ColumnSelection>(
      MultiEditColumnEstimator.example.columnSelection,
    );

    return new ColumnSelectionMixer(
      viewColumnSelections,
      multiEditColumnSelection,
    );
  }

  // ######################
  // Private
  // ######################
  private _columnSelection$: Observable<ColumnSelection>;

  private _initColumnSelection(): Observable<ColumnSelection> {
    // Merges all column selections into one master column selection
    return combineLatest([
      this.viewColumnSelections$,
      this.multiEditColumnSelection$,
    ]).pipe(
      map(([viewColumnSelections, multiEditColumnSelection]) => {
        const columnSelections = [
          ...viewColumnSelections,
          multiEditColumnSelection,
        ];
        return ColumnSelection.merge(columnSelections);
      }),
    );
  }
}
