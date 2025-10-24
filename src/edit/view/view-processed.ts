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

import { RowFilterProcessor } from '../filter/row-filter-processor.ts';
import { exampleEmptyRowFilter, RowFilter } from '../filter/row-filter.ts';
import { ColumnSelection } from '../selection/column-selection.ts';
import { RowSort } from '../sort/row-sort.ts';

import { ViewFiltered } from './view-filtered.ts';
import { ViewSelected } from './view-selected.ts';
import { ViewSorted } from './view-sorted.ts';
import { ViewWithData } from './view-with-data.ts';
import { View } from './view.ts';

/**
 * Provides selected and filtered, sorted versions of an master view
 *
 * Each time the master view, column selection, filter or sort changes,
 * the resulting view is updated.
 */
export class ViewProcessed {
  /**
   * Constructor
   * @param masterView$ The master view providing the basic data for the view
   * @param columnSelection$ The columns to be selected from the master view
   * @param filter$ The filter to apply to the view
   * @param sort$ The sort order to apply to the view
   * @param onDispose Handler called when processed view is disposed
   * @param errorHandler Handler called when errors happen
   */
  constructor(
    public readonly masterView$: Observable<View>,
    public readonly columnSelection$: Observable<ColumnSelection>,
    public readonly filter$: Observable<RowFilter>,
    public readonly sort$: Observable<RowSort>,
    public readonly onDispose:
      | ((view: ViewProcessed) => void)
      | undefined = undefined,

    public readonly errorHandler = (e: any) => console.error(e),
  ) {
    this._view$ = this._initView(masterView$, columnSelection$, filter$, sort$);
  }

  /**
   * Call this method when the mixer is to be disposed.
   * Will trigger a call of onDispose
   */
  dispose() {
    this.onDispose?.call(this, this);
  }

  /**
   * The view resulting from applying the column selection,
   * filter and sort to the master view.
   */
  get view(): Observable<View> {
    return this._view$;
  }

  /**
   * An example processed view with example data
   * @param errorHandler Handler called when errors happen
   */
  static example(
    errorHandler: undefined | ((error: any) => void) = undefined,
  ): ViewProcessed {
    const view = ViewWithData.example();
    const masterView = new BehaviorSubject<View>(view);
    const columnSelection = new BehaviorSubject<ColumnSelection>(
      view.columnSelection,
    );
    const filter = new BehaviorSubject<RowFilter>(exampleEmptyRowFilter());
    const sort = new BehaviorSubject<RowSort>(RowSort.empty);

    return new ViewProcessed(
      masterView,
      columnSelection,
      filter,
      sort,
      undefined,
      errorHandler,
    );
  }

  // ######################
  // Private
  // ######################

  _view$: Observable<View>;

  _reportMissingColumnsTimeout: number | undefined = undefined;

  // ...........................................................................
  private _initView(
    masterView: Observable<View>,
    columnSelection: Observable<ColumnSelection>,
    rowFilter: Observable<RowFilter>,
    sort: Observable<RowSort>,
  ): Observable<View> {
    // First select the right columns from the master view
    const viewSelected = combineLatest([masterView, columnSelection]).pipe(
      filter(([view, selection]) => {
        const missingColumns = this._missingColumns(view, selection);
        if (missingColumns.length > 0) {
          // If the missing columns are not cleared within a second, report them
          this._reportMissingColumns(missingColumns);
          return false;
        }

        this._cancelReportMissingColumns();

        return true;
      }),
      map(([view, selection]) => new ViewSelected(selection, view)),
    );

    // Turn filter into filter processor
    const filterProcessor = rowFilter.pipe(
      map((f) => RowFilterProcessor.fromModel(f)),
    );

    // Filter rows
    const viewFiltered = combineLatest([viewSelected, filterProcessor]).pipe(
      map(([view, filter]) => new ViewFiltered(view, filter)),
    );

    // Sort rows
    const viewSorted = combineLatest([viewFiltered, sort]).pipe(
      map(([view, sort]) => new ViewSorted(view, sort)),
    );

    // Share the result
    const result = viewSorted.pipe(
      shareReplay({ bufferSize: 1, refCount: true }),
    );

    return result;
  }

  // ...........................................................................
  private _missingColumns(view: View, selection: ColumnSelection): string[] {
    return selection.addedColumns(view.columnSelection);
  }

  // ...........................................................................
  private _reportMissingColumns(missingColumns: string[]) {
    this._reportMissingColumnsTimeout = setTimeout(() => {
      this.errorHandler(
        'Warning: Could not apply column selection to master view: ' +
          'The following columns are missing: ' +
          missingColumns.join('\n'),
      );
    }, 300) as unknown as number;
  }

  // ...........................................................................
  private _cancelReportMissingColumns() {
    if (this._reportMissingColumnsTimeout !== undefined) {
      clearTimeout(this._reportMissingColumnsTimeout);
      this._reportMissingColumnsTimeout = undefined;
    }
  }
}
