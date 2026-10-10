// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.
export { Connector, stateBeaconEvent } from './connector/connector.ts';
export type {
  ConnectorCallback,
  ConnectorPayload,
  RefArrivalInfo,
  StampCallback,
} from './connector/connector.ts';
export type {
  AckPayload,
  ClientId,
  Conflict,
  ConflictCallback,
  ConflictType,
  GapFillRequest,
  GapFillResponse,
  RefStamp,
  StampPayload,
  SyncConfig,
  SyncEventNames,
} from '@rljson/rljson';
export { Db } from './db.ts';
export {
  exampleEditActionColumnSelection,
  exampleEditActionColumnSelectionOnlySomeColumns,
  exampleEditActionPutComponent,
  exampleEditActionRowFilter,
  exampleEditActionRowSort,
  exampleEditActionSetValue,
  exampleEditSetValueReferenced,
} from './edit/edit-action.ts';
export type {
  EditActionColumnSelection,
  EditActionPutComponent,
  EditActionRowFilter,
  EditActionRowSort,
  EditActionSetValue,
} from './edit/edit-action.ts';
export type {
  EditColumnSelection,
  EditPutComponent,
  EditRowFilter,
  EditRowSort,
  EditSetValue,
} from './edit/edit.ts';
export { EditChainManager } from './edit/edit-chain-manager.ts';
export type {
  EditChainAppendOptions,
  EditChainEntry,
} from './edit/edit-chain-manager.ts';
export { MultiEditManager } from './edit/multi-edit-manager.ts';
export { staticExample } from './example-static/example-static.ts';
export type { BoolOperator } from './join/filter/boolean-filter-processor.ts';
export type { BooleanFilter } from './join/filter/boolean-filter.ts';
export type { ColumnFilter } from './join/filter/column-filter.ts';
export type { NumberOperator } from './join/filter/number-filter-processor.ts';
export type { NumberFilter } from './join/filter/number-filter.ts';
export { emptyRowFilter } from './join/filter/row-filter.ts';
export type { RowFilter } from './join/filter/row-filter.ts';
export type { StringOperator } from './join/filter/string-filter-processor.ts';
export type { StringFilter } from './join/filter/string-filter.ts';
export { Join } from './join/join.ts';
export type { JoinRow, JoinRows } from './join/join.ts';
export { ColumnSelection } from './join/selection/column-selection.ts';
export type {
  ColumnInfo,
  ColumnRoute,
} from './join/selection/column-selection.ts';
export type { SetValue } from './join/set-value/set-value.ts';
export { RowSort } from './join/sort/row-sort.ts';
export type { RowSortOrder, RowSortType } from './join/sort/row-sort.ts';
export { inject } from './tools/inject.ts';
export { isolate } from './tools/isolate.ts';
export { makeUnique } from './tools/make-unique.ts';
export { mergeTrees } from './tools/merge-trees.ts';
