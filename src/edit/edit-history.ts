// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Json } from '@rljson/json';
import { ColumnCfg, TableCfg } from '@rljson/rljson';

export interface EditHistory extends Json {
  timeId: string;
  multiEditRef: string;
  dataRef: string;
  previous: string[];
  _hash: string;
}

export const createMultiEditHistoryTableCfg = (cakeKey: string): TableCfg =>
  ({
    key: `${cakeKey}EditHistory`,
    type: 'components',
    columns: [
      {
        key: '_hash',
        type: 'string',
        titleLong: 'Hash',
        titleShort: 'Hash',
      },
      {
        key: 'timeId',
        type: 'string',
        titleLong: 'Time Identifier',
        titleShort: 'Time ID',
      },
      {
        key: 'multiEditRef',
        type: 'string',
        titleLong: 'Multi Edit Reference',
        titleShort: 'Multi Edit Ref',
        ref: {
          tableKey: `${cakeKey}MultiEdit`,
        },
      },
      {
        key: 'dataRef',
        type: 'string',
        titleLong: 'Data Reference',
        titleShort: 'Data Ref',
        ref: {
          tableKey: cakeKey,
        },
      },
      {
        key: 'previous',
        type: 'jsonArray',
        titleLong: 'Previous Values',
        titleShort: 'Previous',
      },
    ] as ColumnCfg[],
  } as TableCfg);
