// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Json } from '@rljson/json';
import { TableCfg } from '@rljson/rljson';

export interface MultiEdit extends Json {
  previous: string | null;
  edit: string;
  _hash: string;
}

export const createMultiEditTableCfg = (cakeKey: string): TableCfg =>
  ({
    key: `${cakeKey}MultiEdits`,
    type: 'components',
    columns: [
      {
        key: '_hash',
        type: 'string',
        titleLong: 'Hash',
        titleShort: 'Hash',
      },
      {
        key: 'previous',
        type: 'string',
        titleLong: 'Previous Value',
        titleShort: 'Previous',
      },
      {
        key: 'edit',
        type: 'string',
        titleLong: 'Edit Reference',
        titleShort: 'Edit Ref',
        ref: {
          tableKey: `${cakeKey}Edits`,
        },
      },
    ] as TableCfg['columns'],
  } as TableCfg);
