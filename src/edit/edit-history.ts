// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Json } from '@rljson/json';

export interface EditHistory extends Json {
  timeId: string;
  multiEditRef: string;
  dataRef: string;
  previous: string[];
  _hash: string;
}
