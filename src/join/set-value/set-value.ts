// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { Json } from '@rljson/json';

/**
 * Describes an set value that is applied to a specific route.
 */
export interface SetValue extends Json {
  /**
   * The route to the column the value is set to.
   */
  route: string;

  /*
   * The value that is inserted into the column
   */
  value: any;
}
