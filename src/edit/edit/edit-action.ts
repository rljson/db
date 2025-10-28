// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hip } from '@rljson/hash';
import { Json } from '@rljson/json';

/**
 * Describes an action that is applied to a specific route in the json.
 */
export interface EditAction extends Json {
  /**
   * The route the edit is applied to.
   */
  route: string;

  /*
   * The value that is written into the column
   */
  setValue: any;

  /*
   * The hash of the action
   */
  _hash?: string;
}

// .............................................................................
/**
 * An edit action with the column index
 *
 */
export interface EditActionWithIndex extends EditAction {
  index: number;
}

// .............................................................................
export const exampleEditAction = (): EditAction => {
  return hip({
    route: 'carGeneral/doors',
    setValue: 5,
  });
};
