// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { hsh } from '@rljson/hash';

import { describe, expect, it } from 'vitest';

import { exampleEditAction } from '../../src/edit/edit/edit-action.ts';

describe('EditAction', function () {
  it('exampleEditAction', function () {
    expect(exampleEditAction()).toEqual(
      hsh({
        route: 'carGeneral/doors',
        setValue: 5,
      }),
    );
  });
});
