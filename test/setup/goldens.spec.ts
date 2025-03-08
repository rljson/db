// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, expect, it } from 'vitest';

import { updateGoldens } from './goldens';

describe('updateGoldens', () => {
  it('should be true when process.argv contains --update-goldens', () => {
    const backup = process.env['UPDATE_GOLDENS'];
    (process.env as any).UPDATE_GOLDENS = true;
    expect(updateGoldens()).toBe(true);
    (process.env as any).UPDATE_GOLDENS = false;
    expect(updateGoldens()).toBe(false);
    (process.env as any).UPDATE_GOLDENS = backup;
  });
});
