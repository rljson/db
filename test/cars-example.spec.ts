// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, it } from 'vitest';

import { carsExample } from '../src/cars-example';

import { expectGolden } from './setup/goldens';

describe('Cars Example', () => {
  it('should run without error', async () => {
    const ex = await carsExample();
    await expectGolden('test/goldens/cars-example.json').toBe(ex);
  });
});
