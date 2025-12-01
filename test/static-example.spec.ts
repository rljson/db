// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, it } from 'vitest';

import { staticExample } from '../src/example-static/example-static';

import { expectGolden } from './setup/goldens';

describe('Static Example', () => {
  it('should run without error', async () => {
    const ex = await staticExample();
    await expectGolden('test/goldens/static-example.json').toBe(ex);
  });
});
