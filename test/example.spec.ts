// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, it } from 'vitest';

import { example } from '../src/example';

import { checkGoldens } from './setup/goldens';

describe('example', () => {
  it('should run without error', async () => {
    // Prepare logging
    const backup = console.log;
    const messages: string[] = [];
    console.log = (m: string) => messages.push(m);

    // Run example
    await example();

    // Compare log with golden
    checkGoldens('test/goldens/example.log', messages.join('\n'));

    // Restore logging
    console.log = backup;
  });
});
