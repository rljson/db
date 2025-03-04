// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, it } from 'vitest';

import { example } from '../src/example';

describe('example', () => {
  it('should run without error', () => {
    // Mock console.log
    const logMessages: string[] = [];
    console.log = (message: string) => logMessages.push(message);
    example();
    console.log = console.log;
  });
});
