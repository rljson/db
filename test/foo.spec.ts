// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { describe, expect, it } from 'vitest';

import { Foo } from '../src/foo';

describe('Foo', () => {
  describe('foo()', () => {
    it('should return "bar"', () => {
      const foo = new Foo();
      expect(foo.foo()).toBe('bar');
    });
  });
});
