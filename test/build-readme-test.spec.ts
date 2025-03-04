// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { access, cp, readFile, rm } from 'fs/promises';
import { describe, expect, it } from 'vitest';

import { buildReadme } from '../src/build-readme';

import { updateGoldens } from './goldens/update-goldens';

describe('buildReadme()', () => {
  it('should insert example into README.public.md and write the result to dist/README.md', async () => {
    // Delete old dist/README.md
    const path = 'dist/README.md';

    // Remove existing file
    try {
      await rm(path);
    } catch {}

    // Run buildReadme
    await buildReadme();

    // Check if file exists
    expect(access(path)).resolves.toBeUndefined();
  });

  it('golden test', async () => {
    await buildReadme();

    if (updateGoldens) {
      cp('dist/README.md', 'test/goldens/README.md');
    }

    const actual = await readFile('dist/README.md', 'utf-8');
    const golden = await readFile('test/goldens/README.md', 'utf-8');
    expect(actual).toBe(golden);
  });
});
