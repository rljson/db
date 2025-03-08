// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { readFile, writeFile } from 'fs/promises';
import { expect } from 'vitest';

/// If this is set to true, the golden files will be updated.
export const updateGoldens = () => {
  return process.env['UPDATE_GOLDENS'] === 'true';
};

export const checkGoldens = async (goldenFile: string, expected: string) => {
  // Write golden file
  if (updateGoldens()) {
    await writeFile(goldenFile, expected);
  }

  // Compare log messages with golden file
  const golden = await readFile(goldenFile, 'utf8');
  const message =
    `Golden "${goldenFile}" does not match expected content. ` +
    'Consider to rebuild goldens using "npm run updateGoldens".';
  expect(golden, message).toBe(expected);
};
