// Placed by `helix init` and `helix upgrade` — checks on every test run
// that the files this project's DNA generated are unchanged. It writes
// nothing; `gg dna build` applies DNA updates.

import { checkDna } from '@tssuite/helix-js';
import { test } from 'vitest';

test(
  'generated dna files are unchanged',
  async () => {
    await checkDna();
  },
  120000,
);
