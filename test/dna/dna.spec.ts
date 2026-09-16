// Placed by `helix init` — instantiates and verifies this project's DNA
// on every test run. The logic lives in the @tssuite/helix-js
// dev-dependency and is updated through normal dependency updates.

import { runDnaTest } from '@tssuite/helix-js';
import { test } from 'vitest';

test(
  'dna is instantiated and unmodified',
  async () => {
    await runDnaTest();
  },
  120000,
);
