/*
 * @license
 * Copyright (c) 2025 Rljson
 *
 * Use of this source code is governed by terms that can be
 * found in the LICENSE file in the root of this package.
 */

// publish-and-tag.js
import { exec } from 'child_process';

function runCommand(command, options = {}) {
  return new Promise((resolve, reject) => {
    const proc = exec(command, options, (error, stdout, stderr) => {
      if (error) {
        reject({ error, stdout, stderr });
      } else {
        resolve({ stdout, stderr });
      }
    });

    proc.stdout.pipe(process.stdout);
    proc.stderr.pipe(process.stderr);
  });
}

(async () => {
  try {
    console.log('📦 Publishing package...');
    await runCommand('pnpm publish --access public');
    console.log('✅ Publish successful. Adding version tag...');
    await runCommand('node scripts/add-version-tag.js');
    console.log('🏷️ Version tag added.');
  } catch (e) {
    console.error('❌ Operation failed:', e.error?.message || e);
    process.exit(1);
  }
})();
