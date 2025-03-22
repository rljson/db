/*
 * @license
 * Copyright (c) 2025 Rljson
 *
 * Use of this source code is governed by terms that can be
 * found in the LICENSE file in the root of this package.
 */

// push-if-not-main.mjs

// push-if-not-main.mjs

// push-if-not-main.mjs

import { execSync } from 'child_process';

// Execute a shell command and return trimmed output
function runCommand(command, silent = true) {
  console.log(gray(`# ${command}`));
  return execSync(command, {
    encoding: 'utf-8',
    stdio: silent ? ['pipe', 'pipe', 'pipe'] : undefined,
  }).trim();
}

// Simple color functions using ANSI escape codes
const red = (str) => `\x1b[31m${str}\x1b[0m`;
const yellow = (str) => `\x1b[33m${str}\x1b[0m`;
const blue = (str) => `\x1b[34m${str}\x1b[0m`;
const gray = (str) => `\x1b[90m${str}\x1b[0m`;
const green = (str) => `\x1b[32m${str}\x1b[0m`;

try {
  // Check for uncommitted changes
  const status = runCommand('git status --porcelain').toString().trim();
  if (status) {
    console.error(red('Uncommitted changes detected.'));

    console.error(
      red(yellow('Please commit or stash uncommited changes first.')),
    );
    process.exit(1);
  }

  // Get the current Git branch name
  const currentBranch = runCommand('git rev-parse --abbrev-ref HEAD')
    .toString()
    .trim();

  console.log(blue(`Current branch: ${currentBranch}`));

  // Abort if the branch is 'main'
  if (currentBranch === 'main') {
    console.error(red("Abort: Pushing to 'main' is not allowed."));
    process.exit(1);
  }

  // Build and run the push command
  const pushCommand = `git push -u origin ${currentBranch}`;
  console.log(gray(`${pushCommand}`));
  runCommand(pushCommand, { stdio: 'inherit' });
  console.log(green(`✅ Successfully pushed branch: ${currentBranch}`));
} catch (err) {
  console.error(red('Error while executing script:'), yellow(err.message));
  process.exit(1);
}
