/*
 * @license
 * Copyright (c) 2025 Rljson
 *
 * Use of this source code is governed by terms that can be
 * found in the LICENSE file in the root of this package.
 */

import { execSync } from 'child_process';

const gray = (str) => `\x1b[90m${str}\x1b[0m`;
const yellow = (str) => `\x1b[33m${str}\x1b[0m`;
const blue = (str) => `\x1b[34m${str}\x1b[0m`;

function runCommand(command, silent = true) {
  console.log(gray(`# ${command}`));
  return execSync(command, {
    encoding: 'utf-8',
    stdio: silent ? ['pipe', 'pipe', 'pipe'] : undefined,
  }).trim();
}

// Converts a string to kebab-case
function toKebabCase(str) {
  return str
    .toLowerCase()
    .trim()
    .replace(/[^a-z0-9]+/g, '-') // Replace non-alphanumeric characters with dashes
    .replace(/^-+|-+$/g, ''); // Remove leading or trailing dashes
}

// Get command line arguments
const input = process.argv.slice(2).join(' ');

if (!input) {
  console.error('❌ Please provide a branch name.');
  process.exit(1);
}

const kebabCaseName = toKebabCase(input);

try {
  // Create new Git branch
  runCommand(`git checkout -b ${kebabCaseName}`);
  console.log('✅ ' + yellow('Created new branch: ') + blue(kebabCaseName));
} catch (error) {
  console.error('❌ Failed to create branch:', error.message);
}
