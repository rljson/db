/*
 * @license
 * Copyright (c) 2025 Rljson
 *
 * Use of this source code is governed by terms that can be
 * found in the LICENSE file in the root of this package.
 */

import { execSync } from 'child_process';
import { promises as fs } from 'fs';

// Define red, blue, yellow methods
const red = (str) => `\x1b[31m${str}\x1b[0m`;
const blue = (str) => `\x1b[34m${str}\x1b[0m`;
const yellow = (str) => `\x1b[33m${str}\x1b[0m`;
const green = (str) => `\x1b[32m${str}\x1b[0m`;
const gray = (str) => `\x1b[90m${str}\x1b[0m`;

const getCurrentBranch = () => {
  return execSync('git rev-parse --abbrev-ref HEAD').toString().trim();
};

const isMainUpToDate = () => {
  try {
    execSync('git fetch origin main');
    const localHash = execSync('git rev-parse main').toString().trim();
    const remoteHash = execSync('git rev-parse origin/main').toString().trim();
    return localHash === remoteHash;
  } catch (error) {
    console.error(red('Error checking branch status:'), error);
    return false;
  }
};

const getVersion = async () => {
  const packageJson = JSON.parse(await fs.readFile('package.json', 'utf8'));
  return packageJson.version;
};

const createTag = (version) => {
  try {
    execSync(`git tag v${version}`);
    execSync(`git push origin v${version}`);
    console.log(green(`Tag v${version} created and pushed successfully.`));
  } catch (error) {
    console.error(
      red('Error creating or pushing tag\n'),
      gray(error.message.trim()),
    );
  }
};

const main = async () => {
  const branch = getCurrentBranch();
  if (branch !== 'main') {
    console.error(red('Error: You must be on the main branch.'));
    process.exit(1);
  }

  if (!isMainUpToDate()) {
    console.error(
      red('Error: main branch is not up to date with origin/main.'),
    );
    process.exit(1);
  }

  const version = await getVersion();
  createTag(version);
};

main().catch(console.error);
