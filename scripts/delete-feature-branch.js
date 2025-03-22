/*
 * @license
 * Copyright (c) 2025 Rljson
 *
 * Use of this source code is governed by terms that can be
 * found in the LICENSE file in the root of this package.
 */

import { execSync } from 'child_process';

// Color helpers
const red = (str) => `\x1b[31m${str}\x1b[0m`;
const gray = (str) => `\x1b[90m${str}\x1b[0m`;
const yellow = (str) => `\x1b[33m${str}\x1b[0m`;
const green = (str) => `\x1b[32m${str}\x1b[0m`;
const blue = (str) => `\x1b[34m${str}\x1b[0m`;
const lightGray = (str) => gray(str);

// Execute a shell command and return trimmed output
function runCommand(command, silent = true) {
  console.log(gray(`# ${command}`));
  return execSync(command, {
    encoding: 'utf-8',
    stdio: silent ? ['pipe', 'pipe', 'pipe'] : undefined,
  }).trim();
}

// Check for uncommitted changes
function hasUncommittedChanges() {
  console.log(lightGray('Check for uncommitted changes'));
  const status = runCommand('git status --porcelain');
  return status.length > 0;
}

// Check for unpushed commits
function hasUnpushedCommits(branch) {
  try {
    console.log(lightGray('Check for unpushed commits'));
    runCommand(`git rev-parse --abbrev-ref ${branch}@{u}`);
    const ahead = runCommand(`git rev-list --count ${branch}@{u}..${branch}`);
    return parseInt(ahead, 10) > 0;
  } catch {
    return true;
  }
}

// Try a test merge into main and check if it introduces changes
function isBranchEffectivelyMerged(featureBranch) {
  console.log(lightGray('Check if feature was fully merged'));

  try {
    runCommand(`git merge --no-commit --no-ff ${featureBranch}`);
    const changed = hasUncommittedChanges();
    runCommand('git merge --abort');
    return !changed;
  } catch {
    try {
      runCommand('git merge --abort');
    } catch {}
    return false;
  }
}

try {
  console.log(lightGray('Get current branch name'));
  const currentBranch = runCommand('git rev-parse --abbrev-ref HEAD');

  if (currentBranch === 'main') {
    console.log(yellow('Please call this script from a feature branch.'));
    process.exit(0);
  }

  if (hasUncommittedChanges()) {
    console.error(red('❌ You have uncommitted changes.'));
    console.error(
      yellow('Please commit or stash your changes before continuing.'),
    );
    process.exit(1);
  }

  if (hasUnpushedCommits(currentBranch)) {
    console.error(red('❌ You have unpushed commits.'));
    console.error(yellow('Please push your branch before continuing.'));
    process.exit(1);
  }

  console.log(lightGray(`Fetching and pulling 'main'...`));
  runCommand('git fetch');
  runCommand('git checkout main');
  runCommand('git pull origin main');

  const isMerged = isBranchEffectivelyMerged(currentBranch);

  if (isMerged) {
    runCommand(`git branch -d ${currentBranch}`);
    console.log(green(`✅ Branch '${currentBranch}' has been deleted.`));
  } else {
    console.error(
      red(`❌ Branch '${currentBranch}' is not merged into 'main'.`),
    );
    console.log(
      yellow(`Please (create and) merge a pull request and try again.`),
    );
    runCommand(`git checkout ${currentBranch}`);
    console.log(gray(`Switched back to '${currentBranch}'.`));
  }
} catch (error) {
  console.error(red(`Error: ${error.message}`));
  try {
    runCommand('git merge --abort');
  } catch {}
  process.exit(1);
}
