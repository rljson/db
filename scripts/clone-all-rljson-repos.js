/*
 * @license
 * Copyright (c) 2025 Rljson
 *
 * Use of this source code is governed by terms that can be
 * found in the LICENSE file in the root of this package.
 */

import { execSync } from 'child_process';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

// Get current file directory
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ===== CONFIGURATION =====
const orgName = 'rljson'; // Replace with your GitHub org
// ==========================

/**
 * Determine target directory for cloning:
 * - If current directory is a Git repo, use parent directory
 * - Otherwise, use a subfolder called 'repos'
 */
function determineCloneDir() {
  const currentDir = process.cwd();
  const gitDir = path.join(currentDir, '.git');
  const isInGitRepo =
    fs.existsSync(gitDir) && fs.lstatSync(gitDir).isDirectory();

  return isInGitRepo
    ? path.join(currentDir, '..') // Clone one level up
    : path.join(currentDir, '.'); // Default clone dir
}

/**
 * Run a shell command and return stdout
 */
function run(command) {
  return execSync(command, { encoding: 'utf-8' }).trim();
}

/**
 * Use GitHub CLI to fetch all repos of the org
 */
function fetchRepos(org) {
  console.log(`📦 Fetching repositories for org: ${org}`);
  const result = run(
    `gh repo list ${org} --limit 1000 --json nameWithOwner,sshUrl -q '.[] | .sshUrl'`,
  );
  return result.split('\n').filter(Boolean);
}

/**
 * Clone all repos to the target directory
 */
function cloneRepos(repos, targetDir) {
  if (!fs.existsSync(targetDir)) {
    fs.mkdirSync(targetDir, { recursive: true });
  }

  for (const repoUrl of repos) {
    const repoName = repoUrl.split('/').pop().replace('.git', '');
    const repoPath = path.join(targetDir, repoName);

    if (fs.existsSync(repoPath)) {
      console.log(`✅ Skipping existing repo: ${repoName}`);
      continue;
    }

    console.log(`⬇️ Cloning ${repoName}...`);
    run(`git clone ${repoUrl} "${repoPath}"`);
  }

  console.log('🎉 Done!');
}

// ==== Main ====
try {
  const CLONE_DIR = determineCloneDir();
  const repos = fetchRepos(orgName);
  cloneRepos(repos, CLONE_DIR);
} catch (err) {
  console.error('❌ Error:', err.message);
}
