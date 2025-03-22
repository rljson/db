/*
 * @license
 * Copyright (c) 2025 Rljson
 *
 * Use of this source code is governed by terms that can be
 * found in the LICENSE file in the root of this package.
 */

import { promises as fs } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const copyFile = async (src, dest) => {
  await fs.copyFile(src, dest);
};

const createDir = async (dir) => {
  try {
    await fs.mkdir(dir, { recursive: true });
  } catch (err) {
    if (err.code !== 'EEXIST') throw err;
  }
};

const copyReadmeFiles = async () => {
  const srcDir = join(__dirname, '..');
  const destDir = join(__dirname, '..', 'dist');

  const files = await fs.readdir(srcDir);
  const readmeFiles = files.filter((file) => file.startsWith('README'));

  for (const file of readmeFiles) {
    const srcFile = join(srcDir, file);
    const destFile = join(destDir, file);
    await copyFile(srcFile, destFile);
  }
};

const main = async () => {
  const srcExample = join(__dirname, '..', 'src', 'example.ts');
  const destExampleDir = join(__dirname, '..', 'dist', 'src');
  const destExample = join(destExampleDir, 'example.ts');

  await createDir(destExampleDir);
  await copyFile(srcExample, destExample);
  await copyReadmeFiles();

  console.log('Files copied successfully.');
};

main().catch(console.error);
