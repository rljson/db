// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.

import { readFile } from 'fs/promises';
import { join } from 'path';
import { NormalizedPackageJson, readPackage } from 'read-pkg';
import { beforeAll, describe, expect, it } from 'vitest';

describe('package.json', () => {
  let json: NormalizedPackageJson;
  let viteConfig: string;
  let project: string;

  beforeAll(async () => {
    json = await readPackage();
    project = json.name.split('/').pop()!;

    const viteConfigPath = join(__dirname, '..', 'vite.config.mts');
    viteConfig = await readFile(viteConfigPath, 'utf-8');
  });

  describe('main', () => {
    it('points to dist/{package-name}.j', () => {
      expect(json.main).toBe(`dist/${project}.js`);
    });
  });

  describe('dependencies', () => {
    it('must also be listed in vite.config.mts, rollUpOptions/external', () => {
      const externalRegex = /external:\s*\[(.*?)\]/s;
      const match = viteConfig.match(externalRegex);
      expect(match).not.toBeNull();

      const ext = match![1]
        .split(',')
        .map((dep) => dep.trim().replace(/['"]/g, ''));

      const deps = Object.keys(json.dependencies!);
      for (const dep of deps) {
        expect(
          ext,
          `Please open vite.config.mts. Go to rollupOptions.external and add "${dep}". `,
        ).toContain(dep);
      }
    });
  });

  describe('homepage', () => {
    it('should be correct', () => {
      expect(json.homepage).toBe(`https://github.com/rljson/${project}`);
    });
  });

  describe('bugs', () => {
    it('should be correct', () => {
      expect(json.bugs?.url).toBe(
        `https://github.com/rljson/${project}/issues`,
      );
    });
  });

  describe('repository.url', () => {
    it('should be correct', () => {
      expect(json.repository?.url).toBe(
        `git+https://github.com/rljson/${project}.git`,
      );
    });
  });
});
