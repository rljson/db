/// <reference types="vitest" />

import { defineConfig } from 'vite';


// https://vitejs.dev/config/
export default defineConfig(({ mode }) => {
  return {
    test: {
      globals: true,
      environment: 'node',
      setupFiles: ['./test/test-setup.ts'],
      include: ['**/test/**/*.spec.ts'],

      reporters: ['default'],
      coverage: {
        enabled: true,
        provider: 'v8', // "istanbul" or "v8"
        reporter: ['text', 'json', 'html'],
        include: ['src/**/*.ts'],
        exclude: ['src/index.ts'],
        all: true,
        thresholds: {
          statements: 100,
          branches: 100,
          functions: 100,
          lines: 100,
        },
        checkCoverage: true,
      },
    },
    define: {
      'import.meta.vitest': mode !== 'production',
    },
  };
});
