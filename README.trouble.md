<!--

-->

# Trouble shooting

## Table of contents <!-- omit in toc -->

- [Vscode Windows: Debugging is not working](#vscode-windows-debugging-is-not-working)
- [GitHub actions: Cannot find module @rollup/rollup-linux-x64-gnu](#github-actions-cannot-find-module-rolluprollup-linux-x64-gnu)

## Vscode Windows: Debugging is not working

Date: 2025-03-08

⚠️ IMPORTANT: On Windows, please check out the repo on drive C. There is a bug
in the VS Code Vitest extension (v1.14.4), which prevents test debugging from
working: <https://github.com/vitest-dev/vscode/issues/548> Please check from
time to time if the issue has been fixed and remove this note once it is
resolved.

## GitHub actions: Cannot find module @rollup/rollup-linux-x64-gnu

⚠️ Error: Cannot find module @rollup/rollup-linux-x64-gnu. npm has a bug related to
optional dependencies (<https://github.com/npm/cli/issues/4828>). Please try `npm
i` again after removing both package-lock.json and node_modules directory.

Solution:

```bash
rm -rf node_modules
rm package-lock.json
npm install
```

Then push `package-lock.yaml` again
