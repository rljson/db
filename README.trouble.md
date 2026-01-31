<!--

-->

# Trouble shooting

## Table of contents <!-- omit in toc -->

- [Tree INSERT operations failing with empty results](#tree-insert-operations-failing-with-empty-results)
- [Vscode Windows: Debugging is not working](#vscode-windows-debugging-is-not-working)
- [GitHub actions: Cannot find module @rollup/rollup-linux-x64-gnu](#github-actions-cannot-find-module-rolluprollup-linux-x64-gnu)

## Tree INSERT operations failing with empty results

Date: 2026-01-28

### Symptoms

- Tree INSERT operations complete without errors
- GET queries after INSERT return empty results or only root node
- Cell length is 1 instead of expected count
- Navigation stops at root level

### Root Cause

The `treeFromObject` function from `@rljson/rljson` v0.0.74+ creates an explicit root node with `id='root'` at the end of the tree array. When inserting an already-isolated subtree (from `isolate()`), this created a double-root structure:

```
Auto-root (id='root')
  └─ User-root (id='root')
       └─ actual data nodes
```

When `TreeController` navigates the tree, it stops at the first node with `id='root'` (the auto-root), which has no meaningful data.

### Solution

✅ **Fixed in current version**: The `db.ts` file now calls `treeFromObject` with `skipRootCreation: true` parameter:

```typescript
const trees = treeFromObject(treeObject, true);
```

This prevents the automatic root wrapper from being created during INSERT operations.

### Verification

Run the tree INSERT tests:

```bash
pnpm test --run --grep "insert on tree"
```

All tree INSERT tests should pass:

- "insert on tree root node"
- "insert on tree deeper leaf"
- "insert on tree simple branch"
- "insert new child on branch"

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
