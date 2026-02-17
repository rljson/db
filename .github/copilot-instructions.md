# Copilot Instructions for @rljson/db

## Coverage Requirements

- **All metrics MUST be 100%**: Statements, Branches, Functions, Lines
- Coverage validation runs automatically in `pnpm test`
- Build fails if any metric drops below 100%

**MANDATORY: Vitest 4.0 Ignore Patterns (ast-v8-to-istanbul)**

Since Vitest 4.0, coverage uses `ast-v8-to-istanbul` which supports **semantic** ignore hints.
**ALWAYS use semantic hints. NEVER use the old `next N` line-counting pattern.**

All comments MUST include `-- @preserve` to survive esbuild transpilation.

**Allowed patterns:**

| Pattern                                                                      | Meaning                              |
| ---------------------------------------------------------------------------- | ------------------------------------ |
| `/* v8 ignore if -- @preserve */`                                            | Ignore the if-branch                 |
| `/* v8 ignore else -- @preserve */`                                          | Ignore the else-branch               |
| `/* v8 ignore next -- @preserve */`                                          | Ignore the next statement/expression |
| `/* v8 ignore file -- @preserve */`                                          | Ignore the entire file               |
| `/* v8 ignore start -- @preserve */` ... `/* v8 ignore stop -- @preserve */` | Ignore a range of lines              |

**FORBIDDEN patterns (NEVER use):**

```typescript
// ❌ WRONG: Line counting — fragile, breaks on refactoring
/* v8 ignore next 3 -- @preserve */
/* v8 ignore next 5 -- @preserve */

// ❌ WRONG: Missing @preserve — esbuild strips the comment
/* v8 ignore next */
/* v8 ignore start */

// ❌ WRONG: 'end' instead of 'stop'
/* v8 ignore end */
```

**Correct examples:**

```typescript
// Defensive null check — use 'if' to ignore the entire if-block
/* v8 ignore if -- @preserve */
if (!meta) {
  continue;
}

// Error catch blocks — use 'start'/'stop' for multi-line ranges
try {
  result = await someOperation();
} catch {
  /* v8 ignore start -- @preserve */
  // Defensive fallback
  continue;
}
/* v8 ignore stop -- @preserve */

// Ignore one expression
/* v8 ignore next -- @preserve */
if (this._db && this._treeKey) {
  // ...
}

// Ignore else-branch only
/* v8 ignore else -- @preserve */
if (isConnected) {
  handleConnection();
} else {
  // defensive fallback
}
```

**Invalid use of v8 ignore**: Do not use to avoid writing tests for reachable code.

## Package Manager

Uses **pnpm**. Never modify the `scripts` section in `package.json` without explicit user permission.

## Post-Edit Validation (MANDATORY)

**ALWAYS run these checks after editing ANY file. No exceptions.**

1. **Check for TypeScript / lint errors** in every file you touched (use the IDE error checker)
2. **Run `pnpm exec eslint <changed-files>`** to catch lint violations
3. **Run `pnpm test`** to verify tests pass and coverage stays at 100%
4. **Fix all errors before moving on** — never leave red squiggles behind

This applies to source files AND test files. A change is not complete until all diagnostics are clean.

## Git Workflow (MANDATORY)

- **NEVER commit directly to `main`.** Always work on a feature branch.
- When proposing commits, provide a commit message, wait for user approval, then commit.
- **`pnpm link` is acceptable** during development for local cross-repo dependencies.
- **Before PR/merge**: unlink all local overrides (`git restore package.json pnpm-lock.yaml`, remove `pnpm.overrides`), verify tests still pass with published versions.

## Publish Workflow (MANDATORY)

All `@rljson/*` packages share the same publish workflow documented in `doc/develop.md` (or `doc/workflows/develop.md`). **Follow these steps in exact order:**

### Pre-publish checklist

1. **Unlink local overrides** — Remove all `pnpm.overrides` entries that use `link:../...` and restore `package.json` and `pnpm-lock.yaml` to use published versions:
   ```bash
   # Remove overrides from package.json (set "overrides": {})
   # Then reinstall to get published versions:
   pnpm install
   ```
2. **Run tests with published deps** — `pnpm test` must pass with 100% coverage using published (not linked) dependencies.
3. **Rebuild** — `pnpm run build` (which runs tests via `prebuild`).
4. **Increase version** — `pnpm version patch --no-git-tag-version` then `git commit -am"Increase version"`.
5. **Commit ALL files** — including `package.json` and `pnpm-lock.yaml`. Nothing should be left uncommitted.

### Merge & publish steps

```bash
git rebase main
node scripts/push-branch.js
gh pr create --base main --title "<PR title>" --body " "
gh pr merge --auto --squash
node scripts/wait-for-pr.js
node scripts/delete-feature-branch.js
git checkout main && git pull
pnpm login
pnpm publish
```

### Cross-repo publish order

Packages MUST be published bottom-up by dependency order. A downstream package can only be published after its upstream dependency is on npm.

| Order | Package            | Depends on                                                 |
| ----- | ------------------ | ---------------------------------------------------------- |
| 1     | `@rljson/rljson`   | — (Layer 0, no `@rljson` deps)                             |
| 2     | `@rljson/io`       | `@rljson/rljson`                                           |
| 3     | `@rljson/bs`       | `@rljson/rljson`, `@rljson/io`                             |
| 4     | `@rljson/db`       | `@rljson/rljson`, `@rljson/io`                             |
| 5     | `@rljson/server`   | `@rljson/rljson`, `@rljson/io`, `@rljson/bs`, `@rljson/db` |
| 6     | `@rljson/fs-agent` | all of the above                                           |

After publishing an upstream package, downstream packages must `pnpm update --latest` to pick up the new version before their own publish.

## Dependency Pinning (MANDATORY)

- **ESLint**: Pin to `~9.39.2`. ESLint 10+ breaks the build. Never allow `pnpm update --latest` to bump eslint beyond 9.x.

  ```jsonc
  // ✅ CORRECT
  "eslint": "~9.39.2"

  // ❌ WRONG — will pull in v10 which breaks everything
  "eslint": "^10.0.0"
  ```

- After running `pnpm update --latest`, **always verify** eslint stayed on 9.x: `pnpm ls eslint`.

- **TypeScript**: ESM modules (`"type": "module"`)
- **License headers**: Required in all source files
- **Test framework**: Vitest with `describe()`, `it()`, `expect()`
