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

## Coding Style

- **TypeScript**: ESM modules (`"type": "module"`)
- **License headers**: Required in all source files
- **Test framework**: Vitest with `describe()`, `it()`, `expect()`
