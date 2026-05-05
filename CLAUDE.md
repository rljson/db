# CLAUDE.md — @rljson/db

Database layer for the RLJSON ecosystem. Depends on `@rljson/rljson`, `@rljson/io`.

---

## Non-Negotiable Constraints

- **Never commit directly to `main`.** Always work on a feature branch.
- **Never modify the `scripts` section in `package.json`** without explicit user permission.
- **ESLint pinned to `~9.39.2`.** ESLint 10+ breaks the build.
- **100% test coverage** on all new/modified `src/` files (Statements, Branches, Functions, Lines).

---

## Commit Discipline (MANDATORY — NEVER SKIP)

- **Commit small and often** — one logical unit = one commit. Never accumulate more than ~5 changed files before committing.
- `git status --short` must return **nothing** at session end. Never leave uncommitted changes behind.
- **Check state at every session start**: `git status --short`, `git branch`, `git log --oneline -3`.

### Pre-commit checklist (in order, no exceptions)

1. **Update docs FIRST** — update README.public.md, README.architecture.md, CLAUDE.md for any API/behavior change **before** proposing a commit. A feature is NOT complete until documentation matches.
2. **Fix TypeScript/lint errors** in every touched file (use IDE error checker).
3. **`pnpm exec eslint <changed-files>`** to catch lint violations.
4. **`pnpm test`** — must pass at 100% coverage. Fix all errors before moving on.

### Version bump = separate commit

```bash
pnpm version patch --no-git-tag-version
git commit -am"Increase version"
```

---

## Full Ticket Workflow (exact order — complete all steps before starting next ticket)

```bash
# 1. Start clean
git checkout main && git fetch && git pull

# 2. Feature branch
node scripts/create-branch.js "<description>"

# 3. Update deps (verify eslint stays on 9.x)
pnpm update --latest && pnpm ls eslint

# 4. Develop, write tests, update docs

# 5. Commit every ≤5-file logical unit
git add . && git commit -am"<description>"

# 6. Version bump
pnpm version patch --no-git-tag-version && git commit -am"Increase version"

# 7. Build (runs tests via prebuild)
pnpm run build

# 8. Rebase
git rebase main

# 9. Push
node scripts/push-branch.js

# 10. Create PR + auto-merge
gh pr create --base main --title "<title>" --body " "
gh pr merge --auto --squash

# 11. Wait
node scripts/wait-for-pr.js

# 12. Cleanup
node scripts/delete-feature-branch.js
```

**`pnpm link` is acceptable during development for local cross-repo dependencies. Before PR/merge: remove all `pnpm.overrides` using `link:../...` and restore published versions.**

---

## Git Scripts Reference

| Script | Guard |
|---|---|
| `node scripts/create-branch.js "desc"` | Kebab-case; fails without input |
| `node scripts/push-branch.js` | Refuses dirty tree; refuses push to `main` |
| `node scripts/wait-for-pr.js` | Polls until MERGED/CLOSED |
| `node scripts/delete-feature-branch.js` | Requires clean tree + merged |
| `node scripts/is-clean-repo.js` | Prints ✅/❌ |

Never bypass these with raw git commands.

---

## Pre-existing Coverage Failures

Pre-existing failures (in files NOT touched in this ticket) do not block a commit, but:
- Prove pre-existing: `git stash && pnpm test; git stash pop`
- Document in the commit message
- Never add NEW failures in modified files

---

## Coverage Requirements

- **All metrics MUST be 100%**: Statements, Branches, Functions, Lines.
- Coverage validates automatically in `pnpm test`. Build fails below 100%.
- **Never** use `/* v8 ignore */` to avoid writing tests for reachable code.

### Vitest 4.0 semantic ignore hints (MANDATORY)

All hints MUST include `-- @preserve` to survive esbuild transpilation.

| Pattern | Meaning |
|---|---|
| `/* v8 ignore if -- @preserve */` | Ignore the if-branch |
| `/* v8 ignore else -- @preserve */` | Ignore the else-branch |
| `/* v8 ignore next -- @preserve */` | Ignore next statement/expression |
| `/* v8 ignore file -- @preserve */` | Ignore entire file |
| `/* v8 ignore start -- @preserve */` ... `/* v8 ignore stop -- @preserve */` | Ignore a range |

**NEVER use:**

```typescript
/* v8 ignore next 3 -- @preserve */  // ❌ line-counting — fragile, breaks on refactoring
/* v8 ignore next */                  // ❌ missing @preserve — esbuild strips the comment
/* v8 ignore end */                   // ❌ 'end' not 'stop'
```

---

## Package Manager

Uses **pnpm**. **Never modify the `scripts` section in `package.json`** without explicit user permission.

---

## Dependency Pinning (MANDATORY)

```jsonc
"eslint": "~9.39.2"   // ✅ CORRECT — pin to 9.x
"eslint": "^10.0.0"   // ❌ WRONG — ESLint 10 breaks the build
```

After `pnpm update --latest`, always verify: `pnpm ls eslint`.

Also:
- **TypeScript**: ESM modules (`"type": "module"`)
- **License headers**: Required in all source files
- **Test framework**: Vitest with `describe()`, `it()`, `expect()`

---

## Testing

| Command | Purpose |
|---|---|
| `pnpm test` | All tests + lint + coverage |
| `pnpm run build` | Full build (prebuild runs tests) |
| `pnpm updateGoldens` | Regenerate golden snapshot files |
| Debug in VS Code | Open test file → set breakpoint → Alt+click play button in Test Explorer |

---

## Publish Workflow (MANDATORY)

### Pre-publish checklist

1. Remove all `pnpm.overrides` using `link:../...`, restore `package.json` + `pnpm-lock.yaml`.
2. `pnpm install` — reinstall with published versions.
3. `pnpm test` — must pass at 100%.
4. `pnpm run build`.
5. `pnpm version patch --no-git-tag-version && git commit -am"Increase version"`.
6. Commit ALL files including `package.json` and `pnpm-lock.yaml`.

### Merge & publish

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

**Always use exactly `pnpm publish` — no flags, no piping.**

### Cross-repo publish order (bottom-up)

| Order | Package | Depends on |
|---|---|---|
| 1 | `@rljson/rljson` | — (Layer 0) |
| 1 | `@rljson/network` | — (Layer 0) |
| 2 | `@rljson/io` | `@rljson/rljson` |
| 3 | `@rljson/bs` | `@rljson/rljson`, `@rljson/io` |
| 3 | `@rljson/db` | `@rljson/rljson`, `@rljson/io` |
| 4 | `@rljson/server` | `@rljson/rljson`, `@rljson/io`, `@rljson/bs`, `@rljson/db`, `@rljson/network` |
| 5 | `@rljson/fs-agent` | all of the above |

After publishing an upstream package, downstream packages must run `pnpm update --latest` before their own publish.
