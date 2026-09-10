# CLAUDE.md — @rljson/db

Database layer for the RLJSON ecosystem. Depends on `@rljson/rljson`, `@rljson/io`.

---

## Non-Negotiable Constraints

- **Never work on an in-repo copy of a dependency.** If code belongs to an
  `@rljson/*` package, it is changed **in that package**, released, and consumed
  from the registry — never vendored into this repo's `src/`. A repo-local copy
  forks silently. On 2026-09-10 a long-lived branch of `cos-one-client` carried
  359 lines written into a copy of `@rljson/mongo-agent` that `main` had deleted
  two days earlier; the branches could not be merged until that code was ported
  into the package and released from a third repository. Rule of thumb: a
  directory under `src/` named after a dependency **and carrying an `index.ts`**
  is a copy, not glue over one.
- **Never commit directly to `main`.** Always work on a feature branch.
- **Never modify the `scripts` section in `package.json`** without explicit user permission.
- **ESLint pinned to `~9.39.2`.** ESLint 10+ breaks the build.
- **100% test coverage** on all new/modified `src/` files (Statements, Branches, Functions, Lines).

---

## Branch & Workspace Discipline (MANDATORY)

- **Every fix and every larger feature set gets its own branch.** Never work on
  `main`, and never bundle unrelated changes into one branch.
- **Check the branch out as a SEPARATE CLONE next to the repo**, named
  `<repo>-<branch>`, and add that folder to the VS Code workspace. Develop
  there. The primary clone stays on `main` and stays usable.

```bash
git clone --no-hardlinks <repo> <repo>-<branch-name>
cd <repo>-<branch-name>
git remote set-url origin <origin-url>
git checkout -b <branch-name>
pnpm install
```

- **When the branch is merged: remove the folder from the workspace and delete
  it.** A stale clone is a source of work against a dead branch.

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

### rljson package versions (MANDATORY)

- **Every package declares the versions it is built and tested against.** Exact
  pins, no ranges. On a `0.0.x` version `^` allows no range anyway, so a caret
  is a hard pin — usually on a version nobody runs.
- **Never leave a dependency that only a consuming app's `pnpm.overrides`
  corrects.** The moment the declared graph stops matching what runs, every
  green test result is about a stack nobody ships.
- `pnpm install --force` does **not** re-resolve a changed specifier. Use
  `pnpm install --no-frozen-lockfile`, or the old version stays installed while
  `package.json` claims the new one.


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

### Hard rules (NEVER SKIP)

- **Publish only from `main`.** Never from a branch, never from a worktree,
  never with an uncommitted version bump. Merge first, `git checkout main &&
  git pull`, publish from there.
- **Then build the cascade through the npm packages**, bottom-up, one level at
  a time. Publish a level only once it is green and only from its own `main`,
  and wait for the registry before starting the next: `pnpm view <pkg>@<version>`
  must resolve.
- **If the repo's version disagrees with npm, STOP.** Diff a build of `main`
  against the published tarball before doing anything else. A mismatch means
  releases were cut off-branch and `main` is missing shipped code — publishing
  would silently undo it.

*Why these are hard rules: `@rljson/fs-agent` 0.0.61–0.0.67 were cut from an
unmerged branch, so `main` was missing ten commits of field-validated fixes
that existed only in the tarball. Separately, an `io` fix was shipped by
pinning it in one app's overrides, leaving `db`, `server` and `mongo-agent`
declaring a version they did not run. Each cost half a day.*


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
| 4 | `@rljson/bs-fs` | `@rljson/bs` |
| 4 | `@rljson/server` | `@rljson/rljson`, `@rljson/io`, `@rljson/bs`, `@rljson/db`, `@rljson/network` |
| 5 | `@rljson/fs-agent` | all of the above |
| 6 | `@rljson/mongo-agent` | all of the above |
| 7 | consuming app (e.g. `cos-one-client`) | all of the above |

After publishing an upstream package, each downstream package pins the new
version EXPLICITLY, runs its own tests against it, and publishes from its own
`main` before the next level starts.
