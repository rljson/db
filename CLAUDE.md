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
gg one do create ticket <branch-name> -m "<what the ticket does>"
pnpm install
```

`gg one do create ticket` creates the branch and writes `ticket.json`, which
stays local (gitignored).

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
4. **`gg one can commit`** — lint, `tsc`, eslint and the tests at 100% coverage:
   the checks `gg one do commit` runs before it commits. Fix all errors before
   moving on.

### Version and CHANGELOG

Never bump `package.json` by hand: `gg one do publish` asks for the increment
(patch / minor / major) and bumps the version. gg writes nothing into
`CHANGELOG.md` in a TypeScript repository, so the entry is prose written by
hand: under the version the release will carry (the current version plus
the increment named when handing over), or under `## [Unreleased]` for a
change merged with `--merge-only`, which the next release then renames.

---

## Full Ticket Workflow — through gg (exact order)

Every repository chore runs through **gg** in single-repo mode (`gg one …`);
there is no gg ticket workspace. The CI publishes nothing.

The skills the rljson DNA installs in `.claude/skills` (`/ticket`, `/commit`,
`/push`, `/publish`, `/cleanup`) are written for gg's ticket workspace
(`gg do …` from `~/dev`). There is none here: use the `gg one …` commands
below instead.

```bash
# 1. Start clean
git checkout main && git pull

# 2. Branch clone + ticket branch (see Branch & Workspace Discipline)
gg one do create ticket <branch-name> -m "<what the ticket does>"

# 3. Update deps when the ticket needs it (verify eslint stays on 9.x)
gg one do upgrade deps && pnpm ls eslint

# 4. Develop, write tests, update docs and CHANGELOG (see Version and CHANGELOG)

# 5. Commit every ≤5-file logical unit. Runs lint, tsc, eslint and the tests
#    at 100% coverage, commits only when all are green and records the result
#    in .gg/gg.json
gg one do commit -m "<what this step does>"

# 6. Push
gg one do push

# 7. Merge — the OWNER runs this, never Claude (see Publish Workflow)
gg one do publish                # merge + release
gg one do publish --merge-only   # merge only: no version, no npm, no tag

# 8. Cleanup: pull main in the primary clone, delete the branch clone
```

**`pnpm link` is acceptable during development for local cross-repo dependencies. Before the merge: remove all `pnpm.overrides` using `link:../...` and restore published versions.**

---

## gg Reference

| Command | What it does |
|---|---|
| `gg one do create ticket <branch> -m "…"` | Creates the ticket branch, carries uncommitted changes over, writes the local `ticket.json` |
| `gg one can commit` / `gg one do commit -m "…"` | Lint, `tsc`, eslint, tests at 100% coverage; commits only when green; records `.gg/gg.json` |
| `gg one can push` / `gg one do push` | Checks / pushes the feature branch |
| `gg one can publish` | Whether the branch can be released |
| `gg one do publish [--merge-only]` | **Owner only.** See Publish Workflow |
| `gg one did commit` / `did push` / `did publish` | Whether the current state went through gg — the CI Quick Check runs `did commit` on every push |
| `gg one do upgrade deps` | Upgrades the dependencies |
| `node scripts/delete-feature-branch.js` | Local cleanup after the merge: switches to `main`, pulls, deletes the branch only when merged |
| `node scripts/is-clean-repo.js` | Prints ✅/❌ |

`create-branch.js`, `push-branch.js`, `wait-for-pr.js`, `publish-to-npm.js`
and `add-version-tag.js` were removed: gg does what they did. Never replace
a gg step with raw git or `gh`.

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

- **Releases happen only through gg, run by the owner.** The CI publishes
  nothing (`.github/workflows/publish.yml.disabled`). Claude prepares the
  branch — committed and pushed through gg, CHANGELOG written — and hands
  over, naming the increment the CHANGELOG assumes. It never runs
  `gg one do publish` and never writes versions or tags by hand.
- **gg merges before it uploads.** `gg one do publish` merges the feature
  branch into `main` first (an auto-merge pull request) and uploads to npm
  only once that merge went through, so what reaches npm is what `main`
  holds. Then it tags `main`'s release commit and deletes the feature
  branch. A run that fails partway is resumed with
  `gg one do publish --continue`, never finished by hand.
- **Then build the cascade through the npm packages**, bottom-up, one level at
  a time. Release a level only once it is green, and wait for the registry
  before starting the next: `pnpm view <pkg>@<version>` must resolve.
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

1. Remove all `pnpm.overrides` using `link:../...`, restore `package.json` +
   `pnpm-lock.yaml`, `pnpm install`.
2. Everything committed and pushed through gg: `gg one did commit` and
   `gg one did push` are green.
3. `CHANGELOG.md` names the version the release will carry, and the
   hand-over names the increment that produces it.
4. `gg one can publish` is green.

### Merge & publish — the owner runs it

```bash
gg one do publish                # merge + release: asks patch/minor/major
gg one do publish --merge-only   # merge only: no version, no npm, no tag
gg one do publish --continue     # resume a run that failed partway
```

Afterwards: `node scripts/delete-feature-branch.js` in the branch clone,
`git checkout main && git pull` in the primary clone, then remove the branch
clone.

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

<!-- helix:claude_md:start -->

# gg workflow

This repo is developed ticket by ticket with the `gg` CLI. Follow the
development guide, it tells you when to ask the user and which command
comes next:

@doc/guides/for-ai/ai-dev-guide.md

The steps are also available as skills: `/gg-ticket`, `/gg-commit`,
`/gg-push`, `/gg-review`, `/gg-publish`, `/gg-cleanup`. `/gg` lists them
and says which one comes next.

# rljson additions to the gg workflow

- **Pull requests:** CI on a pull request checks that every commit went
  through gg's local checks (`gg one did commit`). The full tests run on
  `main` after the merge: watch them after every publish.

<!-- helix:claude_md:end -->
