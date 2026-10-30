<!--
@license
Copyright (c) rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# rljson Review Guide

How a change is reviewed in the rljson repos, on top of the review guide.
Fail fast: cheap checks first, the first finding ends the review, nothing is
merged on a partial pass.

## Read it whole

- Read the full diff against the current `main`, not the description and not
  the `main` it forked from.
- Check it is neither behind `main` nor conflicting: a stale branch may fix
  what `main` already fixed.
- One concern per branch: an unrelated change is a finding.

## Check the dependencies

- Every `@rljson/*` dependency is one exact, published version
  (`pnpm view @rljson/<pkg> version`).
- No `pnpm.overrides` in `package.json`; no `link:`, `file:` or `workspace:`
  specifier, no `resolutions`, no committed `node_modules`.
- `pnpm install --frozen-lockfile` succeeds.

## Nothing patched into a dependency

- No `patchedDependencies`, no `patches/`, no postinstall step writing into
  `node_modules`, no `dist/` under version control.

## The fix lives in the package it belongs to

- Ask of every changed file whether its logic belongs to an `@rljson/*`
  package. If it does, the change is in that package, tested and covered
  there, and what is left here is a version and wiring.
- An app-side workaround for a package defect is a finding even when it works.

## Build from a clean state

- An ordinary branch: `rm -rf node_modules && pnpm install --frozen-lockfile
  && pnpm run build` in a fresh clone.
- A gg ticket: in the ticket, after `gg do import ticket` and `gg do localize`
  — its branches link each other and do not install alone.
- `git status --short` stays empty afterwards.

## Tests prove the change

- `pnpm test` green, 100 percent on every touched file.
- The new test fails without the change: revert the source, run, restore.

## The lines themselves

- No duplicate of a fix `main` or a package release already has: search for
  the symptom.
- No re-implemented helper, no debug leftovers, no hard-coded endpoints, no
  credentials, keys or `.env` files.
- A `CHANGELOG.md` entry, docs per the rljson guide, `scripts` untouched,
  license headers on new files.

## Report

- Findings go back to the author as a pull request comment. Never push fixes
  into somebody else's branch unless the owner asks.
- Tell the owner in short, plain words what is wrong and where.
