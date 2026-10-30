<!--
@license
Copyright (c) rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# rljson Guide

The rules every rljson repo shares. Read the code, test and develop guides
first; this adds what is particular to the rljson family.

## Change a package where it belongs

- Change an `@rljson/*` package in its own repo: tested and covered there,
  released, then used. In a gg ticket, add that repo to the ticket.
- Never copy a dependency into `src/`. A folder there named after a dependency
  and carrying an `index.ts` is a copy, not glue over one — such a copy of
  `@rljson/mongo-agent` once forked silently for weeks.
- An app-side workaround for a package defect is wrong even when it works.

## Pin the rljson packages exactly

- Every `@rljson/*` dependency names one exact version (`"0.0.86"`); gg's
  release writes the new ones in.
- No `pnpm.overrides` in `package.json`, for anything: pnpm 10 then ignores
  the links gg writes into `pnpm-workspace.yaml` for a ticket. Overrides go
  into `pnpm-workspace.yaml`.
- No `link:`, `file:` or `workspace:` specifier in `package.json`. gg links a
  ticket through `pnpm-workspace.yaml` only.
- pnpm 11 repos list `'@rljson/*'` under `minimumReleaseAgeExclude`: a release
  installs versions published minutes earlier.
- After changing a specifier, run `pnpm install --no-frozen-lockfile`;
  `--force` keeps the old version installed.

## Keep coverage at 100 percent

- Statements, branches, functions and lines on every touched file; `pnpm test`
  fails below.
- Never hint reachable code away. Where a `v8 ignore` hint is right, it carries
  `-- @preserve`, or esbuild strips it:
  - `/* v8 ignore next -- @preserve */`, `if`, `else` or `file` likewise
  - `/* v8 ignore start -- @preserve */` … `/* v8 ignore stop -- @preserve */`
  - never a line count (`next 3`), never `end` for `stop`
- A new test fails without the change it covers: revert the source, run it,
  watch it fail, restore.
- Run a figure just short of 100 percent again before reporting it:
  timer-driven tests cover by luck.

## Prove a failure is old without stashing

- A failure in a file the ticket did not touch does not block. Prove it on a
  clean `main` (a second clone or `git worktree add`), never with `git stash`:
  a blind `stash pop` has destroyed parked work.
- Name it in the commit message. Never add a new one.

## Document in the same ticket

- `README.public.md`, `README.architecture.md` and `CLAUDE.md` follow every API
  or behaviour change in the same ticket; the change is not done before.
- `CHANGELOG.md` is the release notes, prose written by hand under
  `## [Unreleased]`. gg's commit check refuses a version above `package.json`,
  and gg's release does not rename the section in a TypeScript repo: give it
  the released version in the next ticket.

## Leave these alone

- Never change the `scripts` section of `package.json` without the owner's
  permission.
- ESM only (`"type": "module"`), a license header in every source file.

## Release only through gg

- The owner runs `gg do publish`: it merges, publishes and tags in dependency
  order. Never write versions or tags by hand.
- If a repo's version disagrees with npm, stop and diff a build of `main`
  against the published tarball first. A mismatch means a release was cut off
  `main`, and publishing would undo it.
- Watch `main`'s `Build and Test` after every release: pull requests run only
  the quick check.

## Commands

- `pnpm test` — lint, tests and coverage
- `pnpm run build` — the full build
- `pnpm updateGoldens` — regenerate the golden files; review the diff, never
  regenerate to make a test pass
