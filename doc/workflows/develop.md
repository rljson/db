<!--
@license
Copyright (c) 2025 Rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# Ticket workflow — through gg

Every step is a gg command in single-repo mode (`gg one …`). The CI publishes
nothing; a release is a `gg one do publish` run by the owner.

- [Checkout main](#checkout-main)
- [Create a ticket branch](#create-a-ticket-branch)
- [Update dependencies](#update-dependencies)
- [Develop \& debug](#develop--debug)
- [Commit](#commit)
- [Push](#push)
- [Code review](#code-review)
- [Merge and release](#merge-and-release)
- [Clean up](#clean-up)

## Checkout main

```bash
git checkout main
git pull
```

## Create a ticket branch

```bash
gg one do create ticket compare-goldens-as-json -m "Compare goldens based on json and not strings"
```

Uncommitted changes are carried over to the new branch. The `ticket.json` it
writes stays local.

## Update dependencies

```bash
gg one do upgrade deps
```

## Develop & debug

- [Debug & debug with Vscode](./debug-with-vscode.md)
- [Update Goldens](./update-goldens.md)
- [Rename classes](./rename-classes.md)
- [Super hero tricks](./super-hero.md)

## Commit

```bash
gg one do commit -m "Compare goldens based on json and not strings"
```

Runs lint, `tsc`, eslint and the tests at 100 % coverage and commits only when
all of them are green. `gg one can commit` runs the same checks without
committing. gg writes nothing into `CHANGELOG.md` here: the entry is written
by hand.

## Push

```bash
gg one do push
```

## Code review

You need a code review? Open a pull request for it
(`gh pr create --base main --fill`). `gg one do publish` merges the branch
through that pull request afterwards.

## Merge and release

The owner runs, on the feature branch:

```bash
gg one do publish                # merge + release: asks patch/minor/major
gg one do publish --merge-only   # merge only: no version, no npm, no tag
```

gg merges first — an auto-merge pull request — and uploads to npm only once
the merge went through. Then it tags the release on `main` and deletes the
feature branch. A run that fails partway resumes with
`gg one do publish --continue`.

## Clean up

```bash
node scripts/delete-feature-branch.js
```
