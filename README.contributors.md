<!--
// @license
// Copyright (c) 2025 Rljson
//
// Use of this source code is governed by terms that can be
// found in the LICENSE file in the root of this package.
-->

# Contributors Guide

- [Install](#install)
  - [Check out](#check-out)
  - [Install pnpm](#install-pnpm)
  - [Install dependencies](#install-dependencies)
  - [Install Vscode extensions](#install-vscode-extensions)
  - [Uninstall Jest and Jasmine](#uninstall-jest-and-jasmine)
  - [Install GitHub CLI](#install-github-cli)
- [Develop](#develop)
  - [Read architecture doc](#read-architecture-doc)
  - [Debug](#debug)
  - [Update goldens](#update-goldens)
  - [Test and Build](#test-and-build)
- [Workflow](#workflow)
  - [Checkout main](#checkout-main)
  - [Create a branch](#create-a-branch)
  - [Commit](#commit)
  - [Update dependencies](#update-dependencies)
  - [Increase version](#increase-version)
  - [Create a pull request](#create-a-pull-request)
  - [Wait until PR is merged](#wait-until-pr-is-merged)
  - [Delete feature branch](#delete-feature-branch)
  - [Publish to NPM](#publish-to-npm)
- [Troubleshooting](#troubleshooting)
  - [Checkout README.trouble.md](#checkout-readmetroublemd)
  - [File issues on GitHub](#file-issues-on-github)

<!-- ........................................................................-->

## Install

### Check out

```bash
mkdir db
cd db
git clone https://github.com/rljson/db.git
cd db
```

### Install pnpm

Windows:

```bash
corepack enable pnpm
```

Mac:

```bash
sudo corepack enable pnpm
```

### Install dependencies

```bash
pnpm install
```

### Install Vscode extensions

Open this project in `vscode`.

Press `Cmd+Shift+P`.

Type `Extensions: Show Recommended Extensions` and press `Enter`.

The recommended extensions will be shown.

Make sure, all recommended extensions are shown.

### Uninstall Jest and Jasmine

Jest or Jasmine extensions conflict with the `Vitest` extension used for this
project.

Uninstall them, if you have installed them.

### Install GitHub CLI

Install GitHub CLI on Mac

```bash
brew install gh
```

Login

```bash
gh auth login
```

<!-- ........................................................................-->

## Develop

### Read architecture doc

Read [README.architecture.md](./README.architecture.md) to get an overview
of the package's architecture.

### Debug

In Vscode: At the `left side bar` click on the `Test tube` icon to open the `Test explorer`.

At the `top`, click on the `refresh` icon to show update the tests.

Open a test file (`*.spec.ts`)

Set a breakpoint.

Press `alt` and click on the play button left beside the test.

Execution should stop at the breakpoint.

### Update goldens

In various tests we are creating golden files, that are reference files that
are compared against the files created in the tests.

```bash
pnpm updateGoldens
```

### Test and Build

```bash
pnpm test &&\
pnpm build
```

<!-- ........................................................................-->

## Workflow

### Checkout main

```bash
git checkout main && \
git fetch && \
git pull
```

### Create a branch

Please replace `Commit Message` in the next command by your commit message.
It will also used for branch name and pull request

```bash
export MESSAGE="Commit also untracked files" && \
export BRANCH=`echo "$MESSAGE" | tr '[:upper:]' '[:lower:]' | sed 's/[^a-z0-9_]/_/g'` &&\
git checkout -b $BRANCH
```

### Commit

Develop your feature

Commit your changes

If you only have one thing, execute

```bash
git add . && git commit -m "$MESSAGE"
```

### Update dependencies

We aim to work with the latest versions of our dependencies.

```bash
pnpm update --latest &&\
git commit -m"Update dependencies"
```

### Increase version

```bash
pnpm version patch --no-git-tag-version && \
git commit -am"Increase version"
```

### Create a pull request

```bash
git push -u origin $BRANCH && \
gh pr create --base main --title "$MESSAGE" --body "" && \
gh pr merge --auto --squash
```

### Wait until PR is merged

```bash
echo -e "\033[34m$(gh pr view --json url | jq -r '.url')\033[0m"
echo "Wait until PR is closed ..." && \
until gh pr view --json closed | jq -e '.closed == true' >/dev/null; do
  sleep 2 >/dev/null;
done;
```

### Delete feature branch

```bash
git fetch && git checkout main && \
git reset --soft origin/main && \
git stash -m"PR Aftermath" && \
git pull && \
git branch -d $BRANCH
```

### Publish to NPM

```bash
npm publish --access public && \
git tag $(npm pkg get version | tr -d '\\"')
```

<!-- ........................................................................-->

## Troubleshooting

### Checkout README.trouble.md

Checkout [./README.trouble.md](./README.trouble.md)

### File issues on GitHub

Visit <https://github.com/rljson/db/issues>

Check if there is already an issue for your problem.

If no, report the issue.
