# CLAUDE.md — @rljson/db

Database layer for the RLJSON ecosystem. Depends on `@rljson/rljson`, `@rljson/io`.

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

The rules every rljson repo shares:

@doc/guides/rljson-guide.md

- **Reviews** follow `doc/guides/for-ai/rljson-review-guide.md`.
- **Pull requests:** CI on a pull request runs the quick check (`gg one did
  commit`), which passes only for commits made through gg. The full tests run
  on `main` after the merge: watch them after every release.
- **Never merge, publish or delete unasked** — no branch, ticket or ticket
  folder. The owner runs `gg do publish`.

<!-- helix:claude_md:end -->
