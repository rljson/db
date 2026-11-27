# rljson additions to the gg workflow

The rules every rljson repo shares:

@../doc/en/guides/rljson-guide.md

- **Reviews** follow `doc/en/guides/for-ai/rljson-review-guide.md`.
- **Pull requests:** CI on a pull request runs the quick check (`gg one did
  commit`), which passes only for commits made through gg. The full tests run
  on `main` after the merge: watch them after every release.
- **Never merge, publish or delete unasked** — no branch, ticket or ticket
  folder. The owner runs `gg do publish`.
