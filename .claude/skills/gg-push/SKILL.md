---
name: gg-push
description: Pushes the ticket's commits with `gg do push`. Use when the user says "/gg-push" or asks to push the current ticket.
---

# Push

From the ticket folder:

```bash
gg do push
```

`gg do push` pushes the repos of the ticket in dependency order and refuses
repos that still carry uncommitted work. If it reports such a repo, run
`/gg-commit` first and push again.

It upgrades no dependency — the nightly build (`gg do maintain --nightly`)
does. Only when the ticket needs a newer version right away, push with
`gg do push --upgrade`.

## Important

- Push only when the user asked for it.
- Do not create pull requests here — `gg do review` does that.
