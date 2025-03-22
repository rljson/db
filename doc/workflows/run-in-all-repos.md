<!--
@license
Copyright (c) 2025 Rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# Apply a task to all rljson repos

First make sure that all repos are clean.

```bash
node scripts/run-in-all-repos.js .. node `pwd`/scripts/is-clean-repo.js
```

Assume you want to execute `ls -la` in all cloned rljson repos

```bash
node scripts/run-in-all-repos.js ../ ls -la
```
