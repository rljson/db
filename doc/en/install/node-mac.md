<!--
@license
Copyright (c) 2025 Rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# Install Node on Mac

## Install nvm with Homebrew

```bash
brew install nvm
```

## Create nvm directory (if not created automatically)

```bash
mkdir ~/.nvm
```

## Add nvm to your shell configuration

```bash
export NVM_DIR="$HOME/.nvm"
[ -s "/opt/homebrew/opt/nvm/nvm.sh" ] && \. "/opt/homebrew/opt/nvm/nvm.sh"
[ -s "/opt/homebrew/opt/nvm/etc/bash_completion" ] && \. "/opt/homebrew/opt/nvm/etc/bash_completion"
```

## Apply changes

```bash
source ~/.zshrc
```

## Verify installation

```bash
command -v nvm
```

## Install node

```bash
nvm install lts
nvm use lts
```
