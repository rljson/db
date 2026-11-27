<!--
@license
Copyright (c) 2025 Rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# Install Node on Windows

- [Uninstall Node](#uninstall-node)
  - [Clean cache](#clean-cache)
  - [Run uninstaller](#run-uninstaller)
  - [Remove remaining Node folders](#remove-remaining-node-folders)
  - [Remove node path from PATH variable](#remove-node-path-from-path-variable)
- [Install NVM](#install-nvm)
- [Install Node via NVM](#install-node-via-nvm)

# Install Node

## Uninstall Node

We are using `nvm` to install and select Node versions.
If you already have installed `nvm`, you can skip this and the next step

### Clean cache

[Microsoft](https://learn.microsoft.com/de-de/windows/dev-environment/javascript/nodejs-on-windows#install-nvm-windows-nodejs-and-npm)

```bash
npm cache clean --force
```

### Run uninstaller

Press `Windows`

Type `uninstall Node.js`

Klick `Uninstall Node.js`

Follow instructions

### Remove remaining Node folders

[Remove remaining Node folders](https://stackoverflow.com/questions/20711240/how-to-completely-remove-node-js-from-windows)

### Remove node path from PATH variable

Remove Node from PATH variable:

Press `Windows`

Type `environment`

Click `Systemumgebungsvariablen bearbeiten`

In the `upper box`, select `PATH`

Click `Bearbeiten`

Search for Node path

When existing, remove the entry.

Close the dialog

## Install NVM

[Source](https://github.com/coreybutler/nvm-windows#installation--upgrades)

Open <https://github.com/coreybutler/nvm-windows/releases>

Download `nvm-setup.exe` ()

Execute `nvm-setup.exe`

Follow instructions

## Install Node via NVM

Press `Windows`

Type `cmd` and pres `enter`

```bash
nvm install lts
nvm use lts
```
