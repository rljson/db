<!--
@license
Copyright (c) rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# Einen gg-Workspace anlegen

## Einen Entwicklungsordner anlegen <a id="create-a-dev-dir"></a>

```bash
mkdir -p ~/dev2
cd ~/dev2
```

## Einen gg-Workspace initialisieren <a id="init-a-gg-workspace"></a>

```bash
gg do init workspace
```

Das legt den Ordner `.ocean` an und instanziiert das neueste `dna_gg` im
Workspace-Ordner: die gg-Leitfäden, die Skills und den verwalteten Block der
`CLAUDE.md`. Es erledigt, was `gg dna init` und
`gg dna add dna_gg --workspace` von Hand tun – `add` baut die DNA selbst,
es folgt kein `gg dna build` –, hält deren Pub-Dateien aber aus dem
Workspace-Ordner heraus. Jedes Ticket erhält eine Kopie der Skills.

## Die eigenen Repositories hinzufügen <a id="add-your-repositories"></a>

```bash
gg do add dnaOrgUrl
```
