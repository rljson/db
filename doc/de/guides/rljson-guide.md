<!--
@license
Copyright (c) rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# rljson-Leitfaden

Die Regeln, die für jedes rljson-Repo gelten. Zuerst die Leitfäden zu Code,
Tests und Entwicklung lesen; dieser ergänzt, was die rljson-Familie besonders
macht.

## Ein Paket dort ändern, wo es hingehört <a id="change-a-package-where-it-belongs"></a>

- Ein `@rljson/*`-Paket in seinem eigenen Repo ändern: dort testen und
  abdecken, releasen, dann verwenden. In einem gg-Ticket dieses Repo zum
  Ticket hinzufügen.
- Nie eine Abhängigkeit nach `src/` kopieren. Ein Ordner dort, der nach einer
  Abhängigkeit benannt ist und eine `index.ts` enthält, ist eine Kopie, kein
  Kleber darüber — eine solche Kopie von `@rljson/mongo-agent` ist einmal
  wochenlang unbemerkt abgedriftet.
- Ein Workaround in der App für einen Paketfehler ist falsch, auch wenn er
  funktioniert.

## Die rljson-Pakete exakt pinnen <a id="pin-the-rljson-packages-exactly"></a>

- Jede `@rljson/*`-Abhängigkeit nennt genau eine Version (`"0.0.86"`); das
  Release von gg trägt die neuen ein.
- Keine `pnpm.overrides` in `package.json`, für nichts: pnpm 10 ignoriert dann
  die Links, die gg für ein Ticket in `pnpm-workspace.yaml` schreibt.
  Overrides gehören in `pnpm-workspace.yaml`.
- Kein `link:`-, `file:`- oder `workspace:`-Specifier in `package.json`. gg
  verlinkt ein Ticket ausschließlich über `pnpm-workspace.yaml`.
- Repos mit pnpm 11 führen `'@rljson/*'` unter `minimumReleaseAgeExclude`:
  Ein Release installiert Versionen, die Minuten zuvor veröffentlicht wurden.
- Nach dem Ändern eines Specifiers `pnpm install --no-frozen-lockfile`
  ausführen; `--force` lässt die alte Version installiert.

## Abdeckung bei 100 Prozent halten <a id="keep-coverage-at-100-percent"></a>

- Statements, Branches, Funktionen und Zeilen in jeder berührten Datei;
  darunter schlägt `pnpm test` fehl.
- Erreichbaren Code nie per Hinweis wegdefinieren. Wo ein `v8 ignore`-Hinweis
  richtig ist, trägt er `-- @preserve`, sonst entfernt esbuild ihn:
  - `/* v8 ignore next -- @preserve */`, `if`, `else` oder `file` ebenso
  - `/* v8 ignore start -- @preserve */` … `/* v8 ignore stop -- @preserve */`
  - nie eine Zeilenanzahl (`next 3`), nie `end` statt `stop`
- Ein neuer Test schlägt ohne die Änderung fehl, die er abdeckt: Quelle
  zurücksetzen, Test ausführen, scheitern sehen, wiederherstellen.
- Einen Wert knapp unter 100 Prozent vor dem Melden erneut messen:
  timergesteuerte Tests decken nur zufällig ab.

## Einen alten Fehler ohne Stash nachweisen <a id="prove-a-failure-is-old-without-stashing"></a>

- Ein Fehler in einer Datei, die das Ticket nicht berührt hat, blockiert
  nicht. Ihn auf einem sauberen `main` nachweisen (zweiter Klon oder
  `git worktree add`), nie mit `git stash`: Ein blindes `stash pop` hat schon
  geparkte Arbeit zerstört.
- Ihn in der Commit-Nachricht nennen. Nie einen neuen hinzufügen.

## Im selben Ticket dokumentieren <a id="document-in-the-same-ticket"></a>

- `README.public.md`, `README.architecture.md` und `CLAUDE.md` ziehen bei jeder
  API- oder Verhaltensänderung im selben Ticket nach; vorher ist die Änderung
  nicht fertig.
- `CHANGELOG.md` sind die Release Notes, von Hand geschriebene Prosa unter
  `## [Unreleased]`. Der Commit-Check von gg lehnt eine Version über der in
  `package.json` ab, und das Release von gg benennt den Abschnitt in einem
  TypeScript-Repo nicht um: Im nächsten Ticket die veröffentlichte Version
  eintragen.

## Das hier nicht anfassen <a id="leave-these-alone"></a>

- Nie den Abschnitt `scripts` in `package.json` ohne Erlaubnis des Owners
  ändern.
- Nur ESM (`"type": "module"`), ein Lizenz-Header in jeder Quelldatei.

## Nur über gg releasen <a id="release-only-through-gg"></a>

- Der Owner führt `gg do publish` aus: Es merged, veröffentlicht und taggt in
  Abhängigkeitsreihenfolge. Nie Versionen oder Tags von Hand schreiben.
- Weicht die Version eines Repos von npm ab, anhalten und zuerst einen Build
  von `main` mit dem veröffentlichten Tarball vergleichen. Eine Abweichung
  bedeutet, dass ein Release abseits von `main` erstellt wurde, und ein
  Veröffentlichen würde es rückgängig machen.
- Nach jedem Release `Build and Test` auf `main` beobachten: Pull Requests
  führen nur den schnellen Check aus.

## Befehle <a id="commands"></a>

- `pnpm test` — Lint, Tests und Abdeckung
- `pnpm run build` — der vollständige Build
- `pnpm updateGoldens` — die Golden Files neu erzeugen; den Diff prüfen, nie
  neu erzeugen, nur damit ein Test besteht
