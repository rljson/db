<!--
@license
Copyright (c) rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# rljson-Review-Leitfaden

Wie eine Änderung in den rljson-Repos geprüft wird, zusätzlich zum
Review-Leitfaden. Früh abbrechen: günstige Prüfungen zuerst, der erste Befund
beendet das Review, nichts wird nach einem nur teilweise bestandenen Durchlauf
gemergt.

## Vollständig lesen <a id="read-it-whole"></a>

- Den vollständigen Diff gegen das aktuelle `main` lesen, nicht die
  Beschreibung und nicht das `main`, von dem abgezweigt wurde.
- Prüfen, dass der Branch weder hinter `main` liegt noch Konflikte hat: Ein
  veralteter Branch behebt vielleicht, was `main` schon behoben hat.
- Ein Anliegen pro Branch: Eine fremde Änderung ist ein Befund.

## Die Abhängigkeiten prüfen <a id="check-the-dependencies"></a>

- Jede `@rljson/*`-Abhängigkeit ist genau eine veröffentlichte Version
  (`pnpm view @rljson/<pkg> version`).
- Keine `pnpm.overrides` in `package.json`; kein `link:`-, `file:`- oder
  `workspace:`-Specifier, keine `resolutions`, keine eingecheckten
  `node_modules`.
- `pnpm install --frozen-lockfile` gelingt.

## Nichts in eine Abhängigkeit gepatcht <a id="nothing-patched-into-a-dependency"></a>

- Keine `patchedDependencies`, kein `patches/`, kein Postinstall-Schritt, der in
  `node_modules` schreibt, kein `dist/` unter Versionskontrolle.

## Der Fix liegt in dem Paket, zu dem er gehört <a id="the-fix-lives-in-the-package-it-belongs-to"></a>

- Bei jeder geänderten Datei fragen, ob ihre Logik in ein `@rljson/*`-Paket
  gehört. Wenn ja, steckt die Änderung in diesem Paket, dort getestet und
  abgedeckt, und hier bleiben nur eine Version und die Verdrahtung.
- Ein Workaround in der App für einen Paketfehler ist ein Befund, auch wenn er
  funktioniert.

## Aus einem sauberen Zustand bauen <a id="build-from-a-clean-state"></a>

- Ein gewöhnlicher Branch: `rm -rf node_modules && pnpm install --frozen-lockfile
&& pnpm run build` in einem frischen Klon.
- Ein gg-Ticket: im Ticket, nach `gg do import ticket` und `gg do localize` —
  seine Branches verlinken einander und lassen sich nicht einzeln installieren.
- `git status --short` bleibt danach leer.

## Tests belegen die Änderung <a id="tests-prove-the-change"></a>

- `pnpm test` grün, 100 Prozent in jeder berührten Datei.
- Der neue Test schlägt ohne die Änderung fehl: Quelle zurücksetzen,
  ausführen, wiederherstellen.

## Die Zeilen selbst <a id="the-lines-themselves"></a>

- Kein Duplikat eines Fixes, den `main` oder ein Paket-Release schon hat: nach
  dem Symptom suchen.
- Kein nachgebauter Helfer, keine Debug-Reste, keine fest codierten Endpunkte,
  keine Zugangsdaten, Schlüssel oder `.env`-Dateien.
- Ein `CHANGELOG.md`-Eintrag, Doku gemäß rljson-Leitfaden, `scripts`
  unverändert, Lizenz-Header in neuen Dateien.

## Bericht <a id="report"></a>

- Befunde gehen als Pull-Request-Kommentar an den Autor zurück. Nie Fixes in
  den Branch eines anderen pushen, außer der Owner bittet darum.
- Dem Owner in kurzen, einfachen Worten sagen, was falsch ist und wo.
