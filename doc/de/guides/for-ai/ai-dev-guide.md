<!--
@license
Copyright (c) rljson

Use of this source code is governed by terms that can be
found in the LICENSE file in the root of this package.
-->

# Entwicklungsleitfaden für KIs

## Den Entwickler fragen, was das Ticket tun soll <a id="ask-the-developer-what-the-ticket-should-do"></a>

Eine einzige Frage stellen: Was soll das Ticket tun? Eine Freitext-Antwort
genügt – nie getrennt nach Ticket-ID, Titel oder Beschreibung fragen.

## Ein Ticket anlegen <a id="create-a-ticket"></a>

Aus der Antwort ableiten, ohne weitere Fragen:

- **id** – ein kurzer, sprechender Slug in `snake_case`, 2–5 einfache
  ASCII-Wörter (Nicht-ASCII transliterieren, Satzzeichen und Emoji
  weglassen), z. B. `fix_login_crash`. Nie eine fortlaufende Nummer:
  Meist wird an mehreren Tickets parallel gearbeitet, und ein Zähler
  kollidiert zwischen ihnen. Die vorhandenen Ticket-Ordner im
  Workspace-Wurzelordner auflisten und, falls der Slug vergeben ist,
  `_2`, `_3`, … anhängen, bis er frei ist. Lehnt `gg do create ticket` ihn
  trotzdem als vergeben ab (z. B. weil ein anderer Agent gerade denselben
  Slug angelegt hat), das Suffix erhöhen und erneut versuchen.
- **message** – eine Zeile im Imperativ bis ein kurzer Satz, der die
  Änderung beschreibt, für `-m`.

```bash
cd ~/dev/ # workspace
gg do create ticket fix_login_crash -m"Fix issue abc"
cd fix_login_crash
```

Dem Entwickler ID und Message direkt nach dem Anlegen des Tickets nennen –
beides lässt sich jederzeit ändern, z. B. durch Umbenennen des Ordners,
bevor etwas anderes hinzugefügt wird, oder durch Bearbeiten von
`ticket.json`.

## In diesem Dokument ersetzen <a id="replace-in-this-document"></a>

- `~/dev/` durch das Workspace-Verzeichnis aus dem Memory ersetzen
- Nach dem Workspace-Verzeichnis fragen, falls keines bekannt ist
- `fix_login_crash` durch die abgeleitete Ticket-ID ersetzen
- `Fix issue abc` durch die abgeleitete Ticket-Message ersetzen

## Das Projektmanagement-Repo wählen – oder ohne arbeiten <a id="choose-the-project-management-repo--or-work-without-one"></a>

Ein Ticket kann zu einem Projektmanagement-Repo gehören. Es enthält die
Pläne, Entscheidungen und Blogposts eines Projekts, aber keinen Code. Es ist
optional: Den Nutzer fragen, ob dieses Ticket eines verwendet. Gegenüber
dem Nutzer immer „Projektmanagement-Repo“ schreiben, nie „PM-Repo“.

Mit Projektmanagement-Repo: Die `index.jsonc` jedes Repos in .ocean ansehen
und das finden, zu dem das Ticket gehört – dessen Summary oder Domain
handelt von Projektmanagement oder Planung, nicht von Code. Passt eines,
es dem Nutzer vorschlagen. Passt keines, den Nutzer fragen, welches Repo
verwendet oder ob ein neues angelegt werden soll.

Nach Bestätigung durch den Nutzer es **vor** allen anderen Repos zum Ticket
hinzufügen:

```bash
gg do add pm_repo
```

Hat es `doc/en/guides/pm-repo-guide.md`, diese lesen: Sie beschreibt, wie das
Repo aufgebaut ist und wie dort geplant wird. Existiert die Datei nicht, den
Leitfäden folgen, die das Repo hat.

Ohne Projektmanagement-Repo: Diesen Schritt überspringen und mit den
Git-Repositories weitermachen. Das Ticket trägt dann keinen eigenen Plan.

## Git-Repositories hinzufügen <a id="add-git-repositories"></a>

Den Nutzer fragen, wie die Repos ausgewählt werden – als eine einzige Frage
mit den Optionen unten. **Nur das Projektmanagement-Repo** nur anbieten,
wenn oben eines hinzugefügt wurde – ohne eines gibt es kein „nur“.

Manuell: In einem zweiten einzelnen Eingabefeld fragen, welche Repos das
Ticket braucht, z. B. `repo1 repo2`. Jeden Namen gegen die Repos in .ocean
prüfen und bei Namen, die dort nicht existieren, erneut fragen. Die
eingegebene Liste ist die Bestätigung; die Repos wie unten gezeigt
hinzufügen.

Automatisch: Die `index.jsonc` jedes Repos in .ocean ansehen und
entscheiden, welche Repos zum Ticket hinzugefügt werden müssen.
Einen Plan machen, wie das Ticket grob umgesetzt werden soll.
Den Plan gegen den Scope jedes Repos prüfen (siehe
[Jedes Repo in seinem Scope halten](#keep-every-repo-in-its-scope)) und
überall dort, wo ein Teil nicht passt, eine Aufteilung, ein neues Repo oder
eine Verschiebung vorschlagen.
Dem Nutzer außerdem erklären, was grob in welchem Repo geändert werden soll,
um das Ticket umzusetzen, und bestätigen lassen, dass die entsprechenden
Repos zum Ticket hinzugefügt werden.

Nur das Projektmanagement-Repo: überhaupt kein Code-Repo; das Ticket bleibt
reine Planung, und das oben hinzugefügte Projektmanagement-Repo ist alles,
was es enthält. Wird das Ticket nur geplant (siehe unten), ist das der
richtige Weg – das Projektmanagement-Repo darf das einzige Repo des Tickets
sein.

Nach Bestätigung durch den Nutzer die Repos zum Ticket hinzufügen:

```bash
gg do add repo1 repo2
```

Alle Code-Repos in diesem einen Aufruf nennen, nie ein `gg do add` pro
Repo – auch nicht für Repos, die nacheinander gefunden werden. Jeder Aufruf
aktualisiert den Ocean und baut den Abhängigkeitsgraphen neu auf, und ein
Ticket-Repo, das von mehreren neuen Repos abhängt, wird bei jedem Aufruf
neu aufgelöst. Dasselbe gilt für Repos, die später beim Implementieren
gefunden werden: Sie gemeinsam hinzufügen.

## Jedes Repo in seinem Scope halten <a id="keep-every-repo-in-its-scope"></a>

Jedes Repo deckt einen festen Scope ab: `summary` und `domain` seiner
`index.jsonc`. Hat ein Repo keine, den Scope aus seiner README.md nehmen und
die `index.jsonc` mit der ersten Änderung des Tickets in diesem Repo anlegen
(siehe
[Die index.jsonc aktuell halten](#keep-the-indexjsonc-up-to-date)).
Code außerhalb dieses Scopes verwässert das Repo. Den Scope immer prüfen,
wenn entschieden wird, wohin Code gehört – bei der Auswahl der Repos, beim
Implementieren und vor dem Review – und von sich aus, ohne Aufforderung,
eine Änderung vorschlagen, sobald Code nicht in das Repo passt, in dem er
liegt oder landen soll:

- **Repo aufteilen** – das Repo hat einen zweiten Scope entwickelt:
  vorschlagen, es in zwei Repos mit je eigenem Scope aufzuteilen
- **Neues Repo hinzufügen** – der Code gehört zu einer Domain, die kein Repo
  in .ocean abdeckt: vorschlagen, eines mit `gg do create repo` anzulegen,
  das es zum Ticket hinzufügt
- **Vorhandenes Repo hinzufügen** – der Code gehört zu einem Repo in .ocean,
  das noch nicht Teil des Tickets ist: vorschlagen, es mit `gg do add`
  hinzuzufügen, mehrere Repos in einem Aufruf
- **Code verschieben** – vorhandener oder gerade geschriebener Code gehört
  in ein anderes Repo: vorschlagen, ihn dorthin zu verschieben

Für jeden Vorschlag den betroffenen Code nennen, das Repo, in dem er liegt,
das Repo, in das er gehört, und warum er besser zu dessen Scope passt. Dann
den Nutzer entscheiden lassen:

- **Jetzt** – innerhalb dieses Tickets ändern: das Repo hinzufügen oder
  anlegen, den Code verschieben und Abhängigkeiten und Imports beider Seiten
  anpassen
- **Später** – als Folgeticket festhalten: im Projektmanagement-Repo, falls
  das Ticket eines hat, sonst im Ticket. Mit dem Code dort weitermachen, wo
  er ist.

Ändert sich der Scope eines Repos, die `index.jsonc` jedes betroffenen Repos
aktualisieren (siehe
[Die index.jsonc aktuell halten](#keep-the-indexjsonc-up-to-date)): Ein
neues oder abgespaltenes Repo erhält eigene `summary` und `domain`, das
Repo, das Code abgibt, wird entsprechend eingeengt, und `interfaces.uses`
und `interfaces.usedBy` folgen den neuen Abhängigkeiten.

Nie ohne Bestätigung des Nutzers ein Repo aufteilen, eines hinzufügen oder
anlegen oder Code verschieben.

## Den Workspace in VS Code öffnen – nur auf Wunsch <a id="open-the-workspace-in-vscode--only-on-request"></a>

VS Code nicht von sich aus öffnen – viele Tickets brauchen keinen Editor.
Dem Nutzer sagen, dass er den Workspace aus dem Ticket-Ordner mit folgendem
Befehl öffnen kann, und ihn nur ausführen, wenn der Nutzer darum bittet:

```bash
gg do code
```

## Planen (optional) <a id="plan-optional"></a>

Anforderungen kommen vor dem Plan. Führt das Projektmanagement-Repo
Anforderungen (`pm/<lang>/<yyyy>-Q<n>/requirements`, im neuesten
Quartalsordner) und deckt keine seiner akzeptierten Anforderungen das
Ticket ab, diese zuerst mit dem Nutzer erarbeiten – mit
`/gg-plan-requirements`, falls das Repo es hat – und das Ticket erst danach
planen. Bug-Tickets sind die Ausnahme, sie nennen ihre Bug-Datei.

Den Nutzer fragen, ob das Ticket vor der Implementierung geplant wird.

Falls ja, den Plan ins Projektmanagement-Repo schreiben, nach dessen
`doc/en/guides/pm-repo-guide.md`, falls vorhanden, sonst nach seinen anderen
Leitfäden: das Ziel, die Anforderungen, die es erfüllt, die betroffenen
Repos, die groben Schritte und offene Fragen. Ohne Projektmanagement-Repo
den Plan in das Ticket selbst schreiben. Den Nutzer den Plan prüfen lassen
und ihn überarbeiten, bis er ihn bestätigt.

## Implementieren (optional) <a id="implement-optional"></a>

Den Nutzer fragen, ob das Ticket jetzt implementiert oder nur geplant wird.
Manche Tickets werden nur geplant; die Implementierung folgt in einem
späteren Ticket.

- Implementieren: Die Features anhand der Leitfäden implementieren.
  Code-Kommentare und Dokumentation einer Abhängigkeit in ihren eigenen
  Begriffen schreiben, ohne ihre Eltern (siehe
  [Jedes Repo in seinen eigenen Begriffen beschreiben](#describe-every-repo-in-its-own-terms)).
  Jedes Repo beim Programmieren in seinem Scope halten: anhalten und eine
  Aufteilung, ein neues Repo oder eine Verschiebung vorschlagen, sobald Code
  nicht passt (siehe
  [Jedes Repo in seinem Scope halten](#keep-every-repo-in-its-scope)).
  Hat das Repo `doc/en/guides/architecture-guide.md`, vor jeder Änderung
  entscheiden, wohin der Code gehört, wie dessen `@placement` es beschreibt
- Nur planen: Diesen Schritt überspringen. Die folgenden Schritte gelten
  dann nur für das Projektmanagement-Repo.

## Die index.jsonc aktuell halten <a id="keep-the-indexjsonc-up-to-date"></a>

Die `index.jsonc` eines Repos beschreibt, was das Repo ist: seine `summary`,
seine `domain` und seine `interfaces`. Andere Agenten wählen anhand dieser
Datei die Repos eines Tickets aus, deshalb muss sie jederzeit zum Code
passen, nicht erst beim Review.

Bei jeder Code-Änderung in einem Repo prüfen, ob seine `index.jsonc` noch
passt, und sie im selben Schritt aktualisieren:

- `summary` und `domain` – wenn das Repo etwas Neues kann, etwas nicht mehr
  tut oder sich sein Scope geändert hat
- `interfaces.uses` – wenn das Repo ein anderes Repo zu nutzen beginnt oder
  aufhört oder eine andere API davon nutzt
- `interfaces.usedBy` – wenn ein anderes Repo dieses zu nutzen beginnt oder
  aufhört; auch `uses` jenes Repos aktualisieren, damit beide Seiten
  übereinstimmen

Ein Repo ohne `index.jsonc` erhält eine mit der ersten Änderung des
Tickets, egal welche Änderung: `doc/en/templates/index-template.jsonc` nach
`index.jsonc` kopieren und aus README.md und Code befüllen. Nie ein Repo des
Tickets ohne eine lassen.

`doc/en/guides/index-guide.md` befolgen: Stichworte statt Sätze, nur die
Felder des Templates. Existiert die Datei, braucht eine Änderung, die alle
drei Felder zutreffend lässt – ein Bugfix, ein Refactoring innerhalb des
Repos – keine Aktualisierung.

## Jedes Repo in seinen eigenen Begriffen beschreiben <a id="describe-every-repo-in-its-own-terms"></a>

Ein Ticket ändert oft ein Repo zusammen mit einer seiner Abhängigkeiten: Das
Eltern-Repo nutzt die Abhängigkeit. Die Abhängigkeit kennt ihre Eltern
nicht, deshalb beschreibt alles, was in sie geschrieben wird, nur ihre
eigene Änderung – nie, warum ein Elternteil sie braucht. Das gilt für alles,
was in die Abhängigkeit geschrieben wird:

- Commit-Messages, `nextCommitMessage` und `mergeMessage` – und mit ihnen
  der Eintrag in `CHANGELOG.md` und der Titel des Pull Requests
- Blogposts
- Code-Kommentare und Dokumentation
- ihre `index.jsonc`, außer `interfaces.usedBy`, das die Eltern bewusst
  nennt

Dort nie ein Eltern-Repo nennen, weder das Ziel des Tickets noch den
Anwendungsfall des Elternteils. Das Elternteil wiederum darf die
Abhängigkeit nennen und was es von ihr nutzt.

- Falsch in `gg_git`: `Add remote creation for gg do create repo`
- Richtig in `gg_git`: `Add a function that creates a remote repository`
- Richtig in `gg`: `Create the remote repository with gg_git`

Welches Repo von welchem abhängt, steht in `interfaces.uses` der
`index.jsonc` und in `pubspec.yaml` oder `package.json`.

## Die Publish-Konfiguration aktuell halten <a id="keep-the-publish-config-up-to-date"></a>

Jedes Repo des Tickets hat eine Datei `.gg/publish_config.json`. gg bietet
ihre Werte als Vorgaben an: `gg do commit` befüllt den Editor für die
Commit-Message mit `nextCommitMessage` vor, `gg do publish` wählt
`versionIncrement` vor und befüllt die Merge-Message mit `mergeMessage`
vor. git ignoriert die Datei.

Bei jeder Code-Änderung in einem Repo `nextCommitMessage` dieses Repos so
aktualisieren, dass sie alles beschreibt, was noch nicht committet ist,
nicht nur den letzten Schritt. Die Datei anlegen, falls sie nicht existiert.
Nur die Schlüssel ändern, die geändert werden sollen, und den Rest
behalten – `commits` schreibt gg, nie von Hand.

```json
{
  "publishConfig": {
    "nextCommitMessage": {
      "firstLine": "Let the user pick the ticket repos",
      "details": ["Ask whether repos are chosen automatically or manually"]
    },
    "mergeMessage": "Let the user pick the ticket repos manually",
    "versionIncrement": "minor"
  }
}
```

- `firstLine`: Imperativ, höchstens 60 Zeichen, in den Begriffen des Repos
  selbst (siehe [Jedes Repo in seinen eigenen Begriffen beschreiben](#describe-every-repo-in-its-own-terms))
- `details`: optional, ein Eintrag pro Zeile
- `mergeMessage`: der Titel des Pull Requests und die Merge-Commit-Message
  des Repos
- `versionIncrement`: `patch`, `minor` oder `major`, passend zu dem, was das
  Ticket im Repo ändert

`mergeMessage` und `versionIncrement` setzen, sobald die Änderung des Repos
klar ist, und anpassen, wenn sie sich ändert. Nur `gg do commit` im
Ticket-Ordner liest `nextCommitMessage`; `gg one do commit` nicht.

## Committen <a id="commit"></a>

Aus dem Ticket-Ordner committen, alle Repos des Tickets auf einmal:

```bash
gg can commit
gg do commit --no-interactive
```

`gg can commit` prüft die Repos parallel und speichert das Ergebnis jedes
einzelnen, sodass `gg do commit` es nur bestätigt und committet. Mit
`--no-interactive` erhält jedes Repo seine eigene `nextCommitMessage` (siehe
[Jedes Repo in seinen eigenen Begriffen beschreiben](#describe-every-repo-in-its-own-terms))
und nichts wird gefragt; Repos ohne eine erhalten die Ticket-Beschreibung.

Nie mit `gg one can commit` oder `gg one do commit` über die Repos
schleifen: Die Schleife prüft ein Repo nach dem anderen und dauert ein
Vielfaches. Die Form `gg one` ist für ein eigenständiges Repo gedacht, das
zu keinem Ticket gehört.

`gg can commit` stoppt beim ersten roten Repo. Die Ursache beheben und es
dann erneut für das ganze Ticket ausführen – die Repos, die vorher bestanden
haben, bestätigen nur ihr Ergebnis.

`-m"<message>"` nur verwenden, wenn jedes Repo dieselbe Message erhalten
soll und kein Repo des Tickets von einem anderen abhängt. `gg do commit`
ohne `--no-interactive` öffnet die Messages aller Repos in einem Editor,
den der Nutzer bestätigen muss – diesen Weg dem Nutzer überlassen.

## Das Dart-SDK aktualisieren <a id="upgrade-the-dart-sdk"></a>

Soll ein Dart-Repo das neueste SDK verwenden, `environment: sdk:` in
`pubspec.yaml` nie von Hand anheben. Zuerst das neueste Dart-SDK
installieren, dann im Repo ausführen:

```bash
dart --version   # e.g. Dart SDK version: 3.14.0
dart migrate --apply --target-sdk=3.14.0
```

`dart migrate` hebt die Untergrenze der SDK-Constraint eine Sprachversion
nach der anderen an und wendet die Fixes an, die jeder Schritt braucht. Ohne
`--target-sdk` geht es nur eine Version weiter; die installierte Version
übergeben, um sie in einem Lauf zu erreichen – darüber hinaus geht es
nicht. `--dry-run` statt `--apply` zeigt die Änderungen zuerst. Danach das
Ticket wie unter [Committen](#commit) beschrieben prüfen und committen.

## Pushen <a id="push"></a>

```bash
gg do push
```

`gg do push` merged die Main-Branches in die Feature-Branches, prüft und
pusht. Es aktualisiert keine Abhängigkeit: Neue Versionen, Major-Versionen
eingeschlossen, kommen über den Nightly-Build (`gg do maintain --nightly`).
Braucht das Ticket sofort eine neuere Version einer Abhängigkeit? Deren
Constraint als Teil der Änderung anheben oder `gg do push --upgrade`
ausführen.

## Review

Zuerst den Diff jedes Repos des Tickets gegen seinen Scope prüfen (siehe
[Jedes Repo in seinem Scope halten](#keep-every-repo-in-its-scope)) und
für alles, was nicht passt, eine Aufteilung, ein neues Repo oder eine
Verschiebung vorschlagen.

Dann die Änderung in jedem Repo dokumentieren, damit die Dokumentation in
den Pull Requests landet und mit dem Code reviewt wird:

- `/blog` in jedem Repo des Tickets ausführen, das
  `doc/en/guides/blog-guide.md` hat, das Projektmanagement-Repo
  eingeschlossen. Der Post einer Abhängigkeit beschreibt ihre eigene
  Änderung, ohne ihre Eltern (siehe
  [Jedes Repo in seinen eigenen Begriffen beschreiben](#describe-every-repo-in-its-own-terms))
- Die README.md aktualisieren und noch einmal prüfen, dass jedes Repo eine
  `index.jsonc` hat und dass sie zum Code des Tickets passt
- Erneut committen und pushen

Den Nutzer bestätigen lassen, dass die Review-Phase gestartet wird.

```bash
gg do review
```

gg legt für jedes Repo Pull Requests an und gibt die URLs im Terminal aus.
Jede URL an den Nutzer ausgeben, eine Zeile pro Repo. Die Pull Requests
nicht im Browser öffnen – das dem Nutzer überlassen.

Danach den Skill review-light laden und ausführen.

## Veröffentlichen <a id="publish"></a>

- `mergeMessage` und `versionIncrement` in `.gg/publish_config.json` jedes
  Repos prüfen – `gg do publish` bietet sie als Vorgaben an
- Wurde das Ticket nur geplant, nur das Projektmanagement-Repo
  veröffentlichen

Den Nutzer bitten, folgenden Befehl **manuell** aus dem
Workspace-Wurzelordner auszuführen, nicht aus dem Ticket-Ordner:

```bash
cd ~/dev/ # workspace root
gg do publish -i fix_login_crash
```

Am Ende verschiebt gg den Ticket-Ordner in den Papierkorb. Windows
verweigert das Verschieben eines Ordners, in dem ein Terminal steht („used
by another process“, errno 32), deshalb muss das Terminal außerhalb des
Tickets bleiben – `-i` benennt stattdessen das Ticket.

gg löst den Merge der Pull Requests aus und veröffentlicht die Änderungen in
der Registry. Zum Schluss wird der Versions-Tag angelegt und gepusht.

Ein Publish löst die Abhängigkeiten auf (`dart pub get`, `<pm> install`) und
analysiert; er aktualisiert nichts und führt die Tests nicht erneut aus –
sie sind beim Commit bestanden. Nur die Constraints auf Repos, die dasselbe
Ticket veröffentlicht, ändern sich.
