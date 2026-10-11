# SpeedMQ Upgrade- und Migrationsplan

> Gilt für Version: `1.0.0` (`broker.Version`, siehe `speedmq_build_info` unter `/metrics`).
> Alle „gemessenen“ Aussagen in diesem Dokument stammen aus echtem Betrieb auf diesem Rechner; alles nicht Gemessene ist ausdrücklich mit **【Nicht verifiziert】** gekennzeichnet.
> Umgebung dieses Rechners: Windows + PowerShell 5.1, Go 1.27.1 windows/386, temporäres `data_dir` + Nicht-Standard-Ports.

---

## 1. Migration (Umstieg von RabbitMQ auf SpeedMQ)

Dieses Projekt ist auf **Kompatibilität auf AMQP-0-9-1-Protokollebene** ausgelegt, daher besteht die „Migration“ im Wesentlichen aus **dem Ändern der Verbindungsadresse**:

- Keine Änderung am Anwendungscode, nur `host/port/vhost` anpassen (Designdokument G3 „Migration ohne Kosten“).
- Die Management-Toolchain (`rabbitmqadmin`, Management-UI, Monitoring-Skripte) muss lediglich auf den Management-Port zeigen; die Schnittstellenform ist an RabbitMQ angeglichen (Konventionen wie `amq.default`, `%2F`, `{error, reason}` werden übernommen).
- Standardports sind mit RabbitMQ identisch: AMQP `5672`, Management-Ebene `15672`; MQTT ist `1883`, das knotenübergreifende RPC `25672`.

**Vor der Migration selbst zu prüfende Semantikunterschiede** (alle bewusst in diesem Repository so umgesetzt, gemäß README / Designdokument):

| Punkt | SpeedMQ-Verhalten | Auswirkung auf die Migration |
| --- | --- | --- |
| Transiente (nicht persistente und nicht exklusive) Queue | **Deklaration wird abgelehnt** (541), `auto_delete` befreit nicht | Alte Clients, die auf solche Queues angewiesen sind, schlagen fehl; müssen auf durable oder exclusive umgestellt werden |
| Standard-vhost `/` | **nicht löschbar** (400), RabbitMQ erlaubt dies | Automatisierungsskripte, die den Standard-vhost löschen, schlagen fehl (dies ist die einzige aktive Sicherheitsbeschränkung) |
| Daten klassischer Queues | werden **nicht repliziert**, die Daten liegen nur auf dem Owner-Knoten | Für knotenübergreifende Redundanz auf Quorum-Queues `x-queue-type=quorum` umsteigen |
| Quorum-Queues | Skalierung der Replikate nach oben wird unterstützt, **Verkleinerung wird nicht unterstützt** | bei der Planung gleich richtig dimensionieren |
| Plugins | kein Erlang-Plugin-Ökosystem, AMQP 1.0 / STOMP nicht implementiert | Szenarien, die diese Protokolle nutzen, sind derzeit nicht migrierbar |

**Datenmigration**: Die Speicherformate von SpeedMQ und RabbitMQ sind nicht kompatibel; es wird **kein Online-/Offline-Werkzeug zum Datentransfer angeboten**.
Das Vorgehen ist „leeres SpeedMQ neu aufsetzen → Parallelbetrieb und Verifikation → schrittweise Umschaltung des Datenverkehrs“. **【Nicht verifiziert】** Dieses Dokument enthält keinen echten RabbitMQ-Datentransfer-Durchlauf.

---

## 2. Grundprinzipien des Upgrades

1. **Zuerst sichern** (siehe `backup-restore.md`) – als Absicherung für einen fehlgeschlagenen Upgrade.
2. **Erst den Prozess stoppen, dann ersetzen** (für das Datenverzeichnis gilt ein Single-Writer-Prinzip, siehe §4.2).
3. **Nach dem Upgrade muss verifiziert werden**: Prozess startet, `/api/overview` ist lesbar, `/metrics` ist abrufbar, die Queue-Nachrichtenzahl stimmt mit der vor der Sicherung überein.
4. Cluster-Upgrades erfolgen **rollierend Knoten für Knoten**, es wird jeweils nur ein Knoten bearbeitet (siehe §5).

---

## 3. Layout des Datenverzeichnisses (Faktenbasis für Upgrade/Migration)

Das auf diesem Rechner im Einzelknotenbetrieb **gemessene** `data_dir`-Layout:

```
data/
├── meta/
│   ├── state.json        # Metadaten-Snapshot im Einzelknotenmodus (vhost/Exchanges/Queues/Bindings/Benutzer/Berechtigungen/Policies)
│   ├── users.seeded      # Bootstrap-Markierung: die users aus der Konfigurationsdatei wurden bereits eingespielt
│   ├── vhosts.seeded     # Bootstrap-Markierung: die vhosts aus der Konfigurationsdatei wurden bereits eingespielt
│   ├── raft.state        # 【Clustermodus】Raft-Amtszeit/Wahlstimmen
│   ├── raft.log          # 【Clustermodus】Raft-Log
│   └── snapshot.json     # 【Clustermodus】Raft-Snapshot + Mitgliedertabelle
├── msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/
│   ├── 000001.seg        # Segmentdatei (Nachrichteninhalt + Eigenschaften), Datensatzformat: <len u32><crc32 u32><payload>
│   └── index/000001.idx  # Queue-Index: seq-id → (Segmentnummer, Offset im Segment, Länge, Status)
└── quorum/<safe(vhost)>/<safe(queue)>/   # 【Cluster】je Queue eine Raft-Gruppe für Quorum-Queues (Log/Snapshot)
```

**Hinweis (zwei Punkte, die der Intuition widersprechen; maßgeblich sind Code/Messung)**:

- Im Clustermodus liegen die Raft-Persistenzdateien **direkt unter `meta/`** (`raft.state` / `raft.log` / `snapshot.json`),
  **es gibt kein Unterverzeichnis `meta/raft/`**. Beleg: die Dateinamenkonstanten in `internal/raft/log.go` + in `internal/broker/cluster.go`
  `Dir: filepath.Join(b.cfg.DataDir, "meta")`. **【Verifiziert（2026-10-10, lokaler Docker-Desktop-3-Knoten-Cluster）】** —— Der Inhalt der Volume-Sicherung der Cluster-Knoten zeigt direkt `meta/raft.log`, `meta/raft.state`, `meta/*.seeded` (kein Unterverzeichnis `meta/raft/` sichtbar).
- Die Verzeichnisnamen sind **nicht die ursprünglichen vhost-/Queue-Namen**, sondern eine `store.SafeDirName`-Kodierung: Präfix `q_`, Bytes außerhalb `[A-Za-z0-9._-]` werden als `%XX` escaped.
  Messung: vhost `/` → Verzeichnis `q_%2F`, Queue `persist.q` → Verzeichnis `q_persist.q`.
  Diese Gestaltung dient der Vermeidung von Path Traversal und Windows-reservierten Gerätenamen (`con`/`nul` usw.).

---

## 4. Datenkompatibilität

### 4.1 Können alte Daten direkt gelesen werden – ja

- **Indexformat vorwärtskompatibel**: M8-1 hat dem Index-Datensatz ein Feld „Segmentnummer“ hinzugefügt (25 Bytes); **das alte Format (21 Bytes, ohne Segmentnummer) kann weiterhin unverändert gelesen werden**,
  beim Lesen entspricht es „es gibt nur ein Segment (seg=1)“, **das Upgrade erfordert kein Migrationsskript**.
  Beleg: die Konstanten `indexEntrySize` / `legacyIndexEntrySize` in `internal/store/store.go` und die `recover()`-Logik; README M8-1.
- **Crash-Semantik unverändert**: Jeder Datensatz hat ein Längenpräfix + CRC32; bei der Wiederherstellung werden **halb geschriebene/beschädigte Datensätze am Ende verworfen** und die Datei wird abgeschnitten.
  Messung (siehe `backup-restore.md` §6): Nach dem Stoppen und Neustarten des Prozesses werden **alle** 5 persistenten Nachrichten der durable-Queue **wiederhergestellt**,
  im Log erscheint `已从磁盘恢复队列消息 ... messages=5`.

**【Verifiziert（2026-10-10, lokaler Rechner）】** Das Lesen über Hauptversionen hinweg wurde gemessen: Die mit dem Tag `v1.1.0` geschriebenen Daten einer durable-Queue werden unter `1.1.05` unverändert wiederhergestellt (Befehle und Ausgabe siehe §5).

### 4.2 Die Handhabung von `vhosts` / `users` in der Konfiguration (die häufigste Stolperfalle beim Upgrade)

- Beide werden **nur beim ersten Bootstrap wirksam**: Beim ersten Start werden die vhosts/users aus der Konfiguration in die Metadaten geschrieben und die Markierungsdateien
  `meta/vhosts.seeded` / `meta/users.seeded` abgelegt; **danach sind die Metadaten maßgeblich**.
- Daher **solltest du beim Upgrade/Wechsel der Konfiguration nicht darauf setzen, Konten oder vhosts durch Ändern der Konfigurationsdatei hinzuzufügen oder zu entfernen** – Änderungen werden nicht wirksam;
  verwende stattdessen die Management-API oder `speedmqctl`.
- Umgekehrt überschreibt das Upgrade **nicht** bestehende Konten mit der Konfiguration: Ein zur Laufzeit geändertes Passwort wird beim Neustart nicht auf den alten Wert aus der Konfiguration zurückgesetzt,
  und ein zur Laufzeit gelöschtes Konto wird nicht wiederbelebt. Beleg: die Seeding-Markierungslogik in `cluster.go`; README M8-4 / M8-7.

**【Verifiziert（2026-10-10, lokaler Rechner）】** Änderungen zur Laufzeit werden tatsächlich nicht von der Konfiguration überschrieben: Vor dem Upgrade wurde das Passwort von `guest` über die Management-API auf `rotated` geändert; nach dem Neustart mit `1.1.05` + demselben `data_dir` liefert `guest/guest` (das alte Passwort aus der Konfiguration) **401** und `guest/rotated` **200**.

### 4.3 Segment-Rotation und Speicherfreigabe

- Nachrichten werden nach Größe in Segmente aufgeteilt (Standard 8 MiB); **sobald alle Nachrichten im Segment ack-bestätigt und das Segment abgeschlossen ist, wird das gesamte Segment gelöscht** und der Index entsprechend komprimiert und neu geschrieben.
- Das Upgrade ändert dieses Verhalten nicht; eine von der alten Instanz hinterlassene einzelne Segmentdatei funktioniert unter der neuen Segment-Rotationslogik unverändert.

---

## 5. Binary-Upgrade (Bare-Metal)

> **【Verifiziert（2026-10-10, lokaler Rechner）】** Versionsübergreifender Test auf echter Hardware: Das mit dem Tag `v1.1.0` gebaute alte Binary schrieb eine durable Exchange/Queue/Bindung + 5 persistente Nachrichten + vhost/Konto/Berechtigungen/Policies und änderte zur Laufzeit das Passwort; nach dem Stoppen des Prozesses wurde mit dem aktuellen `1.1.05`-Binary + **demselben `data_dir` + demselben Port** gestartet —— der Prozess startet normal, die Objekte und Nachrichtenzahlen von `/api/queues` stimmen, ein echter AMQP-Client holt alle 5 persistenten Nachrichten zurück, und `/metrics` meldet `speedmq_build_info{version="1.1.05"}`. Die folgenden Schritte bleiben ein allgemeines Vorgehen und lassen sich auf beliebige benachbarte Versionen anwenden; **Downgrade** siehe §8.

### 5.1 Schritte

```powershell
$base = "C:\speedmq"
$data = "$base\data"

# 1) Prozess stoppen (sauberes Herunterfahren führt ein abschließendes Flush durch; siehe §4 „Konsistenz“)
#    Läuft er im Vordergrund: Ctrl+C; als Dienst: Stop-Service / Stop-Process
Stop-Process -Name speedmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) Datenverzeichnis sichern (unbedingt nach dem Stoppen des Prozesses)
Copy-Item -Recurse -Force $data "$base\backup-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) Binary ersetzen (die neue Version von speedmqd.exe / speedmqctl.exe an den ursprünglichen Pfad legen)
#    Copy-Item .\new\speedmqd.exe $base\speedmqd.exe -Force

# 4) Starten
& "$base\speedmqd.exe" -config "$base\configs\speedmqd.json" -log-level info

# 5) Verifikation: Prozess lebt + Management-API ist lesbar
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

### 5.2 Checkliste zur Verifikation nach dem Upgrade

- Im Startlog erscheinen `SpeedMQ 启动中 ... version=<新版本>` und `管理面已启动`;
- Die `object_totals` / `queue_totals` von `/api/overview` stimmen mit dem Zustand vor der Sicherung überein (vergleiche `backup-restore.md` §5);
- Die `messages` / `messages_ready` jeder durable-Queue in `/api/queues` stimmen mit dem Zustand vor der Sicherung überein;
- `/metrics` ist abrufbar und `speedmq_plugin_up{name="amqp091"} 1`, `{name="mqtt"} 1`.

---

## 6. Image-Upgrade (Container)

Das Image ist etwa 13 MB groß (statisch gelinktes Binary + alpine), **läuft als Nicht-root (uid 10001)**, und das Datenverzeichnis ist unter `/var/lib/speedmq` eingebunden.

```powershell
# 1) Neues Image ziehen/bauen (den Tag auf die neue Versionsnummer setzen, um Verwechslung alt/neu zu vermeiden)
docker build -t speedmq:1.0.0 .

# 2) Alten Container stoppen (compose behält das benannte Volume speedmq-data)
docker compose down

# 3) Neue Version starten (im compose-File das image auf den neuen Tag ändern)
docker compose up -d

# 4) Status und Logs
docker compose ps
docker compose logs -f --tail 100
```

> **Einmalige Aufgabe im Container** (z. B. `speedmqctl` im Container ausführen): `run` von `docker compose ...` muss in einer nicht-interaktiven Umgebung mit `-T` versehen werden,
> sonst schlägt es fehl, weil ein TTY angefordert wird:
> ```powershell
> docker compose run -T --rm broker speedmqctl -user guest -pass guest status
> ```

Die Datenpersistenz beruht auf dem **benannten Volume** `speedmq-data` von compose; beim Neuerstellen des Containers gehen keine Daten verloren (seit M4 wirklich auf der Platte).
Muss der Volume-Inhalt vor dem Upgrade gesichert werden, entspricht das der Sicherung von `/var/lib/speedmq` (siehe `backup-restore.md` §3.2).

> **【Verifiziert（2026-10-10, lokales Docker Desktop）】** Es wurde ein echter Image-Upgrade-Durchlauf durchgeführt: Lokal wurde das alte Image `speedmq:1.1.0-old` (git tag `v1.1.0`) und das neue Image `speedmq:1.1.05-new` (aktueller Quellcode) gebaut; mit einem Einzelknoten-compose das alte Image gestartet → durable-Queue `persist.q` angelegt und 5 persistente Nachrichten veröffentlicht (`delivery_mode=2`) → `docker compose down` (**ohne `-v`**, damit das benannte Volume `speedmq-data` erhalten bleibt) → nach Änderung des image-Tags im compose auf das neue Image `docker compose up -d`.
> Ergebnis: Der neue Container startet normal, im Startlog erscheint `已从磁盘恢复队列消息 ... queue=persist.q messages=5`, `/metrics` meldet `speedmq_build_info{version="1.1.05"}`, `queue_totals.messages=5` von `/api/overview` und `messages=5` der Queue `persist.q` stimmen vor und nach dem Upgrade überein, und ein echter AMQP-Client (`rabbitmq/amqp091-go`) holt alle 5 persistenten Nachrichten einzeln zurück (`deliveryMode=2`).
> Hinweis: `v1.1.0` ist das Image der Marke `speedmq` von vor der Umbenennung (Binary `speedmqd`, Metrik `speedmq_build_info`, Standarddatenverzeichnis `/var/lib/speedmq`) und unterscheidet sich in der Benennung von der aktuellen Version; im Durchlauf galt die Konfiguration `data_dir=/var/lib/speedmq`, und für das benannte Volume wurde der Eigentümer vorab gesetzt (im alten Image gibt es diesen beschreibbaren Pfad nicht). Dies beeinträchtigt die Schlussfolgerung „Container per Tag-Wechsel neu erstellen, benannte Volume-Daten gehen nicht verloren“ nicht.

---

## 7. Schrittweise Umstellung und Rollback

### 7.1 Einzelknoten

- **Schrittweise Umstellung**: SpeedMQ bietet im Einzelknotenbetrieb keine eingebaute Fähigkeit für „alte und neue Version im selben Prozess“. Eine praktikable schrittweise Umstellung ist ein **Bypass-Schatten**:
  Die neue Instanz wird zunächst über **Read-Only-Konsum/Schatten-Queues** an denselben Upstream-Datenverkehr gehängt und beobachtet; erst nach Bestätigung wird der Schreibende umgeschaltet.
- **Rollback**:
  1. den Prozess der neuen Version stoppen;
  2. das alte Binary zurücktauschen;
  3. hat die neue Version bereits Daten geschrieben, **muss `data_dir` aus der Sicherung vor dem Upgrade wiederhergestellt werden** (siehe unten).
  Es gibt **keine** Garantie für „neue Version hat geschrieben, alte Version liest direkt“ – versionsübergreifender Downgrade siehe §8.

### 7.2 Cluster (rollierendes Upgrade)

Die Plattformseite bietet kein „Rolling-Upgrade mit einem Klick“; es muss manuell Knoten für Knoten in der folgenden Reihenfolge vorgegangen werden:

1. **Immer nur einen Knoten auf einmal upgraden**: Knoten stoppen → sein `data_dir` sichern → Binary tauschen → starten → warten, bis er wieder beitritt und aufholt
   (`speedmqctl cluster_status` / `GET /api/cluster` auf `role`, `commit_index`/`last_applied` prüfen).
2. **Empfohlene Reihenfolge**: zuerst **learner / nicht stimmberechtigte Mitglieder** (ohne Auswirkung auf die Mehrheit) upgraden, dann **follower**, zuletzt **leader**
   (das Upgrade des leader löst eine Leader-Wahl aus, während der kurze Zeit nicht geschrieben werden kann).
3. **Auswirkung der Stilllegung auf die Mehrheit** (entscheidend):
   - 3-Knoten-Cluster: **gleichzeitig höchstens 1** stimmberechtigtes Mitglied stoppen; beim Stoppen von 2 geht die Mehrheit verloren, unter `pause_minority` **wird der gesamte Cluster-Dienst ausgesetzt**.
   - 2-Knoten-Cluster: Beim Stoppen von 1 geht bereits die Mehrheit verloren, **keine Fähigkeit zum rollierenden Upgrade** (mindestens 3 Knoten empfohlen).
   - Daher beim rollierenden Upgrade **strikt verboten, mehrere stimmberechtigte Mitglieder auf einmal zu stoppen**.
4. **Mitgliederänderungen und Upgrade nicht parallel durchführen**: Mitgliederänderungen haben **kein Joint Consensus**, es ist jeweils nur eine nicht committete Konfigurationsänderung erlaubt;
   vermeide während des Upgrades gleichzeitig `add_member` / `remove_member`.
5. Nach Abschluss des Upgrades prüfen, dass `object_totals` von `GET /api/cluster` mit dem Zustand vor dem Upgrade übereinstimmt.

> **【Verifiziert（2026-10-10, lokaler Docker-Desktop-3-Knoten-Cluster）】** Es wurde ein echter **versionsübergreifender** rollierender Upgrade-Durchlauf eines echten Clusters durchgeführt: Basis war das lokal gebaute `speedmq:1.1.0-old` (git tag `v1.1.0`), Knoten für Knoten (n1 → n2 → n3, jeweils einer: erst stoppen, dann sein benanntes Volume sichern, dann Image tauschen und starten) auf das aktuelle `1.1.05` (lokal gebaut `speedmq:1.1.05-new`, compose wechselte das Image per knotenweisem Override). Während des gesamten Vorgangs behielt der Cluster stets einen leader und die Mehrheit — beim Stoppen des aktuellen leader wurde in etwa 0,6 s ein neuer leader gewählt, `pause_minority` löste keine Aussetzung aus, und während der Stilllegung gab es keine externe Unterbrechung; nach dem Upgrade stimmen die `object_totals` von `GET /api/cluster` mit dem Zustand davor überein (`queues:1`), die 25 persistenten Nachrichten der 3-Replikat-Quorum-Queue `qroll` bleiben in der Zahl unverändert und sind einzeln rücklesbar, und nach Veröffentlichung von 5 weiteren Nachrichten sind alle 30 abrufbar.
> Die obige Reihenfolge gilt weiterhin aufgrund der allgemeinen Raft-Beschränkungen; dieser Durchlauf deckt das 3-Knoten-Szenario ab. **【Verifiziert（2026-10-10, lokales Docker Desktop）2-Knoten: Stopp von 1 verliert bereits die Mehrheit】** Ein zusätzlicher minimaler 2-Knoten-Cluster (n1/n2, deren `peers` nur diese beiden enthalten, `partition_policy=pause_minority`) wurde gemessen: Bei beiden online ist `GET /api/cluster` `has_quorum=true / paused=false` (n1 leader, n2 follower); nach `docker stop` von n2 wird n1 innerhalb von etwa 2 s zu `has_quorum=false / paused=true` —— d. h. schon das Stoppen von 1 stimmberechtigten Mitglied verliert die Mehrheit, `pause_minority` setzt den Dienst aus, **was bestätigt, dass 2 Knoten keine Fähigkeit zum rollierenden Upgrade haben**; nach `docker start` zur Wiederherstellung von n2 kehrt n1 innerhalb von etwa 2 s zu `has_quorum=true / paused=false` zurück (Dienst erholt sich automatisch).

> **【Verifiziert (2026-10-11, 3-Knoten-Cluster auf lokalem Docker Desktop)】Rollierendes Upgrade unter Netzwerkpartition wurde gemessen** (`v1.1.0` → `1.1.05` versionsübergreifend; die Partition nutzte eine Dual-Netzwerk-Topologie: Raft-RPC zwischen den Knoten läuft nur über `cluster-net` mit statischen IPs, Client-/Management-Traffic über `client-net`, und `docker network disconnect/connect` schaltet die Partition um):
> - **Partitionierter Follower**: Die Mehrheitsseite hielt während der gesamten Partition das Quorum, veröffentlichte/konsumierte normal und vollzog bei bestehender Partition an einem Mehrheitsknoten „Stopp → Image-Tausch → Start → Aufholen“; der Mischversions-Cluster (1.1.0 + 1.1.05) blieb dienstfähig. Nach der Heilung trat der isolierte Knoten (alte Version) wieder bei und holte auf; der Nachrichtenabgleich per Eindeutigkeits-ID-Menge ergab 35/35 ohne Verlust.
> - **Partitionierter Leader**: Die Mehrheit wählte in etwa **630 ms** einen neuen Leader; der isolierte alte Leader behielt `role=leader`, aber mit `has_quorum=false / paused=true` (`pause_minority` wirkt, er erhöht die Term nicht eigeninitiativ); das Upgrade eines weiteren Knotens auf der Mehrheitsseite gelang während der Partition; nach der Heilung trat der alte Leader wieder bei – kein Split-Brain, keine Datendivergenz.
> - **Erwartetes Verhalten ohne PreVote (wahrheitsgemäß protokolliert)**: Der isolierte Follower startete sofort wiederholte Wahlen, seine Term stieg etwa alle 2 s um +2 (die beiden Raft-Gruppen für Metadaten und Queue wechselten sich ab); im Moment der Heilung **riss er mit seiner höheren Term die Führung an sich** (im Drill innerhalb von 1,4 s gewählt) – eine Leader-Wechsel-Störung, aber kein Korrektheitsproblem.
> - **Zwei Betriebsfallen**: ① Wenn `peers` Hostnamen verwendet und die Container zusätzlich an einem zweiten erreichbaren Netzwerk hängen, läuft Raft-Traffic auf Umwegen und erzeugt eine „falsche Partition“ (der isolierte Knoten zeigte nach über einer Minute noch `has_quorum=true`) – Knoten-RPC muss über genau ein Netzwerk laufen (`peers` zeigt direkt auf die statischen IPs dieses Netzwerks); ② bei der Heilung muss `docker network connect` `--ip <ursprüngliche IP>` mitbringen, sonst erhält der Knoten eine neue IP, während die Mehrheit weiter die alte Adresse anwählt, und der isolierte Knoten kämpft mit veraltetem Log und aufgeblähter Term wiederholt um die Wahl (im Drill zog er die Cluster-Term von 320 auf 488 und den Metadaten-Commit von 13 auf 97 hoch), wobei ein transienter Zustand auftreten kann, in dem der Leader `quorum=true` zeigt, aber kurz `paused=true`.
> - Während der Partition blieb die Management-API der Minderheitsseite lesbar (nützlich zur Beobachtung), AMQP-Publish wurde abgelehnt (`pause_minority` wirkt).

---

## 8. Nicht unterstützte / nicht verifizierte Teile (explizit aufgeführt)

- **Downgrade über Hauptversionen hinweg: nicht unterstützt, nicht verifiziert**. Hat die neue Version bereits Daten in neuem Format/neuer Semantik geschrieben, gibt es **keine** Garantie für „Rückfall auf das alte Binary und unverändertes Lesen“;
  ein Rollback ist nur über die Sicherung vor dem Upgrade möglich.
- **Konfigurationsformat unverändert**: weiterhin JSON + `SPEEDMQ_*`-Umgebungsvariablen. **YAML-Konfiguration wird noch nicht unterstützt** (erfordert die Einführung einer Parsing-Abhängigkeit, M8-17 noch zu bewerten),
  das Upgrade bringt kein YAML.
- **Online-Hot-Upgrade von Plugins/Protokollen**: Plugins werden mit dem Kernel kompiliert (Form A) oder gemäß Konfiguration per `spawn` gestartet (Form B);
  Kernel-Upgrade = Prozess-Neustart; es gibt **keinen** Mechanismus zum Hot-Replace des Binarys an Ort und Stelle.
- **Migration der Speicher-Engine an Ort und Stelle**: Segment-Rotation/Index-Komprimierung sind Hintergrundverhalten zur Laufzeit, es gibt **keinen** eigenständigen Befehl „Datenmigration/Komprimierung“.
- **Cluster-Upgrade bei vollgeschriebener Platte**: **nicht verifiziert**. (Das rollierende Upgrade unter Netzwerkpartition wurde am 2026-10-11 gemessen, siehe die Notiz am Ende von §7.)
- Dieses Dokument **enthält keine** Verifikation des Datentransfers zwischen SpeedMQ und anderen Brokern (RabbitMQ).
