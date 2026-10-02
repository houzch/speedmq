# SwiftMQ Upgrade- und Migrationsplan

> Gilt für Version: `1.0.0` (`broker.Version`, siehe `swiftmq_build_info` unter `/metrics`).
> Alle „gemessenen“ Aussagen in diesem Dokument stammen aus echtem Betrieb auf diesem Rechner; alles nicht Gemessene ist ausdrücklich mit **【Nicht verifiziert】** gekennzeichnet.
> Umgebung dieses Rechners: Windows + PowerShell 5.1, Go 1.27.1 windows/386, temporäres `data_dir` + Nicht-Standard-Ports.

---

## 1. Migration (Umstieg von RabbitMQ auf SwiftMQ)

Dieses Projekt ist auf **Kompatibilität auf AMQP-0-9-1-Protokollebene** ausgelegt, daher besteht die „Migration“ im Wesentlichen aus **dem Ändern der Verbindungsadresse**:

- Keine Änderung am Anwendungscode, nur `host/port/vhost` anpassen (Designdokument G3 „Migration ohne Kosten“).
- Die Management-Toolchain (`rabbitmqadmin`, Management-UI, Monitoring-Skripte) muss lediglich auf den Management-Port zeigen; die Schnittstellenform ist an RabbitMQ angeglichen (Konventionen wie `amq.default`, `%2F`, `{error, reason}` werden übernommen).
- Standardports sind mit RabbitMQ identisch: AMQP `5672`, Management-Ebene `15672`; MQTT ist `1883`, das knotenübergreifende RPC `25672`.

**Vor der Migration selbst zu prüfende Semantikunterschiede** (alle bewusst in diesem Repository so umgesetzt, gemäß README / Designdokument):

| Punkt | SwiftMQ-Verhalten | Auswirkung auf die Migration |
| --- | --- | --- |
| Transiente (nicht persistente und nicht exklusive) Queue | **Deklaration wird abgelehnt** (541), `auto_delete` befreit nicht | Alte Clients, die auf solche Queues angewiesen sind, schlagen fehl; müssen auf durable oder exclusive umgestellt werden |
| Standard-vhost `/` | **nicht löschbar** (400), RabbitMQ erlaubt dies | Automatisierungsskripte, die den Standard-vhost löschen, schlagen fehl (dies ist die einzige aktive Sicherheitsbeschränkung) |
| Daten klassischer Queues | werden **nicht repliziert**, die Daten liegen nur auf dem Owner-Knoten | Für knotenübergreifende Redundanz auf Quorum-Queues `x-queue-type=quorum` umsteigen |
| Quorum-Queues | Skalierung der Replikate nach oben wird unterstützt, **Verkleinerung wird nicht unterstützt** | bei der Planung gleich richtig dimensionieren |
| Plugins | kein Erlang-Plugin-Ökosystem, AMQP 1.0 / STOMP nicht implementiert | Szenarien, die diese Protokolle nutzen, sind derzeit nicht migrierbar |

**Datenmigration**: Die Speicherformate von SwiftMQ und RabbitMQ sind nicht kompatibel; es wird **kein Online-/Offline-Werkzeug zum Datentransfer angeboten**.
Das Vorgehen ist „leeres SwiftMQ neu aufsetzen → Parallelbetrieb und Verifikation → schrittweise Umschaltung des Datenverkehrs“. **【Nicht verifiziert】** Dieses Dokument enthält keinen echten RabbitMQ-Datentransfer-Durchlauf.

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
  `Dir: filepath.Join(b.cfg.DataDir, "meta")`. **【Cluster-Layout nicht gemessen】** (auf diesem Rechner lief nur eine Einzelknoten-Instanz).
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

### 4.2 Die Handhabung von `vhosts` / `users` in der Konfiguration (die häufigste Stolperfalle beim Upgrade)

- Beide werden **nur beim ersten Bootstrap wirksam**: Beim ersten Start werden die vhosts/users aus der Konfiguration in die Metadaten geschrieben und die Markierungsdateien
  `meta/vhosts.seeded` / `meta/users.seeded` abgelegt; **danach sind die Metadaten maßgeblich**.
- Daher **solltest du beim Upgrade/Wechsel der Konfiguration nicht darauf setzen, Konten oder vhosts durch Ändern der Konfigurationsdatei hinzuzufügen oder zu entfernen** – Änderungen werden nicht wirksam;
  verwende stattdessen die Management-API oder `swiftmqctl`.
- Umgekehrt überschreibt das Upgrade **nicht** bestehende Konten mit der Konfiguration: Ein zur Laufzeit geändertes Passwort wird beim Neustart nicht auf den alten Wert aus der Konfiguration zurückgesetzt,
  und ein zur Laufzeit gelöschtes Konto wird nicht wiederbelebt. Beleg: die Seeding-Markierungslogik in `cluster.go`; README M8-4 / M8-7.

### 4.3 Segment-Rotation und Speicherfreigabe

- Nachrichten werden nach Größe in Segmente aufgeteilt (Standard 8 MiB); **sobald alle Nachrichten im Segment ack-bestätigt und das Segment abgeschlossen ist, wird das gesamte Segment gelöscht** und der Index entsprechend komprimiert und neu geschrieben.
- Das Upgrade ändert dieses Verhalten nicht; eine von der alten Instanz hinterlassene einzelne Segmentdatei funktioniert unter der neuen Segment-Rotationslogik unverändert.

---

## 5. Binary-Upgrade (Bare-Metal)

> Auf diesem Rechner wurde **kein versionsübergreifender Test auf echter Hardware durchgeführt** (das Repository hat derzeit nur eine Version, `1.0.0`, es gibt kein älteres Binary zum Upgraden). Die folgenden Schritte sind eine **Replay-Verifikation derselben Version + ein allgemeines Vorgehen** für die bereits vorhandenen Fähigkeiten dieses Repositories; versionsübergreifende Teile sind mit **【Nicht verifiziert】** gekennzeichnet.

### 5.1 Schritte

```powershell
$base = "C:\swiftmq"
$data = "$base\data"

# 1) Prozess stoppen (sauberes Herunterfahren führt ein abschließendes Flush durch; siehe §4 „Konsistenz“)
#    Läuft er im Vordergrund: Ctrl+C; als Dienst: Stop-Service / Stop-Process
Stop-Process -Name swiftmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) Datenverzeichnis sichern (unbedingt nach dem Stoppen des Prozesses)
Copy-Item -Recurse -Force $data "$base\backup-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) Binary ersetzen (die neue Version von swiftmqd.exe / swiftmqctl.exe an den ursprünglichen Pfad legen)
#    Copy-Item .\new\swiftmqd.exe $base\swiftmqd.exe -Force

# 4) Starten
& "$base\swiftmqd.exe" -config "$base\configs\swiftmqd.json" -log-level info

# 5) Verifikation: Prozess lebt + Management-API ist lesbar
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

### 5.2 Checkliste zur Verifikation nach dem Upgrade

- Im Startlog erscheinen `SwiftMQ 启动中 ... version=<新版本>` und `管理面已启动`;
- Die `object_totals` / `queue_totals` von `/api/overview` stimmen mit dem Zustand vor der Sicherung überein (vergleiche `backup-restore.md` §5);
- Die `messages` / `messages_ready` jeder durable-Queue in `/api/queues` stimmen mit dem Zustand vor der Sicherung überein;
- `/metrics` ist abrufbar und `swiftmq_plugin_up{name="amqp091"} 1`, `{name="mqtt"} 1`.

---

## 6. Image-Upgrade (Container)

Das Image ist etwa 13 MB groß (statisch gelinktes Binary + alpine), **läuft als Nicht-root (uid 10001)**, und das Datenverzeichnis ist unter `/var/lib/swiftmq` eingebunden.

```powershell
# 1) Neues Image ziehen/bauen (den Tag auf die neue Versionsnummer setzen, um Verwechslung alt/neu zu vermeiden)
docker build -t swiftmq:1.0.0 .

# 2) Alten Container stoppen (compose behält das benannte Volume swiftmq-data)
docker compose down

# 3) Neue Version starten (im compose-File das image auf den neuen Tag ändern)
docker compose up -d

# 4) Status und Logs
docker compose ps
docker compose logs -f --tail 100
```

> **Einmalige Aufgabe im Container** (z. B. `swiftmqctl` im Container ausführen): `run` von `docker compose ...` muss in einer nicht-interaktiven Umgebung mit `-T` versehen werden,
> sonst schlägt es fehl, weil ein TTY angefordert wird:
> ```powershell
> docker compose run -T --rm broker swiftmqctl -user guest -pass guest status
> ```

Die Datenpersistenz beruht auf dem **benannten Volume** `swiftmq-data` von compose; beim Neuerstellen des Containers gehen keine Daten verloren (seit M4 wirklich auf der Platte).
Muss der Volume-Inhalt vor dem Upgrade gesichert werden, entspricht das der Sicherung von `/var/lib/swiftmq` (siehe `backup-restore.md` §3.2). **【Image-Upgrade nicht gemessen】** (Docker wurde auf diesem Rechner nicht ausgeführt).

---

## 7. Schrittweise Umstellung und Rollback

### 7.1 Einzelknoten

- **Schrittweise Umstellung**: SwiftMQ bietet im Einzelknotenbetrieb keine eingebaute Fähigkeit für „alte und neue Version im selben Prozess“. Eine praktikable schrittweise Umstellung ist ein **Bypass-Schatten**:
  Die neue Instanz wird zunächst über **Read-Only-Konsum/Schatten-Queues** an denselben Upstream-Datenverkehr gehängt und beobachtet; erst nach Bestätigung wird der Schreibende umgeschaltet.
- **Rollback**:
  1. den Prozess der neuen Version stoppen;
  2. das alte Binary zurücktauschen;
  3. hat die neue Version bereits Daten geschrieben, **muss `data_dir` aus der Sicherung vor dem Upgrade wiederhergestellt werden** (siehe unten).
  Es gibt **keine** Garantie für „neue Version hat geschrieben, alte Version liest direkt“ – versionsübergreifender Downgrade siehe §8.

### 7.2 Cluster (rollierendes Upgrade)

Die Plattformseite bietet kein „Rolling-Upgrade mit einem Klick“; es muss manuell Knoten für Knoten in der folgenden Reihenfolge vorgegangen werden:

1. **Immer nur einen Knoten auf einmal upgraden**: Knoten stoppen → sein `data_dir` sichern → Binary tauschen → starten → warten, bis er wieder beitritt und aufholt
   (`swiftmqctl cluster_status` / `GET /api/cluster` auf `role`, `commit_index`/`last_applied` prüfen).
2. **Empfohlene Reihenfolge**: zuerst **learner / nicht stimmberechtigte Mitglieder** (ohne Auswirkung auf die Mehrheit) upgraden, dann **follower**, zuletzt **leader**
   (das Upgrade des leader löst eine Leader-Wahl aus, während der kurze Zeit nicht geschrieben werden kann).
3. **Auswirkung der Stilllegung auf die Mehrheit** (entscheidend):
   - 3-Knoten-Cluster: **gleichzeitig höchstens 1** stimmberechtigtes Mitglied stoppen; beim Stoppen von 2 geht die Mehrheit verloren, unter `pause_minority` **wird der gesamte Cluster-Dienst ausgesetzt**.
   - 2-Knoten-Cluster: Beim Stoppen von 1 geht bereits die Mehrheit verloren, **keine Fähigkeit zum rollierenden Upgrade** (mindestens 3 Knoten empfohlen).
   - Daher beim rollierenden Upgrade **strikt verboten, mehrere stimmberechtigte Mitglieder auf einmal zu stoppen**.
4. **Mitgliederänderungen und Upgrade nicht parallel durchführen**: Mitgliederänderungen haben **kein Joint Consensus**, es ist jeweils nur eine nicht committete Konfigurationsänderung erlaubt;
   vermeide während des Upgrades gleichzeitig `add_member` / `remove_member`.
5. Nach Abschluss des Upgrades prüfen, dass `object_totals` von `GET /api/cluster` mit dem Zustand vor dem Upgrade übereinstimmt.

> **【Nicht verifiziert】** Auf diesem Rechner wurde kein rollierender Upgrade-Durchlauf eines echten Clusters durchgeführt (weder Cluster-Pfad noch Container wurden ausgeführt); die obige Reihenfolge stammt aus den allgemeinen Beschränkungen von Message-Brokern
> und Raft sowie aus den Implementierungsfakten dieses Repositories zu `pause_minority` / Mitgliederänderungen und ist kein Messergebnis dieses Rechners.

---

## 8. Nicht unterstützte / nicht verifizierte Teile (explizit aufgeführt)

- **Downgrade über Hauptversionen hinweg: nicht unterstützt, nicht verifiziert**. Hat die neue Version bereits Daten in neuem Format/neuer Semantik geschrieben, gibt es **keine** Garantie für „Rückfall auf das alte Binary und unverändertes Lesen“;
  ein Rollback ist nur über die Sicherung vor dem Upgrade möglich.
- **Konfigurationsformat unverändert**: weiterhin JSON + `SWIFTMQ_*`-Umgebungsvariablen. **YAML-Konfiguration wird noch nicht unterstützt** (erfordert die Einführung einer Parsing-Abhängigkeit, M8-17 noch zu bewerten),
  das Upgrade bringt kein YAML.
- **Online-Hot-Upgrade von Plugins/Protokollen**: Plugins werden mit dem Kernel kompiliert (Form A) oder gemäß Konfiguration per `spawn` gestartet (Form B);
  Kernel-Upgrade = Prozess-Neustart; es gibt **keinen** Mechanismus zum Hot-Replace des Binarys an Ort und Stelle.
- **Migration der Speicher-Engine an Ort und Stelle**: Segment-Rotation/Index-Komprimierung sind Hintergrundverhalten zur Laufzeit, es gibt **keinen** eigenständigen Befehl „Datenmigration/Komprimierung“.
- **Cluster-Upgrade unter realem Netzwerk**: Dieses Repository hat nur verkleinertes Chaos (Kill auf Prozessebene) durchgeführt, **keinen** Upgrade-Durchlauf unter Netzwerkpartition oder vollgeschriebener Platte.
- Dieses Dokument **enthält keine** Verifikation des Datentransfers zwischen SwiftMQ und anderen Brokern (RabbitMQ).
