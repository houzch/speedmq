# SpeedMQ Sicherung und Wiederherstellung

> Alle „gemessenen“ Aussagen in diesem Dokument stammen aus einem echten Übungsdurchlauf unter **Windows + PowerShell 5.1** (temporäres `data_dir` und temporäre Ports).
> Die Übungsbefehle und die wichtigsten Ausgaben sind unverändert in §6 wiedergegeben. Als **【Nicht verifiziert】** markierte Teile werden ausdrücklich gekennzeichnet (z. B. ein echter physischer Stromausfall).

> **【Verifiziert (2026-10-11, 3-Knoten-Cluster auf lokalem Docker Desktop)】** Das harte Beenden von Prozessen (`docker kill`) und gleichzeitige Schreibvorgänge während der Wiederherstellung wurden gemessen;
> die Aufzeichnungen siehe §6.2 / §6.3; die Formulierung zum „Stromausfall“ in §2.3 und die Verifikationsstatusliste in §7 wurden entsprechend aktualisiert.

---

## 1. Was gesichert werden muss

Unter `data_dir` **muss alles vollständig gesichert werden**; entscheidend sind die folgenden Teile (Layout siehe `upgrade.md` §3):

| Pfad | Zweck | Folge bei Verlust |
| --- | --- | --- |
| `meta/state.json` | Metadaten-Snapshot im Einzelknotenmodus: vhost / Exchanges / Queues / Bindings / Benutzer / Berechtigungen / Policies | Topologie und Konten gehen vollständig verloren |
| `meta/raft.log`, `meta/raft.state`, `meta/snapshot.json` | 【Cluster】Raft-Log / Amtszeit und Wahlstimmen / Snapshot + Mitgliedertabelle | Cluster-Identität und Metadaten-Konsistenz gehen verloren |
| `meta/users.seeded`, `meta/vhosts.seeded` | Bootstrap-Markierungen | Bei Verlust werden die users/vhosts aus der Konfiguration **erneut eingespielt (seeding)** (gelöschte Konten/vhosts werden wiederbelebt) |
| `msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/0000NN.seg` + `index/0000NN.idx` | Nachrichtendaten und Index klassischer Queues | Persistente Nachrichten gehen verloren |
| `quorum/<safe(vhost)>/<safe(queue)>/` | 【Cluster】Raft-Log/Snapshot der Quorum-Queues | Daten der Quorum-Queues gehen verloren |
| Zertifikatsdateien (die PEM-Dateien, auf die `cert_file`/`key_file`/`ca_file` in der Konfiguration zeigen) | TLS-Zertifikate | getrennt von `data_dir` sichern, sonst startet TLS nach einem Neustart nicht |

> Weicher Zustand (unbestätigte Nachrichten, Consumer, prefetch-Zähler) existiert **nur im Speicher** und wird nicht auf die Platte geschrieben; die Sicherung **enthält ihn nicht** und sollte ihn auch nicht enthalten.

---

## 2. Konsistenzanforderung: **der Prozess muss zuerst gestoppt werden**; Hot-Backup ist **unsicher**

### 2.1 Fazit

- ✅ **Sichere Vorgehensweise**: **den Broker-Prozess stoppen** (ein sauberes Herunterfahren führt ein abschließendes Flush auf die Platte durch) und dann `data_dir` kopieren.
- ❌ **Hot-Backup (Dateien direkt kopieren, während der Prozess läuft): unsicher, keine Garantie.**

### 2.2 Warum Hot-Backup unsicher ist

Der Nachrichtenspeicher besteht aus **zwei Dateien** (Segmentdatei `*.seg` und Indexdatei `index/*.idx`), die **nicht atomar committet** werden:

- Bei der Wiederherstellung entscheidet **der Index** darüber, „welche Nachrichten noch leben“, und liest dann anhand von `(Segmentnummer, Offset, Länge)` aus dem Index aus der Segmentdatei.
- Beim Hot-Backup kann ein Zwischenzustand kopiert werden, in dem **der Index bereits referenziert, die Segmentdatei aber noch nicht vollständig geschrieben ist** (oder umgekehrt):
  - Der Index verweist auf einen Datensatz, der im Segment nicht existiert → die Nachricht **kann nicht gelesen werden und wird übersprungen** (das entspricht dem Verlust einer bereits bestätigten persistenten Nachricht);
  - Im Segment existiert ein Datensatz, aber der Index referenziert ihn nicht → die Nachricht **wird nicht wiederhergestellt**.
- Die Wiederherstellung verwirft zwar mit CRC32 **halb geschriebene Datensätze am Ende**, das deckt jedoch nur „einen beschädigten Datei-Schluss“ ab und **kann die Asynchronität zwischen Index und Segment nicht reparieren**.

### 2.3 Zum Thema „wann Schreibvorgänge auf die Platte gelangen“ (Messbeobachtung)

- Standard `fsync: os` + `flush_interval_ms: 200`: Nachrichten werden von einer Hintergrund-Flush-Routine innerhalb von **höchstens etwa 200 ms** per `write()` an das Betriebssystem übergeben
  (ohne fsync); die publisher confirm wird ebenfalls danach zurückgegeben.
- Messung: Fragt man **unmittelbar** nach dem Veröffentlichen einer persistenten Nachricht die Segmentdateigröße ab, sind die Daten bereits sichtbar (`t=0ms seg=832`); d. h. die „für das OS sichtbaren Bytes“ sind im Wesentlichen synchron zur confirm.
- **Achtung**: Dies zeigt nur, dass die Daten „im OS-Puffer“ angekommen sind. Ein **hartes Beenden des Prozesses führt nicht zum Verlust** (wird der Prozess beendet, geht der OS-Puffer nicht verloren), aber **ein Stromausfall führt zum Verlust**.
  Wenn „confirm = bereits per fsync auf der Platte“ gelten soll, ändere `storage.fsync` auf `batch` / `always`.
  **【Verifiziert (2026-10-11, 3-Knoten-Cluster auf lokalem Docker Desktop)】** Das harte Beenden eines Knotens mit `docker kill` (SIGKILL) während des Schreibens wurde bei beiden fsync-Stufen gemessen, ohne dass bestätigte Nachrichten verloren gingen:
  `fsync: os` mit Kill eines Followers (50.000 Nachrichten alle bestätigt, Abgleich missing=0), `fsync: always` mit Kill des Quorum-Queue-Leaders (200.000 Nachrichten alle bestätigt,
  missing=0; zusätzlich wurden 1.355 bereits auf der Platte geschriebene, aber nicht bestätigte Nachrichten erneut zugestellt – von der at-least-once-Semantik erlaubt; ein Consumer, der nach Nachrichten-ID dedupliziert, ist idempotent).
  Bonus: Beim Veröffentlichen mit der Standardstufe wurde die gesamte Docker-VM per `wsl --shutdown` hart gestoppt (approximiert einen maschinenweiten Stromausfall mit Verlust des OS-Puffers); nach der Wiederherstellung ergab der
  ID-für-ID-Abgleich der 69.669 bestätigten Nachrichten missing=0. **Weiterhin nicht verifiziert**: ein echter physischer Stromausfall (Stromverlust des Hosts, Festplatten-Cache-Ebene), der lokal nicht simuliert werden kann.

---

## 3. Sicherungsschritte

### 3.1 Einzelknoten (empfohlen)

```powershell
# 1) Prozess stoppen (Vordergrund: Ctrl+C; Hintergrund: Stop-Process)
Stop-Process -Name speedmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) Das gesamte data_dir kopieren (mit Zeitstempel)
$data = "C:\speedmq\data"
Copy-Item -Recurse -Force $data "C:\backup\speedmq-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) (optional) Prüfen, ob der Metadaten-Snapshot in der Sicherung parsbart ist
Get-Content "C:\backup\speedmq-...\meta\state.json" -Raw | ConvertFrom-Json | Select-Object -ExpandProperty VHosts
```

### 3.2 Cluster

- **Jeder Knoten sichert sein eigenes `data_dir`** (Metadaten werden über Raft an alle repliziert, die Nachrichtendaten liegen auf dem Owner-Knoten, Quorum-Queue-Replikate im jeweiligen Raft-Verzeichnis).
- Stilllegungsreihenfolge: **immer nur einen Knoten auf einmal stoppen**; stoppe nicht mehrere stimmberechtigte Mitglieder gleichzeitig (siehe `upgrade.md` §7.2).
- Um einen **clusterweit konsistenten Snapshot** zu erhalten, müssen alle Knoten der Reihe nach gestoppt und jeweils kopiert werden; in der Produktion ist „Stop/Kopieren/Start pro Knoten“ üblicher.
- **【Verifiziert (2026-10-10, 3-Knoten-Cluster auf lokalem Docker Desktop)】** In einem 3-Knoten-compose-Cluster wurde Knoten für Knoten „Stop/Kopieren/Start“ durchgeführt: Das benannte Volume jedes Knotens wurde mit einem Wegwerf-alpine-Container gepackt
  (`docker run --rm -v <Volume>:/data -v <Host-Verzeichnis>:/backup alpine tar czf ...`), das Archiv enthielt `meta/raft.log`, `meta/raft.state`, `meta/*.seeded`, `quorum/...`;
  nach der Wiederherstellung konnte der Knoten als bestehendes Mitglied wieder beitreten und aufholen (siehe §4.2, derselbe Beleg wie in `upgrade.md` §7.2).

### 3.3 Docker (benanntes Volume)

```powershell
# Nach dem Stoppen des Containers mit einem Wegwerf-Container den Volume-Inhalt als Archiv herauskopieren
docker compose down
docker run --rm -v speedmq-data:/data -v ${PWD}:/backup alpine `
  tar czf /backup/speedmq-data.tar.gz -C /data .
```
> **【Verifiziert (2026-10-10, lokaler Docker Desktop)】** Dieser Befehl wurde tatsächlich auf den benannten Volumes eines 3-Knoten-Clusters ausgeführt (das Archiv enthielt `meta/`, `quorum/`)
> und darauf aufbauend ein Wiederherstellungsdurchlauf für das Volume eines einzelnen Knotens abgeschlossen (siehe §4.2).

---

## 4. Wiederherstellungsschritte

### 4.1 Einzelknoten

```powershell
# 1) Prüfen, dass der Prozess gestoppt ist
Get-Process -Name speedmqd -ErrorAction SilentlyContinue

# 2) Das aktuelle data_dir verschieben (oder löschen), um ein Vermischen alter und neuer Dateien zu vermeiden
Move-Item "C:\speedmq\data" "C:\speedmq\data.broken"

# 3) Aus der Sicherung wiederherstellen
Copy-Item -Recurse -Force "C:\backup\speedmq-YYYYMMDD-HHMMSS" "C:\speedmq\data"

# 4) Starten
& "C:\speedmq\speedmqd.exe" -config "C:\speedmq\configs\speedmqd.json" -log-level info
```

Wichtige Punkte:
- **Das alte Verzeichnis muss zuerst verschoben werden**; man darf nicht „Sicherungsdateien über ein halb verbliebenes Verzeichnis kopieren“;
- Das wiederhergestellte `data_dir` muss **dieselbe vhost-/Queue-Menge** wie zum Sicherungszeitpunkt enthalten (die Verzeichnisnamen sind kodiert und maschinenübergreifend verwendbar);
- Nutze die Wiederherstellung **nicht** dazu, `vhosts`/`users` in der Konfigurationsdatei zu ändern (sie werden nur beim ersten Bootstrap wirksam, Änderungen bringen nichts, siehe `upgrade.md` §4.2).

### 4.2 Cluster

- Einzelnen Knoten wiederherstellen: `data_dir` dieses Knotens gemäß §4.1 wiederherstellen und starten; er tritt als bestehendes Mitglied wieder bei und holt den Raft-Log auf.
- Gesamten Cluster wiederherstellen: **zuerst die Mehrheit der Knoten wiederherstellen und starten** (≥ die Hälfte der stimmberechtigten Mitglieder), damit der Cluster einen leader wählen kann; dann die übrigen Knoten wiederherstellen.
- **【Verifiziert (2026-10-10, 3-Knoten-Cluster auf lokalem Docker Desktop)】** Messung von „n3 stoppen → sein Volume leeren → mit der Sicherung aus §3.3 überschreiben → neu starten“:
  n3 trat als follower wieder bei und holte zum leader auf (`commit_index == last_applied`); Nachrichtenzahl der Quorum-Queues und Mitgliedertabelle blieben unverändert.

---

## 5. Verifikationsmethoden nach der Wiederherstellung

Mit der Management-API und echten Clients gegengeprüft (alle empfohlen):

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }

# a) Gesamtzahl der Objekte und Nachrichten (Anzahl Queues/Exchanges/Bindings/Benutzer; messages/ready/unacked)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# b) Queue für Queue messages / messages_ready abgleichen (kann mit den Aufzeichnungen vor der Sicherung verglichen werden)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/queues' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# c) Ob vhost / Benutzer / Policies alle vorhanden sind
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/vhosts' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/users' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/policies' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# d) Cluster (im Einzelknotenmodus wird enabled=false / mode=local / role=single zurückgegeben)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/cluster' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

- **Startlog ansehen**: Es sollten `已从磁盘恢复队列消息 ... messages=N` und `队列已恢复持久化消息 ... messages=N` erscheinen; N sollte mit dem Wert vor der Sicherung übereinstimmen.
- **Warnungen im Log**: Wurden in einer Queue zuvor Nachrichten konsumiert/geleert, können bei der Wiederherstellung
  `恢复消息失败，已跳过 ... seq=K err="读取记录头失败: EOF"` und `恢复时清理了无存活消息的段` auftreten.
  Dabei handelt es sich um Index-Reste von **bereits abgerechneten (ack/purge) Datensätzen**; das ist **bekanntes Log-Rauschen und beeinträchtigt die Datenkorrektheit nicht** (siehe §7).
- **Echter Client**: Nachrichten aus der Queue abrufen und Anzahl/Inhalt prüfen (siehe §6 Schritt (7)).

---

## 6. Gemessener Übungsdurchlauf (echte Befehle und Ausgaben)

> Umgebung: `data_dir` in einem temporären Verzeichnis, AMQP `127.0.0.1:5676`, Management-Ebene `127.0.0.1:15677`, MQTT `127.0.0.1:1884`,
> Standardkonto `guest/guest`. Startlog:
> ```
> level=INFO msg="SpeedMQ 启动中" version=1.0.0 ... data_dir=...\data ... fsync=os
> level=INFO msg=管理面已启动 component=management addr=127.0.0.1:15677
> ```

**(1) Durable-Topologie anlegen + 5 persistente Nachrichten senden (echter Client `amqp091-go`)**

```
OK  已声明 durable 交换机 persist.ex / 队列 persist.q，并绑定 key=k
OK  已发布 5 条持久消息（delivery-mode=2）并收到全部 confirm
```

**(2) Benutzer / vhost / Berechtigungen / Policies anlegen (Management-API)**

```
vhost PUT -> 201
user PUT -> 201
perm PUT -> 204
policy PUT -> 201
```

**(3) Zustand vor der Sicherung (Management-API)**

```
=== /api/overview ===
"object_totals":{"connections":0,"channels":0,"queues":1,"consumers":0,"exchanges":13}
"queue_totals":{"messages":5,"messages_ready":5,"messages_unacknowledged":0}

=== /api/queues/%2F/persist.q ===
"durable":true,"effective_policy_definition":{"max-length":100},"messages":5,"messages_ready":5,
"name":"persist.q","policy":"drillpol","type":"classic","vhost":"/"

=== /api/vhosts ===  名称: ["/","drillvh"]
=== /api/users ===   名称: ["drilluser","guest"]
=== /api/policies === [{"apply-to":"queues","definition":{"max-length":100},"name":"drillpol","pattern":"persist.*","priority":1,"vhost":"/"}]
```

**(4) Dateien auf der Platte vor der Sicherung**

```
data\meta\state.json                                             (1118 B)
data\meta\users.seeded                                           (37 B)
data\meta\vhosts.seeded                                          (37 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\000001.seg       (1152 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\index\000001.idx (1023 B)
```

**(5) Prozess stoppen → sichern → leeren → wiederherstellen**

```
listeners still up: 0                       # 5676/1884/15677 sind alle geschlossen
=== 备份内容 ===   （与 (4) 完全一致，逐字节复制）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
data 目录是否存在(应为 False): False         # ursprüngliches data_dir gelöscht, um Datenverlust zu simulieren
=== 恢复后内容 ===   （从备份复制回来，与 (4) 一致）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
```

**(6) Wiederherstellungs-Log nach dem Neustart (Schlüsselzeilen)**

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=persist.q messages=5 segments=2
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=persist.q queue=persist.q messages=5
level=INFO msg=元数据层已打开 component=broker mode=local ... queues=1 exchanges=1 bindings=1 users=2
```
> Gleichzeitig erscheinen mehrere Zeilen `level=WARN msg=恢复消息失败，已跳过 ... seq=1..13 err="读取记录头失败: EOF"`:
> Diese sind Index-Reste von Nachrichten, die in diesem Durchlauf **zuvor per purge verworfen wurden** (abgerechnet, Segmentdaten bereits freigegeben), und **beeinträchtigen die Wiederherstellung der folgenden 5 Nachrichten nicht**.

**(7) Zusicherungen nach der Wiederherstellung: Management-API + echter Client**

```
=== 恢复后 /api/overview ===
"object_totals":{"queues":1,"exchanges":13,"consumers":0},"queue_totals":{"messages":5,"messages_ready":5,"messages_unacknowledged":0}

=== 恢复后 /api/queues/%2F/persist.q ===
"durable":true,"effective_policy_definition":{"max-length":100},"messages":5,"name":"persist.q","policy":"drillpol","type":"classic"

=== 恢复后 /api/vhosts (name) ===  / , drillvh
=== 恢复后 /api/users (name) ===   drilluser , guest
=== 恢复后 /api/policies ===       [{... "name":"drillpol","pattern":"persist.*" ...}]

=== 真实客户端断言 ===
OK  拓扑仍在：交换机 persist.ex / 队列 persist.q（声明时 message_count=5）
OK  取回 5 条持久消息: [persist-0 persist-1 persist-2 persist-3 persist-4]
```

**Fazit**: durable-Topologie (Exchange+Queue+Binding), 5 persistente Nachrichten, Benutzer, vhost, Berechtigungen, Policies **alle wiederhergestellt**;
der echte Client kann alle Nachrichten unverändert abrufen. **Der Übungsdurchlauf ist erfolgreich.**

### 6.1 Vergleich: Wiederherstellung einer Queue ohne Konsum/purge verläuft „ruhiger“

Um zu unterscheiden, ob die obigen WARN ein generelles Phänomen sind, wurde eine weitere **kontrollierte Gegenprobe** durchgeführt: neue durable Queue `clean.q` anlegen, 3 persistente Nachrichten senden,
**weder konsumieren noch leeren**, den Prozess stoppen und neu starten:

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=clean.q messages=3 segments=1
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=clean.q queue=clean.q messages=3
```
**Keine einzige WARN.** Das zeigt, dass WARN nur in der Situation auftreten, in der „im Index noch abgerechnete Datensätze verbleiben“ (siehe §7).

### 6.2 Hartes Beenden eines Knotens während des Schreibens (`docker kill`, 2026-10-11)

3-Knoten-Cluster (`partition_policy=pause_minority`), Quorum-Queue `pwr.q` mit 3 Repliken; der Client nutzt publisher confirms, jede Nachricht trägt eine eindeutige ID, und bestätigte IDs werden zum Abgleich auf der Platte gespeichert.

**Runde 1 (`fsync: os`, Kill des Followers n3)**: `docker kill pwr-n3` während der Veröffentlichung von 50.000 Nachrichten:

```
DONE published=50000 confirmed=50000 nacked=0 retries=0 elapsed=28.267s   # 杀后速率无可见变化
docker start pwr-n3 → 三节点 commit_index==last_applied==12，has_quorum=true
DRAIN total=50000 unique=50000 duplicates=0
confirmed_unique=50000 consumed_unique=50000 missing=0 extra=0
```

**Runde 2 (`fsync: always`, Kill des Quorum-Leaders n1)**: `docker kill pwr-n1` während der Veröffentlichung von 200.000 Nachrichten:

```
DONE published=200000 confirmed=200000 nacked=3843 retries=3843 elapsed=2m7.494s
# nack 来自转发到已死 leader 的发布（"与节点 n1 通信失败"），客户端重投全部成功
docker start pwr-n1 → 三节点 commit_index==last_applied==16
DRAIN 对账：confirmed_unique=436785 consumed_unique=438140 missing=0 extra=1355
# extra=已落盘但 confirm 未返回的消息，at-least-once 允许，按 ID 去重即幂等
```

**Runde 1b (`fsync: os`, `wsl --shutdown` stoppt die gesamte Docker-VM hart, approximiert einen maschinenweiten Stromausfall)**: harter Stopp mitten im Veröffentlichen; der Host hatte 69.669 bestätigte Zeilen aufgezeichnet;
nach dem Neuaufbau der VM und der Wiederherstellung des Clusters:

```
{"name":"pwr.q","messages":69669,"messages_ready":69669}
DRAIN total=69669 unique=69669 duplicates=0 ；missing=0 extra=0
```

Hinweis: `wsl --shutdown` approximiert nur den „Verlust des OS-Puffers“ und ist **keinem physischen Stromausfall äquivalent** (Stromverlust des Hosts und die Festplatten-Cache-Ebene sind nicht abgedeckt und bleiben unverifiziert).

### 6.3 Gleichzeitige Schreibvorgänge während der Wiederherstellung (2026-10-11)

Auf einem `fsync: always`-Cluster: `docker stop` des Followers n2 → 50.000 Nachrichten im Rückstau veröffentlichen (alle bestätigt) → `docker start pwr-n2`,
und **ab derselben Sekunde** weitere 20.000 Nachrichten an die Knoten der Mehrheitsseite veröffentlichen:

```
DONE published=20000 confirmed=20000 nacked=0 retries=0 elapsed=5.978s   # 约 3,300~6,400 条/秒
n2 日志：msg=已安装快照 component=broker index=4096 term=4              # 约 4 秒经快照追平
三节点 commit_index==last_applied==20；queue_totals.messages=82000       # 与四批发布总数精确相符
DRAIN total=82000 unique=80000 missing=0 extra=0                          # 2,000 重复为同 ID 重发批次的预期重复
```

Fazit: Gleichzeitige Schreibvorgänge während des Aufholens gehen weder verloren noch werden sie verfälscht; die Zählungen sind exakt.

---

## 7. Bekannte Probleme und Einschränkungen (wahrheitsgemäß erfasst)

1. **Log-Rauschen bei der Wiederherstellung (real beobachtet)**: Wenn in einer Queue in der Vergangenheit Konsum/Leerung stattgefunden hat (Nachrichten wurden ack/purge),
   bleibt im Index weiterhin eine Referenz auf die freigegebenen Datensätze; bei der Wiederherstellung wird für jeden dieser Datensätze **einzeln die WARN `恢复消息失败，已跳过` ausgegeben**
   und eine leere `000000.seg` neu angelegt/bereinigt (Log `恢复时清理了无存活消息的段`).
   Das **beeinträchtigt die Datenkorrektheit nicht** (noch lebende, unbestätigte Nachrichten werden korrekt wiederhergestellt), **verschmutzt jedoch das Log** und kann bei großen Queues/hohem Durchsatz das Log fluten.
   Empfehlung: Orientiere dich an `已从磁盘恢复队列消息 ... messages=N` und ignoriere diese WARN zu abgerechneten Datensätzen;
   ist die Logmenge nicht akzeptabel, melde dies den Kernel-Maintainern (dieses Dokument ändert keinen Code).
2. **Hot-Backup ist unsicher** (§2): Kopiere `data_dir` nicht direkt, während der Prozess läuft.
3. **`fsync: os` garantiert keinen Verlustfreiheit bei Stromausfall**: Für „confirm = sofort auf der Platte“ verwende `batch` / `always`.
4. **Passwörter im Klartext**: Die Benutzerpasswörter in `meta/state.json` liegen **im Klartext** vor (in der Messung sichtbar als `"password":"drillpass"`) –
   die Sicherungsdateien **müssen daher wie sensible Daten behandelt werden** (Zugriffskontrolle, verschlüsselte Aufbewahrung). Details siehe `security-baseline.md`.
5. **【Verifikationsstatus】** Hartes Beenden von Prozessen (einschließlich Kill des Quorum-Leaders während des Schreibens, bei beiden Stufen `fsync: os`/`always`) und gleichzeitige Schreibvorgänge während der Wiederherstellung (Veröffentlichen mit ca. 4.000–6.000 Nachrichten/s während des Aufholens, Snapshot-Aufholen in 4 Sekunden, exakte Zählungen, Abgleich missing=0) —— **【Verifiziert (2026-10-11, 3-Knoten-Cluster auf lokalem Docker Desktop)】**; `wsl --shutdown` als harter Stopp der gesamten Docker-VM (approximiert einen maschinenweiten Stromausfall) zeigte in einer einzelnen Stichprobe null Verlust bestätigter Nachrichten, aber **ein echter physischer Stromausfall ist weiterhin nicht verifiziert**; Cluster-Sicherung/-Wiederherstellung Knoten für Knoten sowie Sicherung/Wiederherstellung des benannten Docker-Volumes —— **【Verifiziert (2026-10-10, 3-Knoten-Cluster auf lokalem Docker Desktop)】** (siehe §3.2 / §3.3 / §4.2).
6. **Nebenbeobachtungen (im Drill vom 2026-10-11 wahrheitsgemäß protokolliert, nicht blockierend)**: ① Nach dem harten Beenden des Quorum-Leaders zeigte der Publish-Forwarding-Pfad der Follower noch etwa 76 Sekunden lang auf den toten Leader (neue Veröffentlichungen wurden in diesem Fenster genackt, Client-Wiederholungen waren alle erfolgreich, keine bestätigte Nachricht ging verloren) – es gibt ein Verfügbarkeits-Jitterfenster; ② beim einmaligen Entleeren einer großen Queue (438.000 Nachrichten) wurde eine etwa 153-fache Wiederzustellungs-Amplifikation beobachtet (at-least-once-Semantik gilt, am Ende exakt auf null abgebaut); ③ während des Aufholens einer großen Queue trat beim Starten eines Consumers über einen Follower gelegentlich `i/o timeout` auf, bei direkter Verbindung zum Leader war alles normal.
