# SpeedMQ Monitoring und Alarmierung

Dieses Verzeichnis bietet direkt einsetzbare Monitoring-Vorlagen:

| Datei | Zweck |
| --- | --- |
| `prometheus-alerts.yml` | Prometheus-Alarmregeln (`groups: - name: speedmq`) |
| `grafana-dashboard.json` | importierbares Grafana-Dashboard (die Panels decken die unten genannten Schlüsselsignale ab) |
| `README.md` | Verwendung, Metrikliste, Bedeutung und Behandlung jedes Alarms, bekannte Lücken |

---

## 1. Verwendung

### 1.1 Scraping (Prometheus)

Die Management-Ebene (Standard `:15672`) stellt `/metrics` im Prometheus-Textformat bereit, **Basic Auth erforderlich**:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: speedmq
    metrics_path: /metrics
    basic_auth:
      username: guest
      password: guest
    static_configs:
      - targets: ["127.0.0.1:15672"]
```

> Es wird empfohlen, für das Monitoring ein separates Read-Only-Konto anzulegen (der Tag `monitoring` genügt zum Lesen der Metriken); verwende nicht das Administrator-Passwort wieder.

Prüfen, ob das Scraping funktioniert (PowerShell):

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
(Invoke-WebRequest -Uri 'http://127.0.0.1:15672/metrics' -Headers @{Authorization="Basic $pair"} -UseBasicParsing).Content
curl.exe -s -u guest:guest http://127.0.0.1:15672/metrics
```

### 1.2 Alarmregeln

Lege `prometheus-alerts.yml` in das Regelverzeichnis von Prometheus, referenziere es in `prometheus.yml` und lade danach neu:

```yaml
rule_files:
  - "rules/speedmq-alerts.yml"
```

In den Regeln wird einheitlich `job="speedmq"` verwendet; weicht dein Job-Name ab, ersetze ihn im gesamten Text.

### 1.3 Grafana-Dashboard

`grafana-dashboard.json` wird über **Dashboards → Import → JSON hochladen** importiert; wähle beim Import deine Prometheus-Datenquelle
(im Dashboard wird sie über die Variable `${DS_PROMETHEUS}` referenziert). Die Template-Variable `DS_PROMETHEUS` wird im Import-Mapping zugewiesen.

**【Nicht verifiziert】** Auf diesem Rechner wurde keine Grafana-Instanz gestartet und keine echte Import-Verifikation durchgeführt; das JSON wurde nur einer JSON-Syntaxprüfung unterzogen (14 Panels, Parsing erfolgreich).

---

## 2. Echter `/metrics`-Ausschnitt (Beleg)

Das Folgende ist die **echte Ausgabe** von `/metrics` der `1.0.0`-Instanz auf diesem Rechner (es wurde eine durable Queue `persist.q` angelegt,
daher erscheinen die per-queue-Metriken mit den Labels `vhost`/`queue`):

```
# HELP speedmq_up 节点是否存活
# TYPE speedmq_up gauge
speedmq_up 1
# HELP speedmq_build_info 构建信息
# TYPE speedmq_build_info gauge
speedmq_build_info 1{version="1.0.0",node="speedmq@DESKTOP-HBDCVPA"}
# HELP speedmq_resource_blocked 资源水位是否阻塞了生产者（1=阻塞中）
# TYPE speedmq_resource_blocked gauge
speedmq_resource_blocked 0
# HELP speedmq_connections 当前连接数
# TYPE speedmq_connections gauge
speedmq_connections 0
# HELP speedmq_channels 当前通道数
# TYPE speedmq_channels gauge
speedmq_channels 0
# HELP speedmq_queues 当前队列数
# TYPE speedmq_queues gauge
speedmq_queues 0
# HELP speedmq_exchanges 当前交换机数
# TYPE speedmq_exchanges gauge
speedmq_exchanges 6
# HELP speedmq_consumers 当前消费者数
# TYPE speedmq_consumers gauge
speedmq_consumers 0
# HELP speedmq_queue_messages 就绪消息总数
# TYPE speedmq_queue_messages gauge
speedmq_queue_messages 0
# HELP speedmq_queue_messages_unacknowledged 未确认消息总数
# TYPE speedmq_queue_messages_unacknowledged gauge
speedmq_queue_messages_unacknowledged 0
# HELP speedmq_process_memory_bytes 本进程向操作系统申请的内存字节数
# TYPE speedmq_process_memory_bytes gauge
speedmq_process_memory_bytes 1564672
# HELP speedmq_memory_total_bytes 物理内存总量
# TYPE speedmq_memory_total_bytes gauge
speedmq_memory_total_bytes 34181279744
# HELP speedmq_disk_free_bytes 数据目录可用空间
# TYPE speedmq_disk_free_bytes gauge
speedmq_disk_free_bytes 240091688960
# HELP speedmq_memory_high_watermark 内存水位比例
# TYPE speedmq_memory_high_watermark gauge
speedmq_memory_high_watermark 0.4
# HELP speedmq_disk_free_limit_bytes 磁盘剩余空间下限
# TYPE speedmq_disk_free_limit_bytes gauge
speedmq_disk_free_limit_bytes 52428800
# HELP speedmq_queue_messages_ready 队列中的就绪消息数
# TYPE speedmq_queue_messages_ready gauge
# HELP speedmq_queue_messages_unacknowledged 队列中的未确认消息数
# TYPE speedmq_queue_messages_unacknowledged gauge
# HELP speedmq_queue_consumers 队列上的消费者数
# TYPE speedmq_queue_consumers gauge
# HELP speedmq_queue_memory_bytes 队列内存占用估算值
# TYPE speedmq_queue_memory_bytes gauge
# HELP speedmq_queue_messages_published_total 队列累计接收的消息数
# TYPE speedmq_queue_messages_published_total counter
# HELP speedmq_queue_messages_delivered_total 队列累计投递的消息数
# TYPE speedmq_queue_messages_delivered_total counter
# HELP speedmq_queue_messages_acked_total 队列累计确认的消息数
# TYPE speedmq_queue_messages_acked_total counter
speedmq_queue_messages_ready{vhost="/",queue="persist.q"} 5
speedmq_queue_messages_unacknowledged{vhost="/",queue="persist.q"} 0
speedmq_queue_consumers{vhost="/",queue="persist.q"} 0
speedmq_queue_memory_bytes{vhost="/",queue="persist.q"} 365
speedmq_queue_messages_published_total{vhost="/",queue="persist.q"} 5
speedmq_queue_messages_delivered_total{vhost="/",queue="persist.q"} 0
speedmq_queue_messages_acked_total{vhost="/",queue="persist.q"} 0
# HELP speedmq_plugin_info 插件元数据（value 恒为 1，状态见 state 标签）
# TYPE speedmq_plugin_info gauge
# HELP speedmq_plugin_up 插件是否在服务（1=enabled，0=其它状态：disabled/failed/down）
# TYPE speedmq_plugin_up gauge
speedmq_plugin_info{name="amqp091",version="0.1.0",api_version="v1",state="enabled"} 1
speedmq_plugin_up{name="amqp091",state="enabled"} 1
speedmq_plugin_info{name="mqtt",version="0.1.0",api_version="v1",state="enabled"} 1
speedmq_plugin_up{name="mqtt",state="enabled"} 1
```

---

## 3. Metrikliste (alle tatsächlich vorhanden, Quelle `internal/management/metrics.go`)

| Metrik | Typ | Labels | Semantik |
| --- | --- | --- | --- |
| `speedmq_up` | gauge | — | Der Prozess meldet sich als lebendig (derzeit immer 1) |
| `speedmq_build_info` | gauge | `version`,`node` | Build-Information, value immer 1 |
| `speedmq_resource_blocked` | gauge | — | Ob eine Ressourcenschwelle den Producer blockiert (1=blockiert) |
| `speedmq_connections` | gauge | — | aktuelle Verbindungszahl |
| `speedmq_channels` | gauge | — | aktuelle Kanalzahl |
| `speedmq_queues` | gauge | — | aktuelle Queue-Anzahl |
| `speedmq_exchanges` | gauge | — | aktuelle Exchange-Anzahl |
| `speedmq_consumers` | gauge | — | aktuelle Consumer-Anzahl |
| `speedmq_queue_messages` | gauge | — | **globale** Gesamtzahl bereiter Nachrichten |
| `speedmq_queue_messages_unacknowledged` | gauge | — | **globale** Gesamtzahl unbestätigter Nachrichten |
| `speedmq_process_memory_bytes` | gauge | — | vom Prozess **verwendeter** Speicher (`HeapInuse+StackInuse`) |
| `speedmq_memory_total_bytes` | gauge | — | Gesamter physischer Speicher |
| `speedmq_disk_free_bytes` | gauge | — | freier Speicher des Datenverzeichnisses |
| `speedmq_memory_high_watermark` | gauge | — | Speicher-Watermark-Anteil |
| `speedmq_disk_free_limit_bytes` | gauge | — | Untergrenze für freien Speicherplatz |
| `speedmq_queue_messages_ready` | gauge | `vhost`,`queue` | bereite Nachrichten einer Queue |
| `speedmq_queue_messages_unacknowledged` | gauge | `vhost`,`queue` | unbestätigte Nachrichten einer Queue |
| `speedmq_queue_consumers` | gauge | `vhost`,`queue` | Consumer-Anzahl einer Queue |
| `speedmq_queue_memory_bytes` | gauge | `vhost`,`queue` | geschätzter Speicherverbrauch einer Queue |
| `speedmq_queue_messages_published_total` | counter | `vhost`,`queue` | kumulierte Anzahl empfangener Nachrichten der Queue |
| `speedmq_queue_messages_delivered_total` | counter | `vhost`,`queue` | kumulierte Anzahl zugestellter Nachrichten der Queue |
| `speedmq_queue_messages_acked_total` | counter | `vhost`,`queue` | kumulierte Anzahl bestätigter Nachrichten der Queue |
| `speedmq_plugin_info` | gauge | `name`,`version`,`api_version`,`state` | Plugin-Metadaten, value immer 1 |
| `speedmq_plugin_up` | gauge | `name`,`state` | Ob das Plugin im Dienst ist (1=enabled, 0=sonstiges) |

### 3.1 Hinweise zur Verwendung (um Fehler zu vermeiden)

- **Zwei Serienfamilien mit gleichem Namen, aber unterschiedlicher Kardinalität**: `speedmq_queue_messages_unacknowledged` hat **sowohl** eine globale, label-lose Serie
  **als auch** eine per-queue-Serie mit Labels; auf der „bereit“-Seite heißt die globale Serie `speedmq_queue_messages` und die per-queue-Serie
  `speedmq_queue_messages_ready` (unsymmetrische Namen). Verwende beim Schreiben von Regeln `{queue=~".+"}`, um eindeutig nur die per-queue-Familie zu erfassen.
- **Die Handhabung von `speedmq_process_memory_bytes`**: Die Implementierung ist `MemStats.HeapInuse + StackInuse` (**verwendeter Speicher**)
  und entspricht derselben Handhabung wie die Speicher-Watermark-Beurteilung des Kernels; der `# HELP`-Text lautet jedoch „die vom Betriebssystem angeforderten Speicherbytes“, **der Text stimmt nicht mit der tatsächlichen Handhabung überein**,
  maßgeblich ist dieses Dokument.
- **per-queue-Serien erscheinen nur, wenn die Queue existiert**: Nach dem Löschen der Queue verschwindet die Serie (auf der Prometheus-Seite wird sie stale).
  Für Alarme zu „die Queue sollte existieren, liefert aber keine Daten“ kann `absent()` oder das Grafana-`or vector(0)` kombiniert werden.
- **counter werden nach einem Prozess-Neustart auf null zurückgesetzt**: `*_total` ist eine prozessinterne Kumulation und beginnt nach einem Neustart bei 0; verwende `rate()`/`increase()`
  und setze keine Schwellen direkt auf Absolutwerte.
- **Es gibt keine Cluster-Signale als Metriken**: siehe §5.

---

## 4. Bedeutung der Alarme und empfohlene Behandlung (entsprechend `prometheus-alerts.yml`)

| Alarm | Auslösebedingung | Bedeutung | Empfohlene Behandlung |
| --- | --- | --- | --- |
| `SpeedMQScrapeDown` | `up{job="speedmq"} == 0` 1m | Das gesamte Scrape-Ziel ist nicht erreichbar | Prozess/Port/Netzwerk/Authentifizierung prüfen; neu starten und Startlog ansehen |
| `SpeedMQProcessNotUp` | `speedmq_up == 0` 1m | Scraping erfolgreich, aber der Prozess meldet sich als nicht lebendig | Auffangpunkt; Log zu unerwartetem Beenden prüfen |
| `SpeedMQPluginDown` | `speedmq_plugin_up == 0` 2m | Plugin disabled/failed/down | Mit `speedmqctl plugins show <name>` `runtime_note` prüfen; externe Plugins mit `restart=always` heilen sich meist selbst |
| `SpeedMQResourceBlocked` | `speedmq_resource_blocked == 1` 5m | Speicher-/Platten-Watermark ausgelöst, Producer werden blockiert | Speicher-Watermark und freien Speicher prüfen; bestätigen, ob Consumer vorankommen |
| `SpeedMQMemoryWatermarkHigh` | Anteil des verwendeten Speichers > 0,9× Watermark 10m | Nähert sich der Speicher-Watermark | Rückstand verringern/Konsumgeschwindigkeit erhöhen, um eine Blockierung zu vermeiden |
| `SpeedMQDiskFreeLow` | verfügbar < 1,5× Untergrenze 10m | Das Datenverzeichnis läuft fast voll | Erweitern/Aufräumen; beim Erreichen der Untergrenze werden Producer blockiert |
| `SpeedMQQueueBacklogGrowing` | bereit >10000 und 15m monoton steigend | Die Queue sammelt kontinuierlich Rückstand an | Consumer skalieren / Konsumseite prüfen; Dead-Letter/TTL-Anomalien untersuchen |
| `SpeedMQQueueNoConsumers` | Consumer=0 und bereite Nachrichten vorhanden 15m | Niemand konsumiert | Konsumprozess prüfen; bestätigen, dass der Consumer nicht offline ist |
| `SpeedMQUnackedPileUp` | unbestätigt >1000 15m | Der Consumer hängt / bestätigt nicht | Verarbeitungslogik des Consumer und prefetch prüfen; bei Bedarf die Verbindung schließen und neu zustellen |
| `SpeedMQConnectionSpike` | Verbindungen >10000 10m | Anomale Verbindungszahl | Auf Verbindungslecks prüfen; Clients sollten Verbindungen wiederverwenden |

> Die Schwellenwerte (10000 / 1000 usw.) sind **Ausgangswerte**; passe sie an deine Queue-Größe und Geschäftscharakteristik an.

---

## 5. Bekannte Lücke: für Cluster „Mehrheit verloren / kein leader“ gibt es derzeit keine Metrik

- **Fakt**: `/metrics` hat **keine** Cluster-Metriken (keine `speedmq_cluster_*`). Der Cluster-Status steht nur im JSON von `GET /api/cluster`:
  `{"enabled":true,"mode":"raft","role":"leader","leader":"...","has_quorum":true,"paused":false,...}`.
- **Daher** werden in `prometheus-alerts.yml` **absichtlich keine** Alarme auf Basis von Cluster-Metriken geschrieben – sie würden **niemals auslösen**
  (Prometheus meldet keinen Fehler, wenn ein Metrikname nicht existiert), was einer „scheinbar richtigen, tatsächlich aber wirkungslosen“ Lieferung entspräche.
- **Eigenbau-Lösung** (eine von zwei; beide müssen außerhalb von SpeedMQ eingerichtet werden und liegen nicht im Umfang dieses Repositories):
  1. Mit einem generischen JSON-Exporter `/api/cluster` scrapen, auf benutzerdefinierte Metriken abbilden (z. B. `speedmq_cluster_has_quorum`) und darauf alarmieren;
  2. Ein Probe-Skript ruft regelmäßig `/api/cluster` auf und alarmiert, wenn `has_quorum=false` oder `paused=true`.
- Relevante Handhabung: `has_quorum=false` bedeutet, dass die Verbindung zur Mehrheit verloren wurde; unter `pause_minority` (Standard) **wird der Dienst dann ausgesetzt und Verbindungen werden getrennt**.

---

## 6. Weitere **nicht verifizierte** Punkte

- Das Grafana-Dashboard wurde **nicht in einem echten Grafana importiert und verifiziert** (nur die JSON-Syntaxprüfung war erfolgreich).
- Die Alarmregeln wurden **nicht in einem echten Prometheus/Alertmanager geladen und verifiziert** (auf diesem Rechner wurde kein Prometheus gestartet).
  Die **Metriknamen in den Regeln wurden jedoch einzeln gegen die echte `/metrics`-Ausgabe abgeglichen** (siehe §2/§3), es besteht kein Problem darin, dass „ein falsch geschriebener Name zu einer nie auslösenden Regel führt“.
