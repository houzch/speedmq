# SpeedMQ monitoring en alerts

Deze map biedt direct te gebruiken monitoringtemplates:

| Bestand | Functie |
| --- | --- |
| `prometheus-alerts.yml` | Prometheus-alertregels (`groups: - name: speedmq`) |
| `grafana-dashboard.json` | Importeerbaar Grafana-dashboard (de panelen dekken de hieronder genoemde kernsignalen) |
| `README.md` | Gebruik, metriclijst, betekenis en aanpak van elke alert, bekende hiaten |

---

## 1. Hoe te gebruiken

### 1.1 Scrapen (Prometheus)

Het beheervlak (standaard `:15672`) stelt `/metrics` in Prometheus-tekstformaat beschikbaar; **Basic Auth is vereist**:

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

> Het wordt aanbevolen om een apart alleen-lezen-account voor monitoring aan te maken (de tag `monitoring` is voldoende om metrics te lezen); hergebruik niet het beheerderswachtwoord.

Verifiëren of het scrapen normaal werkt (PowerShell):

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
(Invoke-WebRequest -Uri 'http://127.0.0.1:15672/metrics' -Headers @{Authorization="Basic $pair"} -UseBasicParsing).Content
curl.exe -s -u guest:guest http://127.0.0.1:15672/metrics
```

### 1.2 Alertregels

Plaats `prometheus-alerts.yml` in de regels-map van Prometheus, verwijs ernaar in `prometheus.yml` en reload:

```yaml
rule_files:
  - "rules/speedmq-alerts.yml"
```

In de regels wordt uniform `job="speedmq"` gebruikt; als jouw jobnaam anders is, vervang deze dan in het hele bestand.

### 1.3 Grafana-dashboard

`grafana-dashboard.json` wordt geïmporteerd via **Dashboards → Import → JSON uploaden**; kies bij het importeren je Prometheus-datasource
(in het dashboard wordt ernaar verwezen met de variabele `${DS_PROMETHEUS}`). De templatevariabele `DS_PROMETHEUS` wordt in de importtoewijzing ingesteld.

**【niet geverifieerd】** Op deze machine is geen Grafana-instantie gestart en is geen echte importverificatie gedaan; de JSON is alleen op JSON-syntaxis gecontroleerd (14 panelen, parsing geslaagd).

---

## 2. Echte fragmenten van `/metrics` (bewijs)

Hieronder staat de **echte uitvoer** van `/metrics` van de `1.0.0`-instantie op deze machine (er was een durable queue `persist.q` aangemaakt,
waardoor per-queue-metrics met `vhost`/`queue`-tags verschenen):

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

## 3. Metriclijst (allemaal echt aanwezig, bron `internal/management/metrics.go`)

| Metric | Type | Label | Semantiek |
| --- | --- | --- | --- |
| `speedmq_up` | gauge | — | Proces rapporteert zichzelf als levend (momenteel altijd 1) |
| `speedmq_build_info` | gauge | `version`,`node` | Build-informatie, value altijd 1 |
| `speedmq_resource_blocked` | gauge | — | Of het resourcewatermerk producers blokkeert (1=geblokkeerd) |
| `speedmq_connections` | gauge | — | Huidig aantal verbindingen |
| `speedmq_channels` | gauge | — | Huidig aantal kanalen |
| `speedmq_queues` | gauge | — | Huidig aantal queues |
| `speedmq_exchanges` | gauge | — | Huidig aantal exchanges |
| `speedmq_consumers` | gauge | — | Huidig aantal consumers |
| `speedmq_queue_messages` | gauge | — | **Globaal** totaal aantal gereed berichten |
| `speedmq_queue_messages_unacknowledged` | gauge | — | **Globaal** totaal aantal onbevestigde berichten |
| `speedmq_process_memory_bytes` | gauge | — | **In gebruik** geheugen van het proces (`HeapInuse+StackInuse`) |
| `speedmq_memory_total_bytes` | gauge | — | Totaal fysiek geheugen |
| `speedmq_disk_free_bytes` | gauge | — | Beschikbare ruimte in de gegevensmap |
| `speedmq_memory_high_watermark` | gauge | — | Geheugenwatermerk-ratio |
| `speedmq_disk_free_limit_bytes` | gauge | — | Ondergrens voor resterende schijfruimte |
| `speedmq_queue_messages_ready` | gauge | `vhost`,`queue` | Aantal gereed berichten in een queue |
| `speedmq_queue_messages_unacknowledged` | gauge | `vhost`,`queue` | Aantal onbevestigde berichten in een queue |
| `speedmq_queue_consumers` | gauge | `vhost`,`queue` | Aantal consumers op een queue |
| `speedmq_queue_memory_bytes` | gauge | `vhost`,`queue` | Schatting van het geheugengebruik van een queue |
| `speedmq_queue_messages_published_total` | counter | `vhost`,`queue` | Cumulatief aantal door een queue ontvangen berichten |
| `speedmq_queue_messages_delivered_total` | counter | `vhost`,`queue` | Cumulatief aantal door een queue afgeleverde berichten |
| `speedmq_queue_messages_acked_total` | counter | `vhost`,`queue` | Cumulatief aantal door een queue bevestigde berichten |
| `speedmq_plugin_info` | gauge | `name`,`version`,`api_version`,`state` | Pluginmetadata, value altijd 1 |
| `speedmq_plugin_up` | gauge | `name`,`state` | Of de plugin in dienst is (1=enabled, 0=overig) |

### 3.1 Aandachtspunten bij gebruik (om fouten te voorkomen)

- **Twee reeksenfamilies met dezelfde naam maar een verschillend kardinaliteit**: `speedmq_queue_messages_unacknowledged` heeft **zowel** een globale reeks zonder labels
  **als** een per-queue-reeks met labels; aan de "gereed"-kant heet de globale reeks `speedmq_queue_messages` en de per-queue-reeks
  `speedmq_queue_messages_ready` (asymmetrische namen). Gebruik bij het schrijven van regels `{queue=~".+"}` om expliciet alleen de per-queue-familie te nemen.
- **De reikwijdte van `speedmq_process_memory_bytes`**: de implementatie is `MemStats.HeapInuse + StackInuse` (**geheugen in gebruik**),
  dezelfde reikwijdte als de geheugenwatermerk-beoordeling van de kernel; maar de `# HELP`-tekst zegt "het aantal bytes aan geheugen dat bij het besturingssysteem is aangevraagd", **de tekst komt niet overeen met de werkelijke reikwijdte**;
  houd dit document aan.
- **Per-queue-reeksen verschijnen alleen als de queue bestaat**: nadat een queue is verwijderd, verdwijnt de reeks (aan de Prometheus-kant wordt deze stale).
  Voor alerts over "een queue zou moeten bestaan maar heeft geen gegevens" kun je `absent()` of de `or vector(0)` van Grafana gebruiken.
- **Counters gaan terug naar nul na een herstart van het proces**: `*_total` is een cumulatie binnen het proces en begint na een herstart weer bij 0; gebruik `rate()`/`increase()`
  en stel geen drempels in op de absolute waarde.
- **Clustersignalen zonder metrics**: zie §5.

---

## 4. Betekenis van alerts en aanbevolen aanpak (behorend bij `prometheus-alerts.yml`)

| Alert | Triggervoorwaarde | Betekenis | Aanbevolen aanpak |
| --- | --- | --- | --- |
| `SpeedMQScrapeDown` | `up{job="speedmq"} == 0` 1m | Het scrape-doel is volledig onbereikbaar | Controleer proces/poort/netwerk/authenticatie; herstart en bekijk het opstartlog |
| `SpeedMQProcessNotUp` | `speedmq_up == 0` 1m | Het scrapen is gelukt, maar het proces rapporteert zichzelf als niet-levend | Vangnet-item, controleer het log op een abnormale afsluiting |
| `SpeedMQPluginDown` | `speedmq_plugin_up == 0` 2m | Plugin disabled/failed/down | Bekijk `runtime_note` met `speedmqctl plugins show <name>`; externe plugins met `restart=always` genezen meestal vanzelf |
| `SpeedMQResourceBlocked` | `speedmq_resource_blocked == 1` 5m | Geheugen-/schijfwatermerk geactiveerd, producers worden geblokkeerd | Controleer het geheugenwatermerk en de beschikbare schijfruimte; bevestig of consumers vooruitgang boeken |
| `SpeedMQMemoryWatermarkHigh` | in-gebruik geheugen > 0.9×watermerk 10m | Nadert het geheugenwatermerk | Verminder de achterstand/verhoog de consumptiesnelheid om blokkering te voorkomen |
| `SpeedMQDiskFreeLow` | beschikbaar < 1.5×schijfondergrens 10m | De gegevensmap raakt bijna vol | Schaal op/ruim op; bij het bereiken van de ondergrens worden producers geblokkeerd |
| `SpeedMQQueueBacklogGrowing` | gereed >10000 en 15m monotoon stijgend | De queue raakt continu verder achterop | Schaal consumers op / onderzoek de consumptiekant; onderzoek dead-letter/TTL-afwijkingen |
| `SpeedMQQueueNoConsumers` | consumers=0 en er zijn gereed berichten 15m | Niemand consumeert | Controleer het consumerproces; bevestig dat de consumer niet is weggevallen |
| `SpeedMQUnackedPileUp` | onbevestigd >1000 15m | Consumer zit vast / ackt niet | Controleer de verwerkingslogica van de consumer en prefetch; sluit indien nodig de verbinding en lever opnieuw af |
| `SpeedMQConnectionSpike` | verbindingen >10000 10m | Abnormaal aantal verbindingen | Controleer op verbindingslekken; clients zouden verbindingen moeten hergebruiken |

> De drempels (10000 / 1000, enz.) zijn **startwaarden**; pas ze aan op basis van je queuegrootte en bedrijfskenmerken.

---

## 5. Bekend hiaat: er zijn momenteel geen metrics voor cluster "meerderheid verloren / geen leader"

- **Feit**: `/metrics` **heeft** geen enkele clustermetric (geen `speedmq_cluster_*`). De clusterstatus staat alleen in de JSON van `GET /api/cluster`:
  `{"enabled":true,"mode":"raft","role":"leader","leader":"...","has_quorum":true,"paused":false,...}`.
- **Daarom** zijn in `prometheus-alerts.yml` **bewust geen** alerts op basis van clustermetrics opgenomen — zelfs als je ze schrijft, **zullen ze nooit triggeren**
  (Prometheus geeft geen foutmelding als een metricnaam niet bestaat), wat neerkomt op een levering die "er goed uitziet maar in feite nutteloos is".
- **Zelfbouwsysteem** (kies er één; beide moet je buiten SpeedMQ opzetten en vallen buiten het bereik van deze repository):
  1. Gebruik een generieke JSON-exporter om `/api/cluster` te scrapen, map het naar een aangepaste metric (zoals `speedmq_cluster_has_quorum`) en alarmeer op die metric;
  2. Laat een proberscript periodiek `/api/cluster` aanroepen en alarmeer/punt bij `has_quorum=false` of `paused=true`.
- Betreffende reikwijdte: `has_quorum=false` betekent verlies van contact met de meerderheid; onder `pause_minority` (standaard) **wordt de service dan gepauzeerd en worden verbindingen verbroken**.

---

## 6. Overige **niet geverifieerde** items

- Het Grafana-dashboard is **niet in een echte Grafana geïmporteerd en geverifieerd** (alleen JSON-syntaxiscontrole geslaagd).
- De alertregels zijn **niet in een echte Prometheus/Alertmanager geladen en geverifieerd** (op deze machine is geen Prometheus gestart).
  Maar de **metricnamen in de regels zijn regel voor regel vergeleken met de echte `/metrics`-uitvoer** (zie §2/§3), dus er is geen probleem van "een verkeerde naam waardoor nooit wordt getriggerd".
