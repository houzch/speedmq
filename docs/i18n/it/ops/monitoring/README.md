# Monitoraggio e avvisi di SwiftMQ

Questa directory fornisce modelli di monitoraggio pronti all'uso:

| File | Funzione |
| --- | --- |
| `prometheus-alerts.yml` | Regole di avviso di Prometheus (`groups: - name: swiftmq`) |
| `grafana-dashboard.json` | Dashboard Grafana importabile (i pannelli coprono i segnali chiave descritti sotto) |
| `README.md` | Utilizzo, elenco delle metriche, significato e gestione di ciascun avviso, lacune note |

---

## 1. Come si usa

### 1.1 Raccolta (Prometheus)

Il piano di gestione (predefinito `:15672`) espone `/metrics` in formato testuale Prometheus, **con Basic Auth obbligatoria**:

```yaml
# prometheus.yml
scrape_configs:
  - job_name: swiftmq
    metrics_path: /metrics
    basic_auth:
      username: guest
      password: guest
    static_configs:
      - targets: ["127.0.0.1:15672"]
```

> Si consiglia di creare un account di sola lettura dedicato al monitoraggio (il tag `monitoring` è sufficiente per leggere le metriche) e di non riutilizzare la password dell'amministratore.

Verifica che la raccolta funzioni (PowerShell):

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
(Invoke-WebRequest -Uri 'http://127.0.0.1:15672/metrics' -Headers @{Authorization="Basic $pair"} -UseBasicParsing).Content
curl.exe -s -u guest:guest http://127.0.0.1:15672/metrics
```

### 1.2 Regole di avviso

Metti `prometheus-alerts.yml` nella directory delle regole di Prometheus, referenzialo in `prometheus.yml` e poi esegui il reload:

```yaml
rule_files:
  - "rules/swiftmq-alerts.yml"
```

Le regole usano uniformemente `job="swiftmq"`; se il nome del tuo job è diverso, sostituiscilo in tutto il file.

### 1.3 Dashboard Grafana

`grafana-dashboard.json` si importa tramite **Dashboards → Import → carica JSON**, scegliendo in fase di importazione la tua origine dati Prometheus
(nella dashboard viene referenziata con la variabile `${DS_PROMETHEUS}`). La variabile di template `DS_PROMETHEUS` viene valorizzata nella mappatura di importazione.

**【non verificato】** Sulla macchina locale non è stata avviata un'istanza Grafana e non è stata eseguita una verifica di importazione reale; quel JSON è stato solo sottoposto a validazione della sintassi JSON (14 pannelli, parsing superato).

---

## 2. Frammento reale di `/metrics` (prova)

Quello che segue è l'**output reale** di `/metrics` dell'istanza `1.0.0` locale (essendo stata creata una coda durable `persist.q`,
compaiono le metriche per coda con i tag `vhost`/`queue`):

```
# HELP swiftmq_up 节点是否存活
# TYPE swiftmq_up gauge
swiftmq_up 1
# HELP swiftmq_build_info 构建信息
# TYPE swiftmq_build_info gauge
swiftmq_build_info 1{version="1.0.0",node="swiftmq@DESKTOP-HBDCVPA"}
# HELP swiftmq_resource_blocked 资源水位是否阻塞了生产者（1=阻塞中）
# TYPE swiftmq_resource_blocked gauge
swiftmq_resource_blocked 0
# HELP swiftmq_connections 当前连接数
# TYPE swiftmq_connections gauge
swiftmq_connections 0
# HELP swiftmq_channels 当前通道数
# TYPE swiftmq_channels gauge
swiftmq_channels 0
# HELP swiftmq_queues 当前队列数
# TYPE swiftmq_queues gauge
swiftmq_queues 0
# HELP swiftmq_exchanges 当前交换机数
# TYPE swiftmq_exchanges gauge
swiftmq_exchanges 6
# HELP swiftmq_consumers 当前消费者数
# TYPE swiftmq_consumers gauge
swiftmq_consumers 0
# HELP swiftmq_queue_messages 就绪消息总数
# TYPE swiftmq_queue_messages gauge
swiftmq_queue_messages 0
# HELP swiftmq_queue_messages_unacknowledged 未确认消息总数
# TYPE swiftmq_queue_messages_unacknowledged gauge
swiftmq_queue_messages_unacknowledged 0
# HELP swiftmq_process_memory_bytes 本进程向操作系统申请的内存字节数
# TYPE swiftmq_process_memory_bytes gauge
swiftmq_process_memory_bytes 1564672
# HELP swiftmq_memory_total_bytes 物理内存总量
# TYPE swiftmq_memory_total_bytes gauge
swiftmq_memory_total_bytes 34181279744
# HELP swiftmq_disk_free_bytes 数据目录可用空间
# TYPE swiftmq_disk_free_bytes gauge
swiftmq_disk_free_bytes 240091688960
# HELP swiftmq_memory_high_watermark 内存水位比例
# TYPE swiftmq_memory_high_watermark gauge
swiftmq_memory_high_watermark 0.4
# HELP swiftmq_disk_free_limit_bytes 磁盘剩余空间下限
# TYPE swiftmq_disk_free_limit_bytes gauge
swiftmq_disk_free_limit_bytes 52428800
# HELP swiftmq_queue_messages_ready 队列中的就绪消息数
# TYPE swiftmq_queue_messages_ready gauge
# HELP swiftmq_queue_messages_unacknowledged 队列中的未确认消息数
# TYPE swiftmq_queue_messages_unacknowledged gauge
# HELP swiftmq_queue_consumers 队列上的消费者数
# TYPE swiftmq_queue_consumers gauge
# HELP swiftmq_queue_memory_bytes 队列内存占用估算值
# TYPE swiftmq_queue_memory_bytes gauge
# HELP swiftmq_queue_messages_published_total 队列累计接收的消息数
# TYPE swiftmq_queue_messages_published_total counter
# HELP swiftmq_queue_messages_delivered_total 队列累计投递的消息数
# TYPE swiftmq_queue_messages_delivered_total counter
# HELP swiftmq_queue_messages_acked_total 队列累计确认的消息数
# TYPE swiftmq_queue_messages_acked_total counter
swiftmq_queue_messages_ready{vhost="/",queue="persist.q"} 5
swiftmq_queue_messages_unacknowledged{vhost="/",queue="persist.q"} 0
swiftmq_queue_consumers{vhost="/",queue="persist.q"} 0
swiftmq_queue_memory_bytes{vhost="/",queue="persist.q"} 365
swiftmq_queue_messages_published_total{vhost="/",queue="persist.q"} 5
swiftmq_queue_messages_delivered_total{vhost="/",queue="persist.q"} 0
swiftmq_queue_messages_acked_total{vhost="/",queue="persist.q"} 0
# HELP swiftmq_plugin_info 插件元数据（value 恒为 1，状态见 state 标签）
# TYPE swiftmq_plugin_info gauge
# HELP swiftmq_plugin_up 插件是否在服务（1=enabled，0=其它状态：disabled/failed/down）
# TYPE swiftmq_plugin_up gauge
swiftmq_plugin_info{name="amqp091",version="0.1.0",api_version="v1",state="enabled"} 1
swiftmq_plugin_up{name="amqp091",state="enabled"} 1
swiftmq_plugin_info{name="mqtt",version="0.1.0",api_version="v1",state="enabled"} 1
swiftmq_plugin_up{name="mqtt",state="enabled"} 1
```

---

## 3. Elenco delle metriche (tutte realmente esistenti, origine `internal/management/metrics.go`)

| Metrica | Tipo | Etichette | Semantica |
| --- | --- | --- | --- |
| `swiftmq_up` | gauge | — | Il processo si dichiara vivo (attualmente sempre 1) |
| `swiftmq_build_info` | gauge | `version`,`node` | Informazioni di build, value sempre 1 |
| `swiftmq_resource_blocked` | gauge | — | Se il livello di risorsa blocca il produttore (1=in blocco) |
| `swiftmq_connections` | gauge | — | Numero di connessioni corrente |
| `swiftmq_channels` | gauge | — | Numero di canali corrente |
| `swiftmq_queues` | gauge | — | Numero di code corrente |
| `swiftmq_exchanges` | gauge | — | Numero di exchange corrente |
| `swiftmq_consumers` | gauge | — | Numero di consumatori corrente |
| `swiftmq_queue_messages` | gauge | — | Totale **globale** dei messaggi pronti |
| `swiftmq_queue_messages_unacknowledged` | gauge | — | Totale **globale** dei messaggi non confermati |
| `swiftmq_process_memory_bytes` | gauge | — | Memoria del processo **in uso** (`HeapInuse+StackInuse`) |
| `swiftmq_memory_total_bytes` | gauge | — | Memoria fisica totale |
| `swiftmq_disk_free_bytes` | gauge | — | Spazio disponibile nella directory dei dati |
| `swiftmq_memory_high_watermark` | gauge | — | Proporzione del livello di memoria (high watermark) |
| `swiftmq_disk_free_limit_bytes` | gauge | — | Soglia minima di spazio libero su disco |
| `swiftmq_queue_messages_ready` | gauge | `vhost`,`queue` | Numero di messaggi pronti di una coda |
| `swiftmq_queue_messages_unacknowledged` | gauge | `vhost`,`queue` | Numero di messaggi non confermati di una coda |
| `swiftmq_queue_consumers` | gauge | `vhost`,`queue` | Numero di consumatori di una coda |
| `swiftmq_queue_memory_bytes` | gauge | `vhost`,`queue` | Stima della memoria occupata da una coda |
| `swiftmq_queue_messages_published_total` | counter | `vhost`,`queue` | Numero cumulativo di messaggi ricevuti dalla coda |
| `swiftmq_queue_messages_delivered_total` | counter | `vhost`,`queue` | Numero cumulativo di messaggi consegnati dalla coda |
| `swiftmq_queue_messages_acked_total` | counter | `vhost`,`queue` | Numero cumulativo di messaggi confermati dalla coda |
| `swiftmq_plugin_info` | gauge | `name`,`version`,`api_version`,`state` | Metadati del plugin, value sempre 1 |
| `swiftmq_plugin_up` | gauge | `name`,`state` | Se il plugin è in servizio (1=enabled, 0=altro) |

### 3.1 Avvertenze d'uso (per evitare errori)

- **Due famiglie di serie con lo stesso nome ma cardinalità diversa**: `swiftmq_queue_messages_unacknowledged` ha **sia** una serie globale senza etichette
  **sia** una serie per coda con etichette; mentre sul lato "pronti" quella globale si chiama `swiftmq_queue_messages` e quella per coda
  `swiftmq_queue_messages_ready` (nomi asimmetrici). Quando scrivi le regole, usa `{queue=~".+"}` per selezionare esplicitamente solo la famiglia per coda.
- **Criterio di `swiftmq_process_memory_bytes`**: l'implementazione è `MemStats.HeapInuse + StackInuse` (**memoria in uso**),
  lo stesso criterio usato dal kernel per determinare il livello di memoria; tuttavia il testo del suo `# HELP` recita "byte di memoria richiesti al sistema operativo",
  **un testo che non corrisponde al criterio reale**; fa fede questo documento.
- **Le serie per coda compaiono solo quando la coda esiste**: dopo l'eliminazione della coda la serie scompare (sul lato Prometheus diventa stale).
  Gli avvisi del tipo "la coda dovrebbe esistere ma non ha dati" possono avvalersi di `absent()` o della `or vector(0)` di Grafana.
- **I counter si azzerano al riavvio del processo**: `*_total` è un cumulativo all'interno del processo e riparte da 0 al riavvio; usa `rate()`/`increase()`,
  e non impostare soglie direttamente sui valori assoluti.
- **Nessun segnale di cluster tra le metriche**: vedi §5.

---

## 4. Significato degli avvisi e gestione consigliata (corrispondenti a `prometheus-alerts.yml`)

| Avviso | Condizione di attivazione | Significato | Gestione consigliata |
| --- | --- | --- | --- |
| `SwiftMQScrapeDown` | `up{job="swiftmq"} == 0` 1m | Il target di raccolta è del tutto irraggiungibile | Controlla processo/porta/rete/autenticazione; riavvia e guarda il log di avvio |
| `SwiftMQProcessNotUp` | `swiftmq_up == 0` 1m | Raccolta riuscita ma il processo si dichiara non vivo | Voce di sicurezza; controlla il log di uscita anomala |
| `SwiftMQPluginDown` | `swiftmq_plugin_up == 0` 2m | Plugin in stato disabled/failed/down | `swiftmqctl plugins show <name>` per vedere `runtime_note`; i plugin esterni con `restart=always` di solito si riprendono da soli |
| `SwiftMQResourceBlocked` | `swiftmq_resource_blocked == 1` 5m | Livello di memoria/disco raggiunto, i produttori sono bloccati | Controlla il livello di memoria e lo spazio disponibile su disco; verifica se i consumatori avanzano |
| `SwiftMQMemoryWatermarkHigh` | Memoria in uso > 0.9×livello per 10m | In avvicinamento al livello di memoria | Riduci l'arretrato / aumenta la velocità di consumo, per evitare di innescare il blocco |
| `SwiftMQDiskFreeLow` | Spazio disponibile < 1.5×soglia minima per 10m | La directory dei dati è quasi piena | Espandi/pulisci; raggiunta la soglia minima bloccherai i produttori |
| `SwiftMQQueueBacklogGrowing` | Pronti >10000 e crescita monotona per 15m | La coda accumula continuamente arretrato | Aggiungi consumatori / controlla il lato consumo; verifica anomalie di dead letter/TTL |
| `SwiftMQQueueNoConsumers` | Consumatori=0 e presenza di messaggi pronti per 15m | Nessuno consuma | Controlla il processo del lato consumo; assicurati che i consumatori non siano caduti |
| `SwiftMQUnackedPileUp` | Non confermati >1000 per 15m | I consumatori sono bloccati / non fanno ack | Controlla la logica di elaborazione dei consumatori e il prefetch; se necessario chiudi la connessione e ritrasmetti |
| `SwiftMQConnectionSpike` | Connessioni >10000 per 10m | Numero di connessioni anomalo | Verifica perdite di connessione; i client dovrebbero riutilizzare le connessioni |

> Le soglie (10000 / 1000 ecc.) sono **valori di partenza**; adattale in base alla dimensione delle tue code e alle caratteristiche del tuo traffico.

---

## 5. Lacuna nota: al momento non esistono metriche per il cluster che "perde la maggioranza / resta senza leader"

- **Fatto**: `/metrics` **non** contiene alcuna metrica di tipo cluster (nessuna `swiftmq_cluster_*`). Lo stato del cluster è solo nel JSON di `GET /api/cluster`:
  `{"enabled":true,"mode":"raft","role":"leader","leader":"...","has_quorum":true,"paused":false,...}`.
- **Pertanto** `prometheus-alerts.yml` **non contiene volutamente** avvisi basati su metriche di cluster — anche se scritti **non si attiverebbero mai**
  (Prometheus non segnala errori per nomi di metrica inesistenti), e sarebbe un deliverable "apparentemente corretto ma di fatto inefficace".
- **Soluzioni fai-da-te** (una delle due, entrambe da costruire al di fuori di SwiftMQ, fuori dall'ambito di questo repository):
  1. Usa un exporter JSON generico per raccogliere `/api/cluster`, mappalo su metriche personalizzate (ad esempio `swiftmq_cluster_has_quorum`) e poi crea un avviso su quella metrica;
  2. Usa uno script di sonda che chiami periodicamente `/api/cluster` e invii un avviso quando `has_quorum=false` o `paused=true`.
- Criterio correlato: `has_quorum=false` indica la perdita di contatto con la maggioranza; con `pause_minority` (predefinito) in questo caso **il servizio si sospende e chiude le connessioni**.

---

## 6. Altre voci **non verificate**

- La dashboard Grafana **non è stata importata e verificata in un Grafana reale** (solo la validazione della sintassi JSON è stata superata).
- Le regole di avviso **non sono state caricate e verificate in un Prometheus/Alertmanager reale** (Prometheus non è stato avviato sulla macchina locale).
  Tuttavia i **nomi delle metriche nelle regole sono stati confrontati uno per uno con l'output reale di `/metrics`** (vedi §2/§3), quindi non esiste il problema di "un nome scritto male che non si attiva mai".
