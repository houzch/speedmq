# Pemantauan dan Amaran SpeedMQ

Direktori ini menyediakan templat pemantauan yang boleh digunakan terus:

| Fail | Fungsi |
| --- | --- |
| `prometheus-alerts.yml` | Peraturan amaran Prometheus（`groups: - name: speedmq`） |
| `grafana-dashboard.json` | Papan pemuka Grafana yang boleh diimport（panel meliputi isyarat penting di bawah） |
| `README.md` | Cara guna、senarai metrik、maksud dan pengendalian setiap amaran、jurang yang diketahui |

---

## 1. Cara guna

### 1.1 Pengambilan (Prometheus)

Satah pengurusan（lalai `:15672`）mendedahkan `/metrics` dalam format teks Prometheus，**memerlukan Basic Auth**:

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

> Disyorkan membina akaun baca sahaja secara berasingan untuk pemantauan（tag `monitoring` sudah boleh membaca metrik），jangan guna semula kata laluan pentadbir。

Sahkan pengambilan normal（PowerShell）:

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
(Invoke-WebRequest -Uri 'http://127.0.0.1:15672/metrics' -Headers @{Authorization="Basic $pair"} -UseBasicParsing).Content
curl.exe -s -u guest:guest http://127.0.0.1:15672/metrics
```

### 1.2 Peraturan amaran

Letakkan `prometheus-alerts.yml` ke direktori peraturan Prometheus, rujuk dalam `prometheus.yml` kemudian reload:

```yaml
rule_files:
  - "rules/speedmq-alerts.yml"
```

Peraturan menggunakan `job="speedmq"` secara seragam; jika nama job anda berbeza, sila gantikan di seluruh fail。

### 1.3 Papan pemuka Grafana

`grafana-dashboard.json` diimport melalui **Dashboards → Import → muat naik JSON**，semasa import pilih sumber data Prometheus anda
（dalam papan pemuka menggunakan pembolehubah `${DS_PROMETHEUS}`）。Pembolehubah templat `DS_PROMETHEUS` akan diberi nilai dalam pemetaan import。

**【Belum diverifikasi】** Mesin ini tidak memulakan instans Grafana, tiada pengesahan import sebenar; JSON tersebut hanya menjalani pengesahan sintaks JSON（14 panel, penghuraian lulus）。

---

## 2. Serpihan sebenar `/metrics` (bukti)

Yang berikut ialah **output sebenar** `/metrics` instans `1.0.0` mesin ini（telah membina satu baris gilir durable `persist.q`，
jadi metrik per-queue dengan tag `vhost`/`queue` muncul）:

```
# HELP speedmq_up Sama ada nod masih hidup
# TYPE speedmq_up gauge
speedmq_up 1
# HELP speedmq_build_info Maklumat binaan
# TYPE speedmq_build_info gauge
speedmq_build_info 1{version="1.0.0",node="speedmq@DESKTOP-HBDCVPA"}
# HELP speedmq_resource_blocked Sama ada paras sumber menyekat pengeluar (1=sedang menyekat)
# TYPE speedmq_resource_blocked gauge
speedmq_resource_blocked 0
# HELP speedmq_connections Bilangan sambungan semasa
# TYPE speedmq_connections gauge
speedmq_connections 0
# HELP speedmq_channels Bilangan saluran semasa
# TYPE speedmq_channels gauge
speedmq_channels 0
# HELP speedmq_queues Bilangan baris gilir semasa
# TYPE speedmq_queues gauge
speedmq_queues 0
# HELP speedmq_exchanges Bilangan penukar semasa
# TYPE speedmq_exchanges gauge
speedmq_exchanges 6
# HELP speedmq_consumers Bilangan pengguna semasa
# TYPE speedmq_consumers gauge
speedmq_consumers 0
# HELP speedmq_queue_messages Jumlah mesej sedia global
# TYPE speedmq_queue_messages gauge
speedmq_queue_messages 0
# HELP speedmq_queue_messages_unacknowledged Jumlah mesej belum diakui global
# TYPE speedmq_queue_messages_unacknowledged gauge
speedmq_queue_messages_unacknowledged 0
# HELP speedmq_process_memory_bytes Bilangan bait memori yang dipohon oleh proses ini daripada sistem pengendalian
# TYPE speedmq_process_memory_bytes gauge
speedmq_process_memory_bytes 1564672
# HELP speedmq_memory_total_bytes Jumlah memori fizikal
# TYPE speedmq_memory_total_bytes gauge
speedmq_memory_total_bytes 34181279744
# HELP speedmq_disk_free_bytes Ruang tersedia direktori data
# TYPE speedmq_disk_free_bytes gauge
speedmq_disk_free_bytes 240091688960
# HELP speedmq_memory_high_watermark Nisbah paras memori
# TYPE speedmq_memory_high_watermark gauge
speedmq_memory_high_watermark 0.4
# HELP speedmq_disk_free_limit_bytes Had bawah ruang baki cakera
# TYPE speedmq_disk_free_limit_bytes gauge
speedmq_disk_free_limit_bytes 52428800
# HELP speedmq_queue_messages_ready Bilangan mesej sedia dalam baris gilir
# TYPE speedmq_queue_messages_ready gauge
# HELP speedmq_queue_messages_unacknowledged Bilangan mesej belum diakui dalam baris gilir
# TYPE speedmq_queue_messages_unacknowledged gauge
# HELP speedmq_queue_consumers Bilangan pengguna pada baris gilir
# TYPE speedmq_queue_consumers gauge
# HELP speedmq_queue_memory_bytes Anggaran penggunaan memori baris gilir
# TYPE speedmq_queue_memory_bytes gauge
# HELP speedmq_queue_messages_published_total Bilangan mesej terkumpul diterima baris gilir
# TYPE speedmq_queue_messages_published_total counter
# HELP speedmq_queue_messages_delivered_total Bilangan mesej terkumpul dihantar baris gilir
# TYPE speedmq_queue_messages_delivered_total counter
# HELP speedmq_queue_messages_acked_total Bilangan mesej terkumpul diakui baris gilir
# TYPE speedmq_queue_messages_acked_total counter
speedmq_queue_messages_ready{vhost="/",queue="persist.q"} 5
speedmq_queue_messages_unacknowledged{vhost="/",queue="persist.q"} 0
speedmq_queue_consumers{vhost="/",queue="persist.q"} 0
speedmq_queue_memory_bytes{vhost="/",queue="persist.q"} 365
speedmq_queue_messages_published_total{vhost="/",queue="persist.q"} 5
speedmq_queue_messages_delivered_total{vhost="/",queue="persist.q"} 0
speedmq_queue_messages_acked_total{vhost="/",queue="persist.q"} 0
# HELP speedmq_plugin_info Metadata pemalam (value sentiasa 1, status lihat tag state)
# TYPE speedmq_plugin_info gauge
# HELP speedmq_plugin_up Sama ada pemalam dalam perkhidmatan (1=enabled, 0=status lain: disabled/failed/down)
# TYPE speedmq_plugin_up gauge
speedmq_plugin_info{name="amqp091",version="0.1.0",api_version="v1",state="enabled"} 1
speedmq_plugin_up{name="amqp091",state="enabled"} 1
speedmq_plugin_info{name="mqtt",version="0.1.0",api_version="v1",state="enabled"} 1
speedmq_plugin_up{name="mqtt",state="enabled"} 1
```

---

## 3. Senarai metrik (semuanya benar-benar wujud, sumber `internal/management/metrics.go`)

| Metrik | Jenis | Tag | Semantik |
| --- | --- | --- | --- |
| `speedmq_up` | gauge | — | Proses melaporkan sendiri masih hidup（pada masa ini sentiasa 1） |
| `speedmq_build_info` | gauge | `version`,`node` | Maklumat binaan, value sentiasa 1 |
| `speedmq_resource_blocked` | gauge | — | Sama ada paras sumber menyekat pengeluar（1=sedang menyekat） |
| `speedmq_connections` | gauge | — | Bilangan sambungan semasa |
| `speedmq_channels` | gauge | — | Bilangan saluran semasa |
| `speedmq_queues` | gauge | — | Bilangan baris gilir semasa |
| `speedmq_exchanges` | gauge | — | Bilangan penukar semasa |
| `speedmq_consumers` | gauge | — | Bilangan pengguna semasa |
| `speedmq_queue_messages` | gauge | — | Jumlah mesej sedia **global** |
| `speedmq_queue_messages_unacknowledged` | gauge | — | Jumlah mesej belum diakui **global** |
| `speedmq_process_memory_bytes` | gauge | — | Memori **sedang digunakan** proses（`HeapInuse+StackInuse`） |
| `speedmq_memory_total_bytes` | gauge | — | Jumlah memori fizikal |
| `speedmq_disk_free_bytes` | gauge | — | Ruang tersedia direktori data |
| `speedmq_memory_high_watermark` | gauge | — | Nisbah paras memori |
| `speedmq_disk_free_limit_bytes` | gauge | — | Had bawah baki cakera |
| `speedmq_queue_messages_ready` | gauge | `vhost`,`queue` | Bilangan mesej sedia sesuatu baris gilir |
| `speedmq_queue_messages_unacknowledged` | gauge | `vhost`,`queue` | Bilangan mesej belum diakui sesuatu baris gilir |
| `speedmq_queue_consumers` | gauge | `vhost`,`queue` | Bilangan pengguna sesuatu baris gilir |
| `speedmq_queue_memory_bytes` | gauge | `vhost`,`queue` | Anggaran penggunaan memori sesuatu baris gilir |
| `speedmq_queue_messages_published_total` | counter | `vhost`,`queue` | Bilangan mesej terkumpul diterima baris gilir |
| `speedmq_queue_messages_delivered_total` | counter | `vhost`,`queue` | Bilangan mesej terkumpul dihantar baris gilir |
| `speedmq_queue_messages_acked_total` | counter | `vhost`,`queue` | Bilangan mesej terkumpul diakui baris gilir |
| `speedmq_plugin_info` | gauge | `name`,`version`,`api_version`,`state` | Metadata pemalam, value sentiasa 1 |
| `speedmq_plugin_up` | gauge | `name`,`state` | Sama ada pemalam dalam perkhidmatan（1=enabled，0=lain） |

### 3.1 Perhatian penggunaan (elakkan kesilapan penulisan)

- **Dua keluarga siri dengan nama sama tetapi kardinaliti berbeza**: `speedmq_queue_messages_unacknowledged` **mempunyai** siri global tanpa tag、
  **dan juga** siri per-queue dengan tag; manakala bahagian "sedia" global dipanggil `speedmq_queue_messages`、per-queue dipanggil
  `speedmq_queue_messages_ready`（nama tidak simetri）。Semasa menulis peraturan gunakan `{queue=~".+"}` untuk jelas hanya mengambil keluarga per-queue。
- **Skop `speedmq_process_memory_bytes`**: pelaksanaannya ialah `MemStats.HeapInuse + StackInuse`（**memori sedang digunakan**），
  sama skop dengan penentuan paras memori kernel; tetapi teks `# HELP`nya menulis "bilangan bait memori yang dipohon daripada sistem pengendalian"，**teks tidak selaras dengan skop sebenar**，
  gunakan dokumen ini sebagai rujukan。
- **Siri per-queue hanya muncul apabila baris gilir wujud**: selepas baris gilir dipadam siri tersebut hilang（di pihak Prometheus akan menjadi stale）。
  Amaran yang melibatkan "baris gilir sepatutnya wujud tetapi tiada data" boleh digandingkan dengan `absent()` atau `or vector(0)` Grafana。
- **Counter kembali kepada sifar selepas proses mula semula**: `*_total` ialah jumlah terkumpul dalam proses, mula semula bermakna bermula dari 0; sila gunakan `rate()`/`increase()`，
  jangan tetapkan ambang terus pada nilai mutlak。
- **Tiada isyarat kluster dalam metrics**: lihat §5。

---

## 4. Maksud amaran dan pengendalian yang dicadangkan (sepadan `prometheus-alerts.yml`)

| Amaran | Syarat pencetus | Maksud | Pengendalian dicadangkan |
| --- | --- | --- | --- |
| `SpeedMQScrapeDown` | `up{job="speedmq"} == 0` 1m | Keseluruhan sasaran pengambilan tidak dapat dicapai | Periksa proses/port/rangkaian/pengesahan; mula semula dan lihat log permulaan |
| `SpeedMQProcessNotUp` | `speedmq_up == 0` 1m | Pengambilan berjaya tetapi proses melaporkan tidak hidup | Item jaring keselamatan, periksa log keluar abnormal |
| `SpeedMQPluginDown` | `speedmq_plugin_up == 0` 2m | Pemalam disabled/failed/down | `speedmqctl plugins show <name>` lihat `runtime_note`; pemalam luaran `restart=always` biasanya pulih sendiri |
| `SpeedMQResourceBlocked` | `speedmq_resource_blocked == 1` 5m | Paras memori/cakera dicetuskan, pengeluar disekat | Periksa paras memori dan ruang cakera; pastikan pengguna terus maju |
| `SpeedMQMemoryWatermarkHigh` | Nisbah memori sedang digunakan > 0.9× paras 10m | Menghampiri paras memori | Kurangkan timbunan/percepatkan penggunaan, elakkan pencetusan sekatan |
| `SpeedMQDiskFreeLow` | Tersedia < 1.5× had bawah cakera 10m | Direktori data hampir penuh | Tambah kapasiti/bersihkan; sampai had bawah akan menyekat pengeluar |
| `SpeedMQQueueBacklogGrowing` | Sedia >10000 dan meningkat secara monoton 15m | Baris gilir terus menimbun | Tambah pengguna / periksa bahagian penggunaan; siasat anomali surat mati/TTL |
| `SpeedMQQueueNoConsumers` | Pengguna=0 dan ada mesej sedia 15m | Tiada siapa menggunakan | Periksa proses bahagian penggunaan; pastikan pengguna tidak terputus |
| `SpeedMQUnackedPileUp` | Belum diakui >1000 15m | Pengguna tersekat/tidak ack | Periksa logik pemprosesan pengguna dan prefetch; jika perlu tutup sambungan dan hantar semula |
| `SpeedMQConnectionSpike` | Sambungan >10000 10m | Bilangan sambungan abnormal | Periksa kebocoran sambungan; klien sepatutnya menggunakan semula sambungan |

> Ambang（10000 / 1000 dsb.）ialah **nilai titik permulaan**，sila laraskan mengikut skala baris gilir dan ciri perniagaan anda。

---

## 5. Jurang diketahui: kluster "kehilangan majoriti / tiada leader" pada masa ini tiada metrik

- **Fakta**: `/metrics` **tiada** sebarang metrik jenis kluster（tiada `speedmq_cluster_*`）。Status kluster hanya dalam JSON `GET /api/cluster`:
  `{"enabled":true,"mode":"raft","role":"leader","leader":"...","has_quorum":true,"paused":false,...}`。
- **Oleh itu** `prometheus-alerts.yml` **sengaja tidak menulis** amaran berdasarkan metrik kluster—— tulis pun **tidak akan pernah mencetuskan**
  （Prometheus tidak akan melaporkan ralat kerana nama metrik tidak wujud），itu tergolong dalam penghantaran "kelihatan betul, sebenarnya tidak berkesan"。
- **Penyelesaian bina sendiri**（pilih satu daripada dua, kedua-duanya perlu anda bina di luar SpeedMQ, bukan dalam skop repositori ini）:
  1. Guna JSON exporter umum untuk mengambil `/api/cluster`，memetakan kepada metrik tersuai（seperti `speedmq_cluster_has_quorum`），kemudian amaran pada metrik tersebut;
  2. Guna skrip prob untuk memanggil `/api/cluster` secara berkala，apabila `has_quorum=false` atau `paused=true` cetuskan amaran。
- Skop ambang berkaitan: `has_quorum=false` bermakna terputus hubungan dengan majoriti; di bawah `pause_minority`（lalai）pada masa ini **perkhidmatan akan dijeda dan sambungan diputuskan**。

---

## 6. Item lain **belum diverifikasi**

- Papan pemuka Grafana **belum disahkan melalui import dalam Grafana sebenar**（hanya pengesahan sintaks JSON lulus）。
- Peraturan amaran **belum disahkan dimuatkan dalam Prometheus/Alertmanager sebenar**（mesin ini tidak memulakan Prometheus）。
  Tetapi **nama metrik dalam peraturan telah disemak satu per satu terhadap output sebenar `/metrics`**（lihat §2/§3），tiada masalah "nama salah tulis menyebabkan tidak pernah mencetuskan"。
