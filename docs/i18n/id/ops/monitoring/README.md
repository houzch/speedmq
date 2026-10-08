# SpeedMQ Pemantauan dan Peringatan

Direktori ini menyediakan template pemantauan yang siap pakai:

| File | Fungsi |
| --- | --- |
| `prometheus-alerts.yml` | Aturan peringatan Prometheus (`groups: - name: speedmq`) |
| `grafana-dashboard.json` | Dasbor Grafana yang dapat diimpor (panel mencakup sinyal kunci di bawah) |
| `README.md` | Cara pakai, daftar metrik, makna dan penanganan setiap peringatan, celah yang diketahui |

---

## 1. Cara pakai

### 1.1 Scrape (Prometheus)

Bidang manajemen (default `:15672`) mengekspos `/metrics` dalam format teks Prometheus, **memerlukan Basic Auth**:

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

> Disarankan membuat akun baca-saja terpisah untuk pemantauan (tag `monitoring` sudah cukup untuk membaca metrik), jangan menggunakan ulang kata sandi administrator.

Verifikasi apakah scrape berjalan normal (PowerShell):

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
(Invoke-WebRequest -Uri 'http://127.0.0.1:15672/metrics' -Headers @{Authorization="Basic $pair"} -UseBasicParsing).Content
curl.exe -s -u guest:guest http://127.0.0.1:15672/metrics
```

### 1.2 Aturan peringatan

Letakkan `prometheus-alerts.yml` ke direktori aturan Prometheus, rujuk di `prometheus.yml` lalu reload:

```yaml
rule_files:
  - "rules/speedmq-alerts.yml"
```

Di dalam aturan secara seragam menggunakan `job="speedmq"`; jika nama job Anda berbeda, ganti seluruhnya.

### 1.3 Dasbor Grafana

`grafana-dashboard.json` diimpor melalui **Dashboards → Import → unggah JSON**, saat impor pilih sumber data Prometheus Anda
(di dasbor dirujuk dengan variabel `${DS_PROMETHEUS}`). Variabel template `DS_PROMETHEUS` akan diberi nilai pada pemetaan impor.

**【Belum Diverifikasi】** Mesin lokal tidak menjalankan instans Grafana, belum melakukan verifikasi impor sungguhan; JSON tersebut hanya menjalani validasi sintaks JSON (14 panel, parsing lulus).

---

## 2. Potongan `/metrics` sungguhan (bukti)

Berikut adalah **keluaran sungguhan** `/metrics` instans `1.0.0` mesin lokal (sudah dibuat satu antrean durable `persist.q`,
sehingga metrik per-queue dengan label `vhost`/`queue` muncul):

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

## 3. Daftar metrik (semuanya benar-benar ada, sumber `internal/management/metrics.go`)

| Metrik | Tipe | Label | Semantik |
| --- | --- | --- | --- |
| `speedmq_up` | gauge | — | Proses melaporkan dirinya hidup (saat ini selalu 1) |
| `speedmq_build_info` | gauge | `version`,`node` | Informasi build, value selalu 1 |
| `speedmq_resource_blocked` | gauge | — | Apakah watermark sumber daya memblokir produsen (1=sedang memblokir) |
| `speedmq_connections` | gauge | — | Jumlah koneksi saat ini |
| `speedmq_channels` | gauge | — | Jumlah channel saat ini |
| `speedmq_queues` | gauge | — | Jumlah antrean saat ini |
| `speedmq_exchanges` | gauge | — | Jumlah exchange saat ini |
| `speedmq_consumers` | gauge | — | Jumlah konsumen saat ini |
| `speedmq_queue_messages` | gauge | — | Total pesan siap **global** |
| `speedmq_queue_messages_unacknowledged` | gauge | — | Total pesan belum di-ack **global** |
| `speedmq_process_memory_bytes` | gauge | — | Memori proses yang **sedang dipakai** (`HeapInuse+StackInuse`) |
| `speedmq_memory_total_bytes` | gauge | — | Total memori fisik |
| `speedmq_disk_free_bytes` | gauge | — | Ruang tersedia direktori data |
| `speedmq_memory_high_watermark` | gauge | — | Rasio watermark memori |
| `speedmq_disk_free_limit_bytes` | gauge | — | Batas bawah sisa disk |
| `speedmq_queue_messages_ready` | gauge | `vhost`,`queue` | Jumlah pesan siap antrean tertentu |
| `speedmq_queue_messages_unacknowledged` | gauge | `vhost`,`queue` | Jumlah pesan belum di-ack antrean tertentu |
| `speedmq_queue_consumers` | gauge | `vhost`,`queue` | Jumlah konsumen antrean tertentu |
| `speedmq_queue_memory_bytes` | gauge | `vhost`,`queue` | Estimasi penggunaan memori antrean tertentu |
| `speedmq_queue_messages_published_total` | counter | `vhost`,`queue` | Jumlah pesan yang diterima antrean secara kumulatif |
| `speedmq_queue_messages_delivered_total` | counter | `vhost`,`queue` | Jumlah pesan yang dikirim antrean secara kumulatif |
| `speedmq_queue_messages_acked_total` | counter | `vhost`,`queue` | Jumlah pesan yang di-ack antrean secara kumulatif |
| `speedmq_plugin_info` | gauge | `name`,`version`,`api_version`,`state` | Metadata plugin, value selalu 1 |
| `speedmq_plugin_up` | gauge | `name`,`state` | Apakah plugin melayani (1=enabled, 0=lainnya) |

### 3.1 Catatan penggunaan (agar tidak salah tulis)

- **Dua famili seri dengan nama sama tetapi kardinalitas berbeda**: `speedmq_queue_messages_unacknowledged` **baik** memiliki seri global tanpa label,
  **maupun** seri per-queue dengan label; sedangkan sisi "siap" global bernama `speedmq_queue_messages`, per-queue bernama
  `speedmq_queue_messages_ready` (nama tidak simetris). Saat menulis aturan gunakan `{queue=~".+"}` untuk secara eksplisit hanya mengambil famili per-queue.
- **Ketentuan `speedmq_process_memory_bytes`**: implementasinya adalah `MemStats.HeapInuse + StackInuse` (**memori yang sedang dipakai**),
  sama dengan ketentuan penilaian watermark memori kernel; tetapi teks `# HELP`-nya menulis "byte memori yang diminta dari sistem operasi", **teks tidak sesuai dengan ketentuan sebenarnya**,
  dokumen ini yang berlaku.
- **Seri per-queue hanya muncul saat antrean ada**: setelah antrean dihapus seri tersebut hilang (di sisi Prometheus akan menjadi stale).
  Peringatan yang melibatkan "antrean seharusnya ada tetapi tidak ada data" dapat dikombinasikan dengan `absent()` atau `or vector(0)` di Grafana.
- **counter kembali ke nol setelah proses restart**: `*_total` adalah akumulasi dalam proses, restart memulai dari 0; gunakan `rate()`/`increase()`,
  jangan menetapkan ambang batas langsung pada nilai absolut.
- **Sinyal klaster tidak memiliki metrics**: lihat §5.

---

## 4. Makna peringatan dan penanganan yang disarankan (bersesuaian dengan `prometheus-alerts.yml`)

| Peringatan | Kondisi pemicu | Makna | Penanganan yang disarankan |
| --- | --- | --- | --- |
| `SpeedMQScrapeDown` | `up{job="speedmq"} == 0` 1m | Target scrape tidak dapat dijangkau secara keseluruhan | Periksa proses/port/jaringan/autentikasi; restart dan lihat log startup |
| `SpeedMQProcessNotUp` | `speedmq_up == 0` 1m | Scrape berhasil tetapi proses melaporkan tidak hidup | Item jaring pengaman, periksa log keluar abnormal |
| `SpeedMQPluginDown` | `speedmq_plugin_up == 0` 2m | Plugin disabled/failed/down | `speedmqctl plugins show <name>` lihat `runtime_note`; plugin eksternal `restart=always` umumnya menyembuhkan diri |
| `SpeedMQResourceBlocked` | `speedmq_resource_blocked == 1` 5m | Watermark memori/disk terpicu, produsen diblokir | Periksa watermark memori dan disk tersedia; pastikan konsumen bergerak maju |
| `SpeedMQMemoryWatermarkHigh` | Rasio memori terpakai > 0.9×watermark 10m | Mendekati watermark memori | Turunkan penumpukan/percepat konsumsi, cegah pemicuan blokir |
| `SpeedMQDiskFreeLow` | Tersedia < 1.5×batas bawah disk 10m | Direktori data hampir penuh | Perluas/bersihkan; mencapai batas bawah akan memblokir produsen |
| `SpeedMQQueueBacklogGrowing` | Siap >10000 dan tumbuh monoton 15m | Antrean terus menumpuk | Tambah konsumen / periksa sisi konsumen; selidiki anomali dead letter/TTL |
| `SpeedMQQueueNoConsumers` | Konsumen=0 dan ada pesan siap 15m | Tidak ada yang mengonsumsi | Periksa proses sisi konsumen; pastikan konsumen tidak terputus |
| `SpeedMQUnackedPileUp` | Belum di-ack >1000 15m | Konsumen tersangkut/tidak ack | Periksa logika pemrosesan konsumen dan prefetch; tutup koneksi dan kirim ulang jika perlu |
| `SpeedMQConnectionSpike` | Koneksi >10000 10m | Jumlah koneksi tidak normal | Periksa kebocoran koneksi; klien harus menggunakan ulang koneksi |

> Ambang batas (10000 / 1000 dan lain-lain) adalah **nilai awal**, silakan sesuaikan dengan skala antrean dan karakteristik bisnis Anda.

---

## 5. Celah yang diketahui: klaster "kehilangan mayoritas / tanpa leader" saat ini tidak memiliki metrik

- **Fakta**: `/metrics` **tidak** memiliki metrik jenis klaster apa pun (tidak ada `speedmq_cluster_*`). Status klaster hanya ada di JSON `GET /api/cluster`:
  `{"enabled":true,"mode":"raft","role":"leader","leader":"...","has_quorum":true,"paused":false,...}`.
- **Karena itu** `prometheus-alerts.yml` **sengaja tidak menulis** peringatan berbasis metrik klaster —— jika ditulis juga **tidak akan pernah terpicu**
  (Prometheus tidak akan error karena nama metrik tidak ada), itu termasuk pengiriman yang "terlihat benar, padahal tidak valid".
- **Solusi buat sendiri** (pilih salah satu, keduanya perlu Anda bangun di luar SpeedMQ, di luar cakupan repositori ini):
  1. Gunakan JSON exporter umum untuk scrape `/api/cluster`, petakan menjadi metrik kustom (seperti `speedmq_cluster_has_quorum`), lalu beri peringatan pada metrik tersebut;
  2. Gunakan skrip probe untuk memanggil `/api/cluster` secara berkala, beri tanda peringatan saat `has_quorum=false` atau `paused=true`.
- Ketentuan ambang terkait: `has_quorum=false` berarti terputus dari mayoritas; di bawah `pause_minority` (default), saat itu **layanan akan menangguhkan dan memutus koneksi**.

---

## 6. Item **belum diverifikasi** lainnya

- Dasbor Grafana **belum diverifikasi dengan impor di Grafana sungguhan** (hanya validasi sintaks JSON yang lulus).
- Aturan peringatan **belum diverifikasi dengan memuat di Prometheus/Alertmanager sungguhan** (mesin lokal tidak menjalankan Prometheus).
  Tetapi **nama metrik di dalam aturan sudah dicocokkan satu per satu dengan keluaran sungguhan `/metrics`** (lihat §2/§3), tidak ada masalah "nama salah sehingga tidak pernah terpicu".
