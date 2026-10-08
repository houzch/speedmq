<!-- i18n-switcher -->
[简体中文](../../../README-cn.md) | [繁體中文](../zh-TW/README.md) | [English](../../../README.md) | [日本語](../ja/README.md) | [한국어](../ko/README.md) | [Español](../es/README.md) | [Deutsch](../de/README.md) | [Français](../fr/README.md) | [العربية](../ar/README.md) | [Русский](../ru/README.md) | [Italiano](../it/README.md) | [Nederlands](../nl/README.md) | [Português](../pt/README.md) | [Bahasa Indonesia](../id/README.md) | [ไทย](../th/README.md) | [Tiếng Việt](../vi/README.md) | **Bahasa Melayu** | [Filipino](../fil/README.md)

# SpeedMQ

Perisian tengah mesej **serasi RabbitMQ** yang ditulis dalam Go. Klien RabbitMQ sedia ada **tidak perlu ubah kod, tidak perlu tukar SDK**, hanya tukar alamat sambungan sahaja untuk berintegrasi.

## Pengenalan

- **Keserasian protokol**: AMQP 0-9-1 (termasuk sambungan RabbitMQ) dan MQTT 3.1.1; garis dasar keserasian ialah **semantik RabbitMQ 4.3**.
- **Penggunaan mudah**: satu binari / satu kontena, UI pengurusan sudah terbenam, tidak memerlukan Nginx, pangkalan data atau runtime Node tambahan.
- **Cukup untuk operasi**: UI pengurusan (baris gilir / penukar / sambungan / kebenaran akaun / hos maya / polisi / had / kluster), Prometheus `/metrics`, CLI `speedmqctl`.
- **Port lalai**: `5672` (AMQP), `1883` (MQTT), `15672` (UI pengurusan / HTTP API / metrik).

Keupayaan yang sedia ada: kegigihan (log segmen + gred fsync + pemulihan ranap), pengesahan penerbitan, TTL / surat mati / had panjang, keutamaan pengguna, Direct Reply-To, kluster (metadata Raft + baris gilir kuorum + penghantaran antara nod), mula/henti panas pemalam.

***

## Mula Pantas

### Kaedah satu: Docker (disyorkan)

**Tanpa mengklon repositori: tarik imej dan jalankan terus.**

```bash
docker run -d --name speedmq \
  -p 5672:5672 -p 1883:1883 -p 15672:15672 \
  -v speedmq-data:/var/lib/speedmq \
  houzch/speedmq:1.1.05
```

Imej diterbitkan di dua tempat dengan kandungan sama (pilih yang lebih laju): Docker Hub `houzch/speedmq` dan GitHub GHCR `ghcr.io/houzch/speedmq`; kedua-duanya menyediakan `linux/amd64` dan `linux/arm64`.

- Data disimpan dalam volume bernama `speedmq-data` dan kekal walaupun kontena dicipta semula.
- Henti / buang: `docker stop speedmq`, `docker rm speedmq` (volume data dikekalkan).

**Untuk ubah konfigurasi atau guna compose, klon repositori:**

```bash
git clone https://github.com/houzch/speedmq.git
cd speedmq
docker compose pull && docker compose up -d   # Guna imej yang telah diterbitkan; tukar ke up -d --build untuk bina secara tempatan

docker compose ps        # Status sepatutnya Up (healthy)
docker compose logs -f   # Ikut log
```

- Konfigurasi dipasang baca-sahaja daripada `configs/speedmqd.json`; perubahan berkuat kuasa selepas `docker compose restart`.
- Henti: `docker compose down` (data kekal); `docker compose down -v` (data turut dipadam).

### Kaedah dua: binari tempatan (perlukan Go 1.24+)

```bash
git clone https://github.com/houzch/speedmq.git
cd speedmq
go build -o bin/speedmqd ./cmd/speedmqd
go build -o bin/speedmqctl ./cmd/speedmqctl
./bin/speedmqd -config configs/speedmqd.json
```

> Hasil binaan UI pengurusan tidak dimasukkan ke repositori. Jika mahu menggunakan UI, jalankan `npm ci && npm run build` dalam `web/` dahulu;
> tanpa binaan pun mula serta hantar/terima mesej berfungsi seperti biasa, cuma melawat `/` akan memberitahu "UI pengurusan belum dibina".

### Log masuk kali pertama (wajib tukar akaun lalai dahulu)

| Pintu masuk | Alamat / bukti kelayakan |
| --- | --- |
| UI pengurusan | <http://localhost:15672/>（nama pengguna `guest`，kata laluan `guest`） |
| AMQP | `amqp://guest:guest@localhost:5672/` |
| MQTT | `localhost:1883`（akaun sama seperti di atas） |

Akaun utama bagi instans yang baru dipasang membawa tanda "paksa tukar kata laluan semasa log masuk pertama": selepas log masuk UI pengurusan, anda **dipaksa menukar nama akaun dan kata laluan pada masa yang sama**, dan hanya selepas ditukar barulah boleh masuk ke bahagian pengurusan.

Anda juga boleh terus memanggil API untuk melakukannya (sesuai untuk automasi):

```bash
curl -u guest:guest -X POST -H 'Content-Type: application/json' \
  -d '{"name":"admin","password":"<新口令>"}' \
  http://127.0.0.1:15672/api/users/guest/credentials
```

> ⚠️ `guest/guest` lalai selaras dengan tingkah laku RabbitMQ: **hanya membenarkan log masuk dari mesin tempatan**. Sambungan dari luar kontena / jauh memerlukan `remote_access` dihidupkan untuk pengguna tersebut dalam konfigurasi (konfigurasi contoh sudah dihidupkan untuk senario kontena).
> **Sebaik sahaja perkhidmatan boleh diakses dari luar, tukar bukti kelayakan dengan segera.**

### Sambungkan aplikasi anda (hanya tukar alamat sambungan)

```python
# Python (pika)
import pika
conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
```

```go
// Go (amqp091-go)
conn, _ := amqp.Dial("amqp://guest:guest@127.0.0.1:5672/")
```

```bash
# MQTT (klien mosquitto)
mosquitto_sub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/#' -q 1
mosquitto_pub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/room1/temp' -m 21.5 -q 1
```

HTTP API pengurusan serasi dengan `rabbitmqadmin`; "Tambah baris gilir / penukar" dalam UI pengurusan ialah titik akhir pengisytiharan standard, jadi skrip juga boleh melakukannya:

```bash
# Isytihar baris gilir (baris gilir kuorum dinyatakan melalui arguments: {"x-queue-type":"quorum"})
curl -u guest:guest -X PUT -H 'Content-Type: application/json' \
  -d '{"durable":true,"auto_delete":false,"arguments":{}}' \
  http://127.0.0.1:15672/api/queues/%2F/my.queue
```

### Operasi harian

| Perkara | Pintu masuk |
| --- | --- |
| UI pengurusan | <http://localhost:15672/>：baris gilir / penukar / sambungan / kebenaran akaun / hos maya / polisi / had / suis ciri / kluster, sudut kanan atas boleh menetapkan muat semula automatik dan **bahasa antara muka** |
| Metrik pemantauan | <http://localhost:15672/metrics>（teks Prometheus, perlu pengesahan）; papan pemuka dan amaran lihat [docs/ops/monitoring](ops/monitoring/README.md) |
| Baris arahan | `./bin/speedmqctl status`、`list_queues`、`plugins list`、`plugins disable amqp091`（henti panas, port ditutup serta-merta） |
| Pemeriksaan kesihatan | `nc -z 127.0.0.1 15672`（compose sudah terbina healthcheck） |
| Sandaran dan pemulihan | [docs/ops/backup-restore.md](ops/backup-restore.md) |
| Naik taraf | [docs/ops/upgrade.md](ops/upgrade.md) |
| Garis dasar keselamatan | [docs/ops/security-baseline.md](ops/security-baseline.md) |

Konfigurasi lazim (contoh penuh lihat [configs/speedmqd.json](../../../configs/speedmqd.json)，juga boleh ditimpa dengan pembolehubah persekitaran `SPEEDMQ_*`):

| Item konfigurasi | Penerangan | Lalai |
| --- | --- | --- |
| `data_dir` | Direktori data (mesej + metadata), **wajib digigihkan** | `data` |
| `listeners` | Alamat pendengar setiap protokol, boleh dikonfigurasi TLS | AMQP `:5672` / MQTT `:1883` |
| `management.addr` | Alamat pendengar UI / API pengurusan | `:15672` |
| `management.language` | Bahasa lalai UI pengurusan; jika dikosongkan, dipilih automatik mengikut zon waktu tempat penggunaan | Automatik |
| `storage.fsync` | Gred penulisan cakera `none / os / batch / always`（juga menentukan masa confirm） | `os` |
| `storage.memory_high_watermark`、`storage.disk_free_limit` | Paras sumber: apabila dicetuskan terus menyekat pengeluar, **tidak menggugurkan mesej** | `0.4` / 50 MiB |
| `users` | Jadual pengguna terbina dalam (kata laluan + tag + `remote_access`) | `guest/guest` |
| `cluster.enabled` + `cluster.peers` | Kluster berbilang nod (dimatikan secara lalai), perubahan ahli guna `speedmqctl add_member` | Dimatikan |

> Port mungkin telah digunakan: tukar kepada port lain melalui `listeners` / `management.addr`.

***

## Struktur projek

```
speedmq/
├── cmd/
│   ├── speedmqd/        # titik masuk proses broker (inilah yang perlu dijalankan)
│   └── speedmqctl/      # CLI operasi (melalui HTTP API pengurusan, tidak bergantung pada versi kernel)
├── internal/            # pelaksanaan kernel
│   ├── protocol/        # pemalam protokol: amqp091, mqtt (pengekodan/penyahkodan / kaedah / sesi)
│   ├── broker/          # kernel: vhost, penukar, baris gilir, surat mati, kawalan aliran, pandangan satah pengurusan
│   ├── store/           # kegigihan: log segmen, indeks baris gilir, pemulihan ranap
│   ├── raft/ meta/      # kluster: Raft buatan sendiri dan replikasi metadata
│   ├── management/      # HTTP API pengurusan + metrik Prometheus + perkhidmatan statik UI terbenam
│   ├── transport/ auth/ config/ plugin/
├── pkg/                 # kontrak stabil luaran: API pemalam (plugin) dan protokol wayar pemalam proses luar (sidecar)
├── web/                 # projek frontend UI pengurusan (Vue 3 + Vite), hasil binaan dimasukkan ke binari melalui go:embed semasa binaan
├── configs/             # konfigurasi contoh
├── docs/ops/            # dokumen operasi: sandaran & pemulihan / naik taraf / garis dasar keselamatan / pemantauan
├── Dockerfile、docker-compose.yml
└── speedmq-logo.PNG、1280X1280.PNG（kod QR kumpulan komunikasi）
```

***

## Menyumbang

Kami mengalu-alukan Issue dan Pull Request. Asas kewujudan projek ini ialah **keserasian protokol**, oleh itu:

- Untuk pembetulan pepijat, sila nyatakan tingkah laku RabbitMQ yang sepadan (versi, klien, langkah pembiakan);
- Untuk perubahan yang melibatkan butiran protokol, sila lampirkan hasil perbandingan dengan RabbitMQ;
- Sebelum menghantar, pastikan `go build ./...`、`go vet ./...`、`go test ./...`、`gofmt -l .` semuanya lulus.

***

## Lesen

Projek ini menggunakan [Apache License 2.0](../../../LICENSE)。

Penggunaan, pengubahsuaian dan pengedaran (termasuk penggunaan komersial) dibenarkan, dengan syarat notis hak cipta dan lesen dikekalkan, dan tanpa sebarang jaminan.

Copyright 2026 houzch（lihat [NOTICE](../../../NOTICE)）

***

## Penghargaan

Spesifikasi protokol AMQP 0-9-1 dan semantik tingkah laku [RabbitMQ](https://www.rabbitmq.com/) menjadi rujukan perbandingan untuk kerja keserasian projek ini. Projek ini ialah pelaksanaan bebas, tiada kaitan dengan RabbitMQ rasmi, dan tidak menggunakan kodnya.

***

## Sertai kumpulan komunikasi

Imbas kod untuk menyertai kumpulan komunikasi SpeedMQ, jika ada masalah boleh tanya terus dalam kumpulan:

![Kumpulan komunikasi SpeedMQ](../../../1280X1280.PNG)
