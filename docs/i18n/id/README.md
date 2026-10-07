<!-- i18n-switcher -->
[简体中文](../../../README.md) | [繁體中文](../zh-TW/README.md) | [English](../en/README.md) | [日本語](../ja/README.md) | [한국어](../ko/README.md) | [Español](../es/README.md) | [Deutsch](../de/README.md) | [Français](../fr/README.md) | [العربية](../ar/README.md) | [Русский](../ru/README.md) | [Italiano](../it/README.md) | [Nederlands](../nl/README.md) | [Português](../pt/README.md) | **Bahasa Indonesia** | [ไทย](../th/README.md) | [Tiếng Việt](../vi/README.md) | [Bahasa Melayu](../ms/README.md) | [Filipino](../fil/README.md)

# SwiftMQ

Middleware pesan **kompatibel RabbitMQ** yang ditulis dengan Go. Klien RabbitMQ yang ada **tidak perlu mengubah kode, tidak perlu mengganti SDK** — cukup mengubah alamat koneksi untuk bisa terhubung.

## Pengenalan

- **Kompatibel protokol**: AMQP 0-9-1 (termasuk ekstensi RabbitMQ) dan MQTT 3.1.1; baseline kompatibilitas adalah **semantik RabbitMQ 4.3**.
- **Deployment sederhana**: satu biner / satu kontainer, UI manajemen sudah tertanam, tidak memerlukan Nginx, basis data, atau runtime Node tambahan.
- **Cukup untuk operasional**: UI manajemen (antrean / exchange / koneksi / izin akun / virtual host / policy / limit / klaster), Prometheus `/metrics`, baris perintah `swiftmqctl`.
- **Port bawaan**: `5672` (AMQP), `1883` (MQTT), `15672` (UI manajemen / HTTP API / metrik).

Kemampuan yang sudah dimiliki: persistensi (log segmen + tingkat fsync + pemulihan pasca-crash), publisher confirm, TTL / dead letter / batas panjang, prioritas konsumen, Direct Reply-To, klaster (metadata Raft + antrean quorum + penerusan antar-node), hot start/stop plugin.

***

## Mulai cepat

### Cara pertama: Docker (disarankan)

**Tanpa meng-clone repositori: tarik image-nya lalu jalankan.**

```bash
docker run -d --name swiftmq \
  -p 5672:5672 -p 1883:1883 -p 15672:15672 \
  -v swiftmq-data:/var/lib/swiftmq \
  houzch/swiftmq:1.1.03
```

Image dipublikasikan di dua tempat dengan isi yang sama (pilih yang lebih cepat): Docker Hub `houzch/swiftmq` dan GitHub GHCR `ghcr.io/houzch/swiftmq`; keduanya menyediakan `linux/amd64` dan `linux/arm64`.

- Data tersimpan di volume bernama `swiftmq-data` dan tetap ada meski kontainer dibuat ulang.
- Hentikan / hapus: `docker stop swiftmq`, `docker rm swiftmq` (volume data tetap dipertahankan).

**Untuk mengubah konfigurasi atau memakai compose, clone repositorinya:**

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq
docker compose pull && docker compose up -d   # Memakai image yang sudah dipublikasikan; ganti ke up -d --build untuk build lokal

docker compose ps        # Status seharusnya Up (healthy)
docker compose logs -f   # Ikuti log
```

- Konfigurasi di-mount read-only dari `configs/swiftmqd.json`; perubahan berlaku setelah `docker compose restart`.
- Hentikan: `docker compose down` (data tetap); `docker compose down -v` (data ikut terhapus).

### Cara kedua: biner lokal (memerlukan Go 1.24+)

```bash
git clone https://github.com/houzch/swiftmq.git
cd swiftmq
go build -o bin/swiftmqd ./cmd/swiftmqd
go build -o bin/swiftmqctl ./cmd/swiftmqctl
./bin/swiftmqd -config configs/swiftmqd.json
```

> Hasil build UI manajemen tidak disertakan ke repositori. Jika ingin memakai UI, jalankan `npm ci && npm run build` terlebih dahulu di `web/`;
> tanpa build pun tetap dapat menjalankan dan bertukar pesan secara normal, hanya saja saat mengakses `/` akan muncul pemberitahuan "UI manajemen belum dibangun".

### Login pertama (wajib ganti akun default terlebih dahulu)

| Pintu masuk | Alamat / kredensial |
| --- | --- |
| UI manajemen | <http://localhost:15672/> (nama pengguna `guest`, kata sandi `guest`) |
| AMQP | `amqp://guest:guest@localhost:5672/` |
| MQTT | `localhost:1883` (akun sama seperti di atas) |

Instans yang baru dipasang memiliki akun utama dengan penanda "wajib ganti kata sandi saat login pertama": setelah login ke UI manajemen, Anda **dipaksa untuk mengubah nama akun sekaligus kata sandi**, dan baru bisa masuk ke backend setelah diubah.

Sebagai alternatif, bisa langsung melalui API (cocok untuk otomasi):

```bash
curl -u guest:guest -X POST -H 'Content-Type: application/json' \
  -d '{"name":"admin","password":"<新口令>"}' \
  http://127.0.0.1:15672/api/users/guest/credentials
```

> ⚠️ `guest/guest` bawaan berperilaku sama dengan RabbitMQ: **hanya mengizinkan login dari mesin lokal**. Untuk koneksi dari luar kontainer / jarak jauh, Anda perlu mengaktifkan `remote_access` bagi pengguna tersebut di konfigurasi (konfigurasi contoh sudah mengaktifkannya untuk skenario kontainer).
> **Segera ganti kredensial begitu layanan dapat diakses dari luar.**

### Menghubungkan aplikasi Anda (cukup ubah alamat koneksi)

```python
# Python（pika）
import pika
conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
```

```go
// Go（amqp091-go）
conn, _ := amqp.Dial("amqp://guest:guest@127.0.0.1:5672/")
```

```bash
# MQTT (klien mosquitto)
mosquitto_sub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/#' -q 1
mosquitto_pub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/room1/temp' -m 21.5 -q 1
```

API HTTP manajemen kompatibel dengan `rabbitmqadmin`; tombol "Tambah antrean / exchange" di UI manajemen adalah endpoint deklarasi standar, skrip juga bisa melakukannya:

```bash
# Mendeklarasikan antrean (antrean quorum dinyatakan dengan arguments: {"x-queue-type":"quorum"})
curl -u guest:guest -X PUT -H 'Content-Type: application/json' \
  -d '{"durable":true,"auto_delete":false,"arguments":{}}' \
  http://127.0.0.1:15672/api/queues/%2F/my.queue
```

### Operasional harian

| Hal | Pintu masuk |
| --- | --- |
| UI manajemen | <http://localhost:15672/>: antrean / exchange / koneksi / izin akun / virtual host / policy / limit / feature flag / klaster, di kanan atas dapat mengatur auto-refresh dan **bahasa antarmuka** |
| Metrik pemantauan | <http://localhost:15672/metrics> (teks Prometheus, memerlukan autentikasi); dasbor dan peringatan lihat [dokumentasi pemantauan](ops/monitoring/README.md) |
| Baris perintah | `./bin/swiftmqctl status`, `list_queues`, `plugins list`, `plugins disable amqp091` (hot disable, port langsung tertutup) |
| Pemeriksaan kesehatan | `nc -z 127.0.0.1 15672` (healthcheck sudah terpasang di compose) |
| Pencadangan dan pemulihan | [pencadangan dan pemulihan](ops/backup-restore.md) |
| Peningkatan versi | [peningkatan versi](ops/upgrade.md) |
| Baseline keamanan | [baseline keamanan](ops/security-baseline.md) |

Konfigurasi umum (contoh lengkap lihat [configs/swiftmqd.json](../../../configs/swiftmqd.json), juga dapat ditimpa dengan variabel lingkungan `SWIFTMQ_*`):

| Item konfigurasi | Keterangan | Default |
| --- | --- | --- |
| `data_dir` | Direktori data (pesan + metadata), **wajib dipersistenkan** | `data` |
| `listeners` | Alamat listen tiap protokol, dapat dikonfigurasi TLS | AMQP `:5672` / MQTT `:1883` |
| `management.addr` | Alamat listen UI manajemen / API | `:15672` |
| `management.language` | Bahasa default UI manajemen; jika dikosongkan akan dipilih otomatis berdasarkan zona waktu lokasi deployment | otomatis |
| `storage.fsync` | Tingkat penulisan ke disk `none / os / batch / always` (sekaligus menentukan waktu confirm) | `os` |
| `storage.memory_high_watermark`、`storage.disk_free_limit` | Watermark sumber daya: begitu terpicu akan memblokir produsen, **tidak menghilangkan pesan** | `0.4` / 50 MiB |
| `users` | Tabel pengguna bawaan (kata sandi + tag + `remote_access`) | `guest/guest` |
| `cluster.enabled` + `cluster.peers` | Klaster multi-node (nonaktif secara default), perubahan anggota dengan `swiftmqctl add_member` | nonaktif |

> Port mungkin sudah terpakai: cukup ganti ke port lain melalui `listeners` / `management.addr`.

***

## Struktur proyek

```
swiftmq/
├── cmd/
│   ├── swiftmqd/        # pintu masuk proses broker (inilah yang dijalankan)
│   └── swiftmqctl/      # CLI operasional (lewat HTTP API manajemen, tidak terikat versi kernel)
├── internal/            # implementasi kernel
│   ├── protocol/        # plugin protokol: amqp091、mqtt (encode/decode / method / sesi)
│   ├── broker/          # kernel: vhost、exchange、antrean、dead letter、kontrol aliran、tampilan bidang manajemen
│   ├── store/           # persistensi: log segmen、indeks antrean、pemulihan pasca-crash
│   ├── raft/ meta/      # klaster: Raft buatan sendiri dan replikasi metadata
│   ├── management/      # HTTP API manajemen + metrik Prometheus + layanan statis UI yang tertanam
│   ├── transport/ auth/ config/ plugin/
├── pkg/                 # kontrak stabil ke luar: API plugin (plugin) dan protokol kabel plugin proses eksternal (sidecar)
├── web/                 # proyek frontend UI manajemen (Vue 3 + Vite), hasil build dimasukkan ke biner lewat go:embed saat kompilasi
├── configs/             # contoh konfigurasi
├── docs/ops/            # dokumentasi operasional: pencadangan pemulihan / peningkatan versi / baseline keamanan / pemantauan
├── Dockerfile、docker-compose.yml
└── swiftmq-logo.PNG、1280X1280.PNG (kode QR grup komunikasi)
```

***

## Kontribusi

Kami menyambut baik Issue dan Pull Request. Landasan keberadaan proyek ini adalah **kompatibilitas protokol**, oleh karena itu:

- Saat memperbaiki bug, jelaskan perilaku RabbitMQ yang bersesuaian (versi, klien, langkah reproduksi);
- Untuk perubahan yang menyentuh detail protokol, sertakan hasil perbandingan dengan RabbitMQ;
- Sebelum mengirim, pastikan `go build ./...`, `go vet ./...`, `go test ./...`, `gofmt -l .` semuanya lulus.

***

## Lisensi

Proyek ini menggunakan [Apache License 2.0](../../../LICENSE).

Penggunaan, modifikasi, dan distribusi (termasuk penggunaan komersial) diizinkan, dengan syarat mempertahankan pemberitahuan hak cipta dan lisensi, serta tidak memberikan jaminan apa pun.

Copyright 2026 houzch (lihat [NOTICE](../../../NOTICE))

***

## Ucapan terima kasih

Spesifikasi protokol AMQP 0-9-1 dan semantik perilaku [RabbitMQ](https://www.rabbitmq.com/) menjadi acuan pembanding untuk kerja kompatibilitas proyek ini. Proyek ini merupakan implementasi independen, tidak berafiliasi dengan RabbitMQ resmi, dan tidak menggunakan kodenya.

***

## Bergabung ke grup komunikasi

Pindai kode untuk bergabung ke grup komunikasi SwiftMQ, jika ada pertanyaan bisa langsung ditanyakan di grup:

![Grup komunikasi SwiftMQ](../../../1280X1280.PNG)
