# SpeedMQ Peningkatan Versi dan Rencana Migrasi

> Versi yang berlaku: `1.0.0` (`broker.Version`, lihat `speedmq_build_info` di `/metrics`).
> Semua kesimpulan "hasil uji nyata" dalam dokumen ini berasal dari operasi nyata di mesin lokal; yang belum diuji nyata semuanya ditandai secara eksplisit **【Belum Diverifikasi】**.
> Lingkungan mesin lokal: Windows + PowerShell 5.1, Go 1.27.1 windows/386, `data_dir` sementara + port non-default.

---

## 1. Migrasi (beralih dari RabbitMQ ke SpeedMQ)

Posisi proyek ini adalah **kompatibel pada level protokol AMQP 0-9-1**, sehingga "migrasi" sebagian besar adalah **mengubah alamat koneksi**:

- Kode bisnis tidak perlu diubah sama sekali, hanya mengubah `host/port/vhost` (dokumen desain G3 "migrasi tanpa biaya").
- Rantai alat manajemen (`rabbitmqadmin`, UI manajemen, skrip pemantauan) cukup diarahkan ke port bidang manajemen, bentuk antarmuka selaras dengan RabbitMQ (konvensi seperti `amq.default`, `%2F`, `{error, reason}` disalin apa adanya).
- Port default sama dengan RabbitMQ: AMQP `5672`, bidang manajemen `15672`; MQTT `1883`, RPC antar-node `25672`.

**Perbedaan semantik yang perlu diperiksa sendiri sebelum migrasi** (semuanya disengaja oleh repositori ini, berdasarkan README / dokumen desain):

| Item | Perilaku SpeedMQ | Dampak migrasi |
| --- | --- | --- |
| Antrean sementara (non-persistent dan non-exclusive) | **Menolak deklarasi** (541), `auto_delete` tidak dikecualikan | Klien lama yang bergantung pada antrean jenis ini akan gagal, perlu diubah menjadi durable atau exclusive |
| vhost default `/` | **Tidak dapat dihapus** (400), RabbitMQ mengizinkan | Skrip otomasi yang menghapus vhost default akan gagal (ini satu-satunya batasan keamanan aktif) |
| Data antrean klasik | **Tidak direplikasi**, data hanya di node Owner | Jika perlu redundansi antar-node, gunakan antrean quorum `x-queue-type=quorum` |
| Antrean quorum | Mendukung penambahan replika, **tidak mendukung pengurangan** | Rencanakan sekali dengan tepat |
| Plugin | Tidak ada ekosistem plugin Erlang, AMQP 1.0 / STOMP belum diimplementasikan | Skenario yang menggunakan protokol ini belum dapat dimigrasikan |

**Migrasi data**: format penyimpanan SpeedMQ dan RabbitMQ tidak kompatibel, **tidak menyediakan alat pemindahan data online/offline**.
Cara migrasi adalah "buat SpeedMQ kosong baru → jalankan ganda untuk verifikasi → alihkan aliran secara bertahap". **【Belum Diverifikasi】** Dokumen ini tidak memuat latihan pemindahan data RabbitMQ yang nyata.

---

## 2. Prinsip umum peningkatan versi

1. **Cadangkan terlebih dahulu** (lihat `backup-restore.md`) — jaring pengaman jika peningkatan gagal.
2. **Hentikan proses terlebih dahulu baru ganti** (direktori data memiliki batasan penulis tunggal, lihat §4.2).
3. **Setelah peningkatan wajib diverifikasi**: proses dapat berjalan, `/api/overview` dapat dibaca, `/metrics` dapat di-scrape, jumlah pesan antrean sama dengan sebelum pencadangan.
4. Peningkatan klaster **bergulir per node**, hanya satu node dalam satu waktu (lihat §5).

---

## 3. Tata letak direktori data (dasar faktual untuk peningkatan/migrasi)

Tata letak `data_dir` yang diperoleh dari **uji nyata** instans mandiri mesin lokal:

```
data/
├── meta/
│   ├── state.json        # Snapshot metadata mode mandiri (vhost/exchange/antrean/binding/pengguna/izin/policy)
│   ├── users.seeded      # Penanda bootstrap: users di file konfigurasi sudah pernah di-seed
│   ├── vhosts.seeded     # Penanda bootstrap: vhosts di file konfigurasi sudah pernah di-seed
│   ├── raft.state        # [mode klaster] Term/pemungutan suara Raft
│   ├── raft.log          # [mode klaster] Log Raft
│   └── snapshot.json     # [mode klaster] Snapshot Raft + tabel anggota
├── msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/
│   ├── 000001.seg        # File segmen (badan pesan + properti), format record: <len u32><crc32 u32><payload>
│   └── index/000001.idx  # Indeks antrean: seq-id → (nomor segmen, offset dalam segmen, panjang, status)
└── quorum/<safe(vhost)>/<safe(queue)>/   # [klaster] Satu grup Raft per antrean quorum (log/snapshot)
```

**Catatan (dua hal yang berlawanan dengan intuisi, keduanya berdasarkan kode/uji nyata)**:

- Dalam mode klaster, file persistensi Raft **langsung diletakkan di bawah `meta/`** (`raft.state` / `raft.log` / `snapshot.json`),
  **tidak ada subdirektori `meta/raft/`**. Dasar: konstanta nama file di `internal/raft/log.go` + `Dir: filepath.Join(b.cfg.DataDir, "meta")`
  di `internal/broker/cluster.go`. **【Terverifikasi（2026-10-10, klaster 3 node Docker Desktop lokal）】** —— Isi cadangan volume node klaster langsung terlihat memuat `meta/raft.log`, `meta/raft.state`, `meta/*.seeded` (tidak terlihat subdirektori `meta/raft/`).
- Nama direktori **bukan nama vhost / antrean asli**, melainkan hasil pengodean `store.SafeDirName`: menambahkan awalan `q_`, byte yang bukan `[A-Za-z0-9._-]` di-escape dengan `%XX`.
  Uji nyata: vhost `/` → direktori `q_%2F`, antrean `persist.q` → direktori `q_persist.q`.
  Desain ini untuk menghindari path traversal dan nama perangkat yang direservasi Windows (`con`/`nul` dan lain-lain).

---

## 4. Kompatibilitas data

### 4.1 Apakah data lama dapat langsung dibaca — bisa

- **Format indeks kompatibel maju**: M8-1 menambahkan field "nomor segmen" pada record indeks (25 byte); **format lama (21 byte, tanpa nomor segmen) masih dapat dibaca apa adanya**,
  saat dibaca setara dengan "hanya ada satu segmen (seg=1)", **peningkatan tidak memerlukan skrip migrasi**.
  Dasar: konstanta `indexEntrySize` / `legacyIndexEntrySize` dan logika `recover()` di `internal/store/store.go`; README M8-1.
- **Semantik crash tidak berubah**: setiap record membawa prefiks panjang + CRC32, saat pemulihan **membuang record setengah tertulis/rusak di ekor** dan memotongnya.
  Uji nyata (lihat `backup-restore.md` §6): setelah proses berhenti lalu restart, 5 pesan persisten pada antrean durable **semuanya pulih**,
  log memunculkan `已从磁盘恢复队列消息 ... messages=5`.

**【Terverifikasi（2026-10-10, mesin lokal）】** Pembacaan antar-versi mayor telah diuji: data antrean durable yang ditulis dengan tag `v1.1.0` dipulihkan apa adanya di `1.1.05` (perintah dan keluaran lihat §5).

### 4.2 Ketentuan `vhosts` / `users` di konfigurasi (jebakan yang paling mudah terpicu saat peningkatan)

- Keduanya **hanya berlaku saat bootstrap pertama**: startup pertama akan menulis vhosts/users di konfigurasi ke metadata dan meninggalkan file penanda
  `meta/vhosts.seeded` / `meta/users.seeded`; **setelah itu metadata yang menjadi acuan**.
- Karena itu **saat peningkatan/mengganti konfigurasi, jangan berharap dapat menambah/menghapus akun atau vhost melalui perubahan file konfigurasi** — perubahan tidak akan berlaku;
  gunakan API manajemen atau `speedmqctl`.
- Sebaliknya, peningkatan **tidak akan** menimpa akun yang sudah ada dengan konfigurasi: kata sandi yang diubah saat runtime tidak akan dikembalikan ke nilai lama di konfigurasi oleh restart,
  akun yang dihapus saat runtime juga tidak akan hidup kembali. Dasar: logika penanda seeding di `cluster.go`; README M8-4 / M8-7.

**【Terverifikasi（2026-10-10, mesin lokal）】** Perubahan saat runtime memang tidak ditimpa oleh konfigurasi: sebelum peningkatan, kata sandi `guest` diubah menjadi `rotated` melalui API manajemen; setelah restart dengan `1.1.05` + `data_dir` yang sama, `guest/guest` (kata sandi lama di konfigurasi) mengembalikan **401** dan `guest/rotated` mengembalikan **200**.

### 4.3 Rotasi segmen dan reklamasi disk

- Pesan dipecah menjadi segmen berdasarkan ukuran (default 8 MiB), **setelah semua pesan dalam segmen di-ack dan segmen ditutup, seluruh segmen dihapus**, indeks ikut dikompresi dan ditulis ulang.
- Peningkatan tidak mengubah perilaku ini; file segmen tunggal yang ditinggalkan instans lama tetap berfungsi normal di bawah logika rotasi segmen yang baru.

---

## 5. Peningkatan biner (bare metal)

> **【Terverifikasi（2026-10-10, mesin lokal）】** Latihan nyata antar-versi mayor: biner lama yang dibangun dengan tag `v1.1.0` menulis exchange/antrean/binding durable + 5 pesan persisten + vhost/akun/izin/policy serta mengubah kata sandi saat runtime; setelah proses dihentikan, dijalankan dengan biner `1.1.05` saat ini + **`data_dir` yang sama + port yang sama** —— proses berjalan normal, objek dan jumlah pesan `/api/queues` konsisten, klien AMQP nyata mengambil kembali seluruh 5 pesan persisten, dan `/metrics` melaporkan `speedmq_build_info{version="1.1.05"}`. Langkah-langkah di bawah tetap merupakan alur umum, dapat diterapkan pada versi berdekatan mana pun; **penurunan** lihat §8.

### 5.1 Langkah-langkah

```powershell
$base = "C:\speedmq"
$data = "$base\data"

# 1) Hentikan proses (keluar graceful akan melakukan flush penutup; lihat §4 "konsistensi")
#    Jika dijalankan sebagai foreground: Ctrl+C; jika sebagai layanan: Stop-Service / Stop-Process
Stop-Process -Name speedmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) Cadangkan direktori data (harus setelah proses berhenti)
Copy-Item -Recurse -Force $data "$base\backup-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) Ganti biner (letakkan speedmqd.exe / speedmqctl.exe versi baru di path semula)
#    Copy-Item .\new\speedmqd.exe $base\speedmqd.exe -Force

# 4) Jalankan
& "$base\speedmqd.exe" -config "$base\configs\speedmqd.json" -log-level info

# 5) Verifikasi: proses hidup + API manajemen dapat dibaca
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

### 5.2 Daftar periksa setelah peningkatan

- Log startup memunculkan `SpeedMQ 启动中 ... version=<新版本>` dan `管理面已启动`;
- `object_totals` / `queue_totals` di `/api/overview` sama dengan sebelum pencadangan (bandingkan `backup-restore.md` §5);
- `messages` / `messages_ready` setiap antrean durable di `/api/queues` sama dengan sebelum pencadangan;
- `/metrics` dapat di-scrape dan `speedmq_plugin_up{name="amqp091"} 1`, `{name="mqtt"} 1`.

---

## 6. Peningkatan image (kontainer)

Image berukuran sekitar 13 MB (biner statis + alpine), **berjalan sebagai non-root (uid 10001)**, direktori data dipasang di `/var/lib/speedmq`.

```powershell
# 1) Tarik/build image baru (gunakan nomor versi baru sebagai tag, hindari kebingungan old/new)
docker build -t speedmq:1.0.0 .

# 2) Hentikan kontainer lama (compose akan mempertahankan named volume speedmq-data)
docker compose down

# 3) Jalankan versi baru (ubah image di file compose ke tag baru)
docker compose up -d

# 4) Status dan log
docker compose ps
docker compose logs -f --tail 100
```

> **Tugas sekali jalan di dalam kontainer** (misalnya menjalankan `speedmqctl` di dalam kontainer): `run` pada `docker compose ...` di lingkungan non-interaktif harus menambahkan `-T`,
> jika tidak akan gagal karena meminta TTY:
> ```powershell
> docker compose run -T --rm broker speedmqctl -user guest -pass guest status
> ```

Persistensi data bergantung pada **named volume** `speedmq-data` di compose, data tidak hilang saat kontainer dibuat ulang (sejak M4 benar-benar ditulis ke disk).
Jika perlu mencadangkan isi volume sebelum peningkatan, setara dengan mencadangkan `/var/lib/speedmq` (lihat `backup-restore.md` §3.2).

> **【Terverifikasi（2026-10-10, Docker Desktop lokal）】** Telah dilakukan latihan peningkatan image yang nyata: image lama `speedmq:1.1.0-old` (git tag `v1.1.0`) dan image baru `speedmq:1.1.05-new` (kode sumber saat ini) dibangun secara lokal, image lama dijalankan dengan compose single-node → membuat antrean durable `persist.q` dan mempublikasikan 5 pesan persisten (`delivery_mode=2`) → `docker compose down` (**tanpa `-v`**, volume bernama `speedmq-data` tetap dipertahankan) → setelah tag image di compose diganti ke image baru, `docker compose up -d`.
> Hasil: kontainer baru berjalan normal, log startup memunculkan `已从磁盘恢复队列消息 ... queue=persist.q messages=5`, `/metrics` melaporkan `speedmq_build_info{version="1.1.05"}`, `queue_totals.messages=5` di `/api/overview` dan `messages=5` pada antrean `persist.q` konsisten sebelum dan sesudah peningkatan, dan klien AMQP nyata (`rabbitmq/amqp091-go`) mengambil kembali seluruh 5 pesan persisten satu per satu (`deliveryMode=2`).
> Catatan: `v1.1.0` adalah image merek `speedmq` sebelum pergantian nama (biner `speedmqd`, metrik `speedmq_build_info`, direktori data default `/var/lib/speedmq`), berbeda penamaan dari versi saat ini; dalam latihan, konfigurasi `data_dir=/var/lib/speedmq` yang menjadi acuan, dan pemilik untuk volume bernama disiapkan lebih dulu (image lama tidak memiliki path yang dapat ditulis tersebut). Ini tidak memengaruhi kesimpulan "membangun ulang kontainer dengan mengganti tag tidak menghilangkan data volume bernama".

---

## 7. Canary dan rollback

### 7.1 Mandiri

- **Canary**: SpeedMQ mandiri tidak memiliki kemampuan bawaan "dua versi baru/lama dalam satu proses". Canary yang layak adalah **bayangan di jalur samping**:
  instans versi baru terlebih dahulu dipasang pada aliran upstream yang sama dengan **konsumsi baca-saja/antrean bayangan** untuk diamati, setelah dipastikan benar baru alihkan sisi penulis.
- **Rollback**:
  1. Hentikan proses versi baru;
  2. Kembalikan ke biner lama;
  3. Jika versi baru sudah menulis data, **wajib memulihkan `data_dir` menggunakan cadangan sebelum peningkatan** (lihat di bawah).
  **Tidak ada** jaminan "versi baru sudah menulis, versi lama langsung membaca" — penurunan antar-versi lihat §8.

### 7.2 Klaster (peningkatan bergulir)

Sisi platform tidak menyediakan "peningkatan bergulir sekali klik", perlu dioperasikan manual per node dalam urutan berikut:

1. **Hanya tingkatkan satu node dalam satu waktu**: hentikan node tersebut → cadangkan `data_dir`-nya → ganti biner → jalankan → tunggu sampai ia bergabung kembali dan mengejar
   (`speedmqctl cluster_status` / `GET /api/cluster` lihat `role`, `commit_index`/`last_applied`).
2. **Saran urutan**: tingkatkan dulu **learner / anggota non-voting** (tidak berpengaruh pada mayoritas), lalu **follower**, terakhir **leader**
   (peningkatan leader akan memicu pemilihan pemimpin, selama itu ada waktu singkat tidak dapat menulis).
3. **Dampak penghentian terhadap mayoritas** (kunci):
   - Klaster 3 node: **paling banyak menghentikan 1** anggota voting sekaligus, menghentikan 2 berarti kehilangan mayoritas, di bawah `pause_minority` **seluruh klaster menghentikan layanan**.
   - Klaster 2 node: menghentikan 1 saja sudah kehilangan mayoritas, **tidak memiliki kemampuan peningkatan bergulir** (disarankan minimal 3 node).
   - Karena itu saat peningkatan bergulir **dilarang keras menghentikan beberapa anggota voting sekaligus**.
4. **Jangan lakukan perubahan anggota bersamaan dengan peningkatan**: perubahan anggota **tanpa joint consensus**, hanya boleh ada satu perubahan konfigurasi yang belum di-commit dalam satu waktu;
   selama peningkatan hindari `add_member` / `remove_member` secara bersamaan.
5. Setelah peningkatan selesai, periksa bahwa `object_totals` di `GET /api/cluster` sama dengan sebelum peningkatan.

> **【Terverifikasi（2026-10-10, klaster 3 node Docker Desktop lokal）】** Telah dilakukan latihan peningkatan bergulir **antar-versi** pada klaster nyata: baseline-nya `speedmq:1.1.0-old` yang dibangun lokal (git tag `v1.1.0`), naik satu per satu node (n1 → n2 → n3, satu per satu: hentikan, cadangkan volume bernamanya, ganti image lalu jalankan) ke `1.1.05` saat ini (dibangun lokal `speedmq:1.1.05-new`, compose mengganti image dengan override per node). Sepanjang proses, klaster selalu mempertahankan leader dan mayoritas —— saat leader saat itu dihentikan, leader baru terpilih dalam sekitar 0,6 s, `pause_minority` tidak memicu penundaan, dan tidak ada gangguan eksternal selama penghentian; setelah peningkatan, `object_totals` `GET /api/cluster` konsisten dengan sebelumnya (`queues:1`), jumlah 25 pesan persisten pada antrean quorum 3 replika `qroll` tidak berubah dan dapat dibaca kembali satu per satu, dan setelah mempublikasikan 5 pesan baru, seluruh 30 dapat diambil kembali.
> Urutan di atas tetap berlaku berdasarkan batasan umum Raft; latihan ini mencakup skenario 3 node. **【Terverifikasi（2026-10-10, Docker Desktop lokal）2 node: menghentikan 1 sudah kehilangan mayoritas】** Selain itu dibuat klaster minimal 2 node (n1/n2, `peers` masing-masing hanya memuat kedua node ini, `partition_policy=pause_minority`) dan diuji: saat kedua node online, `GET /api/cluster` bernilai `has_quorum=true / paused=false` (n1 leader, n2 follower); setelah `docker stop` pada n2, n1 menjadi `has_quorum=false / paused=true` dalam sekitar 2 s —— artinya menghentikan 1 anggota voting sudah kehilangan mayoritas dan `pause_minority` menangguhkan layanan, **menegaskan bahwa 2 node tidak memiliki kemampuan peningkatan bergulir**; setelah `docker start` memulihkan n2, n1 kembali ke `has_quorum=true / paused=false` dalam sekitar 2 s (layanan pulih otomatis).

> **【Terverifikasi（2026-10-11, klaster 3 node Docker Desktop lokal）】Peningkatan bergulir di bawah partisi jaringan telah diuji** (`v1.1.0` → `1.1.05` antar-versi; partisi menggunakan topologi jaringan ganda: Raft RPC antar-node hanya melalui `cluster-net` dengan IP statis, lalu lintas klien/manajemen melalui `client-net`, dan `docker network disconnect/connect` mengubah partisi):
> - **Follower terpartisi**: sisi mayoritas terus mempertahankan quorum selama partisi, mempublikasikan/mengonsumsi secara normal, dan menyelesaikan "hentikan → ganti image → jalankan → kejar" satu node mayoritas sementara partisi tetap berlangsung; klaster versi campuran (1.1.0 + 1.1.05) tetap melayani. Setelah partisi pulih, node yang terisolasi (versi lama) bergabung kembali dan mengejar; rekonsiliasi pesan berdasarkan himpunan ID unik 35/35 tanpa kehilangan.
> - **Leader terpartisi**: mayoritas memilih leader baru dalam sekitar **630 ms**; leader lama yang terisolasi mempertahankan `role=leader` tetapi dengan `has_quorum=false / paused=true` (`pause_minority` berlaku, tidak secara aktif menaikkan term); peningkatan node lain di sisi mayoritas selama partisi berhasil; setelah pulih, leader lama bergabung kembali, tanpa split-brain dan tanpa divergensi data.
> - **Perilaku yang diharapkan tanpa PreVote (dicatat apa adanya)**: follower yang terisolasi segera berkampanye berulang kali, term-nya naik sekitar +2 setiap 2 s (dua grup Raft metadata dan antrean bergantian naik); pada saat pemulihan **merebut kepemimpinan** dengan term yang lebih tinggi (terpilih dalam 1,4 s dalam latihan) —— ada gangguan pergantian leader tetapi tidak ada masalah kebenaran.
> - **Dua jebakan operasional**: ① jika `peers` menggunakan hostname dan kontainer juga terhubung ke jaringan kedua yang dapat dijangkau, lalu lintas Raft akan memutar dan menyebabkan "partisi semu" (node yang terisolasi masih menampilkan `has_quorum=true` setelah lebih dari satu menit) —— RPC antar-node harus melalui tepat satu jaringan (dengan `peers` menunjuk langsung ke IP statis jaringan tersebut); ② saat pemulihan, `docker network connect` harus membawa `--ip <IP asli>`, jika tidak node mendapat IP baru sementara mayoritas tetap memanggil alamat lama, dan node yang terisolasi —— membawa log yang tertinggal —— berkampanye berulang kali dengan term yang menggelembung (dalam latihan menyeret term klaster dari 320 ke 488 dan commit metadata dari 13 ke 97), dan keadaan sesaat dapat muncul di mana leader menampilkan `quorum=true` namun sebentar `paused=true`.
> - Selama partisi, API manajemen sisi minoritas tetap dapat dibaca (berguna untuk pengamatan), sementara publikasi AMQP ditolak (`pause_minority` berlaku).

---

## 8. Bagian yang tidak didukung / belum diverifikasi (dicantumkan secara eksplisit)

- **Penurunan antar-versi mayor: tidak didukung, belum diverifikasi**. Jika versi baru sudah menulis data dengan format/semantik baru, **tidak ada** jaminan "kembali ke biner lama dan membaca apa adanya";
  rollback hanya bisa mengandalkan cadangan sebelum peningkatan.
- **Format konfigurasi tidak berubah**: tetap JSON + variabel lingkungan `SPEEDMQ_*`. **Konfigurasi YAML belum didukung** (perlu menambahkan dependensi parser, M8-17 menunggu evaluasi),
  peningkatan tidak akan menghadirkan YAML.
- **Hot upgrade plugin/protokol online**: plugin dikompilasi menyatu dengan kernel (bentuk A) atau dinaikkan sesuai konfigurasi `spawn` (bentuk B),
  peningkatan kernel = restart proses; **tidak ada** mekanisme penggantian biner secara hot di tempat.
- **Migrasi mesin penyimpanan di tempat**: rotasi segmen/kompresi indeks adalah perilaku latar belakang saat runtime, **tidak ada** perintah "migrasi/kompresi data" yang berdiri sendiri.
- **Peningkatan klaster dengan disk penuh**: **belum diverifikasi**. (Peningkatan bergulir di bawah partisi jaringan telah diuji pada 2026-10-11; lihat catatan di akhir §7.)
- Dokumen ini **tidak memuat** verifikasi pemindahan data apa pun antara SpeedMQ dan broker lain (RabbitMQ).
