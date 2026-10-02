# Pelan Naik Taraf dan Migrasi SwiftMQ

> Versi berkenaan: `1.0.0`（`broker.Version`，lihat `swiftmq_build_info` dalam `/metrics`）。
> Semua kesimpulan "ujian sebenar" dalam dokumen ini berasal daripada operasi sebenar pada mesin ini; yang tidak diuji akan ditandakan secara eksplisit **【Belum diverifikasi】**。
> Persekitaran mesin ini: Windows + PowerShell 5.1，Go 1.27.1 windows/386，`data_dir` sementara + port bukan lalai.

---

## 1. Migrasi (beralih daripada RabbitMQ kepada SwiftMQ)

Kedudukan projek ini ialah **keserasian pada peringkat protokol AMQP 0-9-1**, oleh itu "migrasi" terutamanya melibatkan **menukar alamat sambungan**:

- Kod perniagaan sifar perubahan, hanya tukar `host/port/vhost`（dokumen reka bentuk G3 "migrasi sifar kos"）。
- Rantaian alat pengurusan（`rabbitmqadmin`、UI pengurusan、skrip pemantauan）hanya perlu menunjuk ke port satah pengurusan; bentuk antara muka diselaraskan dengan RabbitMQ（konvensyen seperti `amq.default`、`%2F`、`{error, reason}` disalin seadanya）。
- Port lalai sama seperti RabbitMQ: AMQP `5672`、satah pengurusan `15672`; MQTT `1883`，RPC antara nod `25672`.

**Perbezaan semantik yang perlu disemak sendiri sebelum migrasi**（semuanya sengaja dilakukan oleh repositori ini, berdasarkan README / dokumen reka bentuk）:

| Item | Tingkah laku SwiftMQ | Kesan migrasi |
| --- | --- | --- |
| Baris gilir sementara (bukan gigih dan bukan eksklusif) | **Menolak pengisytiharan**（541），`auto_delete` tidak dikecualikan | Klien lama yang bergantung pada baris gilir jenis ini akan gagal, perlu tukar kepada durable atau exclusive |
| vhost lalai `/` | **Tidak boleh dipadam**（400），RabbitMQ membenarkan | Skrip automasi yang memadam vhost lalai akan gagal（ini satu-satunya kekangan keselamatan proaktif） |
| Data baris gilir klasik | **Tidak direplikasi**，data hanya pada nod Owner | Jika redundansi merentas nod diperlukan, tukar kepada baris gilir kuorum `x-queue-type=quorum` |
| Baris gilir kuorum | Menyokong penambahan replika，**tidak menyokong pengurangan** | Rancang sekali gus |
| Pemalam | Tiada ekosistem pemalam Erlang，AMQP 1.0 / STOMP belum dilaksanakan | Senario yang menggunakan protokol ini belum boleh dimigrasi |

**Migrasi data**: format penyimpanan SwiftMQ dan RabbitMQ tidak serasi，**tiada alat pemindahan data dalam talian/luar talian disediakan**。
Cara migrasi ialah "bina SwiftMQ kosong → jalankan serentak untuk pengesahan → potong aliran secara berperingkat"。**【Belum diverifikasi】** Dokumen ini tidak mengandungi sebarang latihan pemindahan data RabbitMQ sebenar.

---

## 2. Prinsip umum naik taraf

1. **Sandarkan dahulu**（lihat `backup-restore.md`）—— jaring keselamatan jika naik taraf gagal.
2. **Hentikan proses dahulu kemudian ganti**（direktori data mempunyai kekangan penulis tunggal, lihat §4.2）。
3. **Mesti sahkan selepas naik taraf**: proses boleh mula、`/api/overview` boleh dibaca、`/metrics` boleh diambil、bilangan mesej baris gilir sama seperti sebelum sandaran.
4. Naik taraf kluster **bergilir nod demi nod**, hanya sentuh satu nod pada satu masa（lihat §5）。

---

## 3. Susun atur direktori data (asas fakta untuk naik taraf/migrasi)

Susun atur `data_dir` yang diperoleh daripada **ujian sebenar** instans mesin tunggal pada mesin ini:

```
data/
├── meta/
│   ├── state.json        # syot kilat metadata mod mesin tunggal (vhost/penukar/baris gilir/pengikatan/pengguna/kebenaran/polisi)
│   ├── users.seeded      # penanda but: users dalam fail konfigurasi telah disemai
│   ├── vhosts.seeded     # penanda but: vhosts dalam fail konfigurasi telah disemai
│   ├── raft.state        # 【mod kluster】Penggal/undian Raft
│   ├── raft.log          # 【mod kluster】Log Raft
│   └── snapshot.json     # 【mod kluster】Syot kilat Raft + jadual ahli
├── msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/
│   ├── 000001.seg        # fail segmen (badan mesej + atribut), format rekod: <len u32><crc32 u32><payload>
│   └── index/000001.idx  # indeks baris gilir: seq-id → (nombor segmen, offset dalam segmen, panjang, status)
└── quorum/<safe(vhost)>/<safe(queue)>/   # 【kluster】baris gilir kuorum, satu kumpulan Raft setiap baris gilir (log/syot kilat)
```

**Perhatian（dua perkara yang berbeza daripada gerak hati, semuanya berdasarkan kod/ujian sebenar）**:

- Dalam mod kluster, fail kegigihan Raft **diletakkan terus di bawah `meta/`**（`raft.state` / `raft.log` / `snapshot.json`），
  **tiada subdirektori `meta/raft/`**。Asas: pemalar nama fail `internal/raft/log.go` + `Dir: filepath.Join(b.cfg.DataDir, "meta")`
  dalam `internal/broker/cluster.go`。**【Susun atur kluster belum diuji】**（mesin ini hanya menjalankan instans mesin tunggal）。
- Nama direktori **bukan nama vhost / baris gilir asal**, tetapi pengekodan `store.SafeDirName`: menambah awalan `q_`，bait yang bukan `[A-Za-z0-9._-]` dieskap sebagai `%XX`。
  Ujian sebenar: vhost `/` → direktori `q_%2F`，baris gilir `persist.q` → direktori `q_persist.q`。
  Reka bentuk ini untuk mengelakkan lintasan laluan dan nama peranti terpelihara Windows（`con`/`nul` dsb.）。

---

## 4. Keserasian data

### 4.1 Data lama boleh dibaca terus —— boleh

- **Format indeks serasi ke hadapan**: M8-1 menambahkan medan "nombor segmen" dalam rekod indeks（25 bait）; **format lama（21 bait, tanpa nombor segmen）masih boleh dibaca seadanya**，
  semasa bacaan ia bersamaan dengan "hanya satu segmen（seg=1）"，**naik taraf tidak memerlukan skrip migrasi**。
  Asas: pemalar `indexEntrySize` / `legacyIndexEntrySize` dan logik `recover()` dalam `internal/store/store.go`; README M8-1。
- **Semantik ranap tidak berubah**: setiap rekod membawa awalan panjang + CRC32, semasa pemulihan **membuang rekod separuh tulis/rosak di hujung** dan memotong.
  Ujian sebenar（lihat `backup-restore.md` §6）: selepas proses berhenti dan mula semula, 5 mesej gigih baris gilir durable **semuanya dipulihkan**，
  log memaparkan `已从磁盘恢复队列消息 ... messages=5`。

### 4.2 Skop `vhosts` / `users` dalam konfigurasi (perangkap paling mudah tersandung semasa naik taraf)

- Kedua-duanya **hanya berkesan semasa but pertama kalinya**: permulaan pertama akan menulis vhosts/users dalam konfigurasi ke metadata dan meninggalkan fail penanda
  `meta/vhosts.seeded` / `meta/users.seeded`; **selepas itu metadata dijadikan rujukan**。
- Oleh itu **semasa naik taraf/menukar konfigurasi, jangan harap dapat menambah/memadam akaun atau vhost dengan mengubah fail konfigurasi**—— ubah pun tidak berkesan;
  sila gunakan API pengurusan atau `swiftmqctl`。
- Sebaliknya, naik taraf **tidak akan** menimpa akaun sedia ada dengan konfigurasi: kata laluan yang diubah semasa operasi tidak akan dikembalikan kepada nilai lama dalam konfigurasi selepas mula semula,
  akaun yang dipadam semasa operasi juga tidak akan hidup semula。Asas: logik penanda semaian `cluster.go`; README M8-4 / M8-7。

### 4.3 Putaran segmen dan pengitaran semula cakera

- Mesej dibahagikan mengikut saiz（lalai 8 MiB），**selepas semua mesej dalam segmen diack dan segmen ditutup, seluruh segmen dipadam**, indeks dipadatkan dan ditulis semula bersamanya。
- Naik taraf tidak mengubah tingkah laku ini; fail segmen tunggal yang ditinggalkan oleh instans lama berfungsi seperti biasa di bawah logik putaran segmen baharu。

---

## 5. Naik taraf binari (mesin kosong)

> Mesin ini **belum melakukan latihan sebenar merentas versi**（repositori pada masa ini hanya mempunyai satu versi `1.0.0`，tiada binari lama untuk dinaik taraf）。Langkah di bawah ialah **pengesahan main semula versi sama + aliran umum** bagi keupayaan yang sedia ada dalam repositori ini, bahagian merentas versi ditandakan **【Belum diverifikasi】**。

### 5.1 Langkah

```powershell
$base = "C:\swiftmq"
$data = "$base\data"

# 1) Hentikan proses (keluar dengan sopan akan melakukan flush akhir; lihat §4 "konsistensi")
#    Jika berjalan di latar depan: Ctrl+C; jika sebagai perkhidmatan: Stop-Service / Stop-Process
Stop-Process -Name swiftmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) Sandarkan direktori data (pastikan selepas proses berhenti)
Copy-Item -Recurse -Force $data "$base\backup-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) Gantikan binari (letakkan swiftmqd.exe / swiftmqctl.exe versi baharu ke laluan asal)
#    Copy-Item .\new\swiftmqd.exe $base\swiftmqd.exe -Force

# 4) Mula
& "$base\swiftmqd.exe" -config "$base\configs\swiftmqd.json" -log-level info

# 5) Sahkan: proses masih hidup + API pengurusan boleh dibaca
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

### 5.2 Senarai semak pengesahan selepas naik taraf

- Log permulaan memaparkan `SwiftMQ 启动中 ... version=<新版本>` dan `管理面已启动`;
- `object_totals` / `queue_totals` dalam `/api/overview` sama seperti sebelum sandaran（banding `backup-restore.md` §5）;
- `messages` / `messages_ready` setiap baris gilir durable dalam `/api/queues` sama seperti sebelum sandaran;
- `/metrics` boleh diambil dan `swiftmq_plugin_up{name="amqp091"} 1`、`{name="mqtt"} 1`。

---

## 6. Naik taraf imej (kontena)

Imej berukuran kira-kira 13 MB（binari pautan statik + alpine），**berjalan sebagai bukan root（uid 10001）**，direktori data dipasang pada `/var/lib/swiftmq`。

```powershell
# 1) Tarik/bina imej baharu (tag guna nombor versi baharu, elakkan kekeliruan old/new)
docker build -t swiftmq:1.0.0 .

# 2) Hentikan kontena lama (compose akan mengekalkan volum bernama swiftmq-data)
docker compose down

# 3) Mulakan versi baharu (tukar image dalam fail compose kepada tag baharu)
docker compose up -d

# 4) Status dan log
docker compose ps
docker compose logs -f --tail 100
```

> **Tugasan sekali guna dalam kontena**（contohnya menjalankan `swiftmqctl` dalam kontena）: `run` bagi `docker compose ...` mesti menambah `-T` dalam persekitaran bukan interaktif,
> jika tidak akan gagal kerana permohonan TTY:
> ```powershell
> docker compose run -T --rm broker swiftmqctl -user guest -pass guest status
> ```

Kegigihan data bergantung pada **volum bernama** `swiftmq-data` compose, data tidak hilang apabila kontena dibina semula（sejak M4 benar-benar ditulis ke cakera）。
Jika perlu menyandarkan kandungan volum sebelum naik taraf, ia bersamaan dengan menyandarkan `/var/lib/swiftmq`（lihat `backup-restore.md` §3.2）。**【Naik taraf imej belum diuji】**（mesin ini tidak menjalankan Docker）。

---

## 7. Pengeluaran berperingkat dan rollback

### 7.1 Mesin tunggal

- **Pengeluaran berperingkat**: SwiftMQ mesin tunggal tiada keupayaan terbina dalam "dua versi lama/baharu dalam proses sama"。Pengeluaran berperingkat yang boleh dilakukan ialah **bayangan sisi**:
  instans versi baharu mula-mula dipasang pada aliran huluan yang sama menggunakan **penggunaan baca sahaja/baris gilir bayangan** untuk pemerhatian, selepas disahkan betul barulah tukar pihak penulis。
- **Rollback**:
  1. Hentikan proses versi baharu;
  2. Tukar kembali kepada binari lama;
  3. Jika versi baharu pernah menulis data, **mesti memulihkan `data_dir` menggunakan sandaran sebelum naik taraf**（lihat di bawah）。
  **Tidak akan** ada jaminan "versi baharu sudah menulis, versi lama boleh baca terus"—— penurunan merentas versi lihat §8。

### 7.2 Kluster (naik taraf bergilir)

Platform tidak menyediakan "naik taraf bergilir satu klik", perlu mengendalikan nod satu demi satu secara manual mengikut urutan di bawah:

1. **Naik taraf satu nod sahaja pada satu masa**: hentikan nod tersebut → sandarkan `data_dir`nya → tukar binari → mula → tunggu ia menyertai semula dan menyusul
   （`swiftmqctl cluster_status` / `GET /api/cluster` lihat `role`、`commit_index`/`last_applied`）。
2. **Cadangan urutan**: naik taraf **learner / ahli bukan pengundi** dahulu（tiada kesan pada majoriti），kemudian **follower**，akhir sekali **leader**
   （naik taraf leader akan mencetuskan satu pemilihan ketua, semasa itu tidak boleh menulis untuk seketika）。
3. **Kesan pemberhentian terhadap majoriti**（penting）:
   - Kluster 3 nod: **paling banyak hentikan 1** ahli pengundi serentak, hentikan 2 bermakna kehilangan majoriti, di bawah `pause_minority` **seluruh kluster menghentikan perkhidmatan**。
   - Kluster 2 nod: hentikan 1 sahaja sudah kehilangan majoriti，**tidak mempunyai keupayaan naik taraf bergilir**（disyorkan sekurang-kurangnya 3 nod）。
   - Oleh itu semasa naik taraf bergilir **dilarang sama sekali menghentikan berbilang ahli pengundi serentak**。
4. **Perubahan ahli dan naik taraf jangan dilakukan serentak**: perubahan ahli **tiada joint consensus**，hanya satu perubahan konfigurasi belum komit dibenarkan pada satu masa;
   semasa naik taraf sila elakkan `add_member` / `remove_member` serentak。
5. Selepas naik taraf selesai, semak `object_totals` `GET /api/cluster` sama seperti sebelum naik taraf。

> **【Belum diverifikasi】** Mesin ini belum melakukan latihan naik taraf bergilir kluster sebenar（laluan kluster dan kontena kedua-duanya tidak dijalankan）; urutan di atas berasal daripada kekangan umum
> perisian tengah mesej dan Raft serta fakta pelaksanaan `pause_minority` / perubahan ahli dalam repositori ini, bukan kesimpulan ujian sebenar mesin ini。

---

## 8. Bahagian yang tidak disokong / belum diverifikasi (disenaraikan secara jelas)

- **Penurunan merentas versi utama: tidak disokong, belum diverifikasi**。Jika versi baharu sudah menulis data dengan format/semantik baharu，**tiada** jaminan "kembali kepada binari lama dan baca seadanya";
  rollback hanya boleh bergantung pada sandaran sebelum naik taraf。
- **Format konfigurasi tidak berubah**: masih JSON + pembolehubah persekitaran `SWIFTMQ_*`。**Konfigurasi YAML belum disokong**（perlu memperkenalkan kebergantungan penghurai, M8-17 menunggu penilaian），
  naik taraf tidak akan membawa YAML。
- **Naik taraf panas pemalam/protokol dalam talian**: pemalam dikompil bersama kernel（bentuk A）atau dilancarkan mengikut konfigurasi `spawn`（bentuk B），
  naik taraf kernel = mula semula proses; **tiada** mekanisme menggantikan binari panas di tempat。
- **Migrasi di tempat enjin penyimpanan**: putaran segmen/pemadatan indeks ialah tingkah laku latar belakang masa operasi，**tiada** arahan "migrasi data/pemadatan" berasingan。
- **Naik taraf kluster dalam rangkaian sebenar**: repositori ini hanya melakukan kekacauan berskala kecil（kill pada peringkat proses），**tidak melakukan** latihan naik taraf di bawah pemisahan rangkaian dan cakera penuh。
- Dokumen ini **tidak mengandungi** sebarang pengesahan pemindahan data antara SwiftMQ dan broker lain（RabbitMQ）。
