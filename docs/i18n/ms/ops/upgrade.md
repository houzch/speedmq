# Pelan Naik Taraf dan Migrasi SpeedMQ

> Versi berkenaan: `1.0.0`（`broker.Version`，lihat `speedmq_build_info` dalam `/metrics`）。
> Semua kesimpulan "ujian sebenar" dalam dokumen ini berasal daripada operasi sebenar pada mesin ini; yang tidak diuji akan ditandakan secara eksplisit **【Belum diverifikasi】**。
> Persekitaran mesin ini: Windows + PowerShell 5.1，Go 1.27.1 windows/386，`data_dir` sementara + port bukan lalai.

---

## 1. Migrasi (beralih daripada RabbitMQ kepada SpeedMQ)

Kedudukan projek ini ialah **keserasian pada peringkat protokol AMQP 0-9-1**, oleh itu "migrasi" terutamanya melibatkan **menukar alamat sambungan**:

- Kod perniagaan sifar perubahan, hanya tukar `host/port/vhost`（dokumen reka bentuk G3 "migrasi sifar kos"）。
- Rantaian alat pengurusan（`rabbitmqadmin`、UI pengurusan、skrip pemantauan）hanya perlu menunjuk ke port satah pengurusan; bentuk antara muka diselaraskan dengan RabbitMQ（konvensyen seperti `amq.default`、`%2F`、`{error, reason}` disalin seadanya）。
- Port lalai sama seperti RabbitMQ: AMQP `5672`、satah pengurusan `15672`; MQTT `1883`，RPC antara nod `25672`.

**Perbezaan semantik yang perlu disemak sendiri sebelum migrasi**（semuanya sengaja dilakukan oleh repositori ini, berdasarkan README / dokumen reka bentuk）:

| Item | Tingkah laku SpeedMQ | Kesan migrasi |
| --- | --- | --- |
| Baris gilir sementara (bukan gigih dan bukan eksklusif) | **Menolak pengisytiharan**（541），`auto_delete` tidak dikecualikan | Klien lama yang bergantung pada baris gilir jenis ini akan gagal, perlu tukar kepada durable atau exclusive |
| vhost lalai `/` | **Tidak boleh dipadam**（400），RabbitMQ membenarkan | Skrip automasi yang memadam vhost lalai akan gagal（ini satu-satunya kekangan keselamatan proaktif） |
| Data baris gilir klasik | **Tidak direplikasi**，data hanya pada nod Owner | Jika redundansi merentas nod diperlukan, tukar kepada baris gilir kuorum `x-queue-type=quorum` |
| Baris gilir kuorum | Menyokong penambahan replika，**tidak menyokong pengurangan** | Rancang sekali gus |
| Pemalam | Tiada ekosistem pemalam Erlang，AMQP 1.0 / STOMP belum dilaksanakan | Senario yang menggunakan protokol ini belum boleh dimigrasi |

**Migrasi data**: format penyimpanan SpeedMQ dan RabbitMQ tidak serasi，**tiada alat pemindahan data dalam talian/luar talian disediakan**。
Cara migrasi ialah "bina SpeedMQ kosong → jalankan serentak untuk pengesahan → potong aliran secara berperingkat"。**【Belum diverifikasi】** Dokumen ini tidak mengandungi sebarang latihan pemindahan data RabbitMQ sebenar.

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
  dalam `internal/broker/cluster.go`。**【Disahkan（2026-10-10, Docker Desktop mesin ini, kluster 3 nod）】** —— kandungan sandaran volum nod kluster boleh dilihat terus mengandungi `meta/raft.log`、`meta/raft.state`、`meta/*.seeded`（tiada subdirektori `meta/raft/` dilihat）。
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

**【Disahkan（2026-10-10, mesin ini）】** Bacaan merentas versi utama telah diuji: data baris gilir durable yang ditulis dengan tag `v1.1.0` dipulihkan seadanya di bawah `1.1.05`（arahan dan output lihat §5）。

### 4.2 Skop `vhosts` / `users` dalam konfigurasi (perangkap paling mudah tersandung semasa naik taraf)

- Kedua-duanya **hanya berkesan semasa but pertama kalinya**: permulaan pertama akan menulis vhosts/users dalam konfigurasi ke metadata dan meninggalkan fail penanda
  `meta/vhosts.seeded` / `meta/users.seeded`; **selepas itu metadata dijadikan rujukan**。
- Oleh itu **semasa naik taraf/menukar konfigurasi, jangan harap dapat menambah/memadam akaun atau vhost dengan mengubah fail konfigurasi**—— ubah pun tidak berkesan;
  sila gunakan API pengurusan atau `speedmqctl`。
- Sebaliknya, naik taraf **tidak akan** menimpa akaun sedia ada dengan konfigurasi: kata laluan yang diubah semasa operasi tidak akan dikembalikan kepada nilai lama dalam konfigurasi selepas mula semula,
  akaun yang dipadam semasa operasi juga tidak akan hidup semula。Asas: logik penanda semaian `cluster.go`; README M8-4 / M8-7。

**【Disahkan（2026-10-10, mesin ini）】** Perubahan masa operasi memang tidak ditimpa oleh konfigurasi: sebelum naik taraf, melalui API pengurusan, kata laluan `guest` diubah kepada `rotated`; selepas mula semula dengan `1.1.05` + `data_dir` yang sama, `guest/guest`（kata laluan lama dalam konfigurasi）memulangkan **401**、`guest/rotated` memulangkan **200**。

### 4.3 Putaran segmen dan pengitaran semula cakera

- Mesej dibahagikan mengikut saiz（lalai 8 MiB），**selepas semua mesej dalam segmen diack dan segmen ditutup, seluruh segmen dipadam**, indeks dipadatkan dan ditulis semula bersamanya。
- Naik taraf tidak mengubah tingkah laku ini; fail segmen tunggal yang ditinggalkan oleh instans lama berfungsi seperti biasa di bawah logik putaran segmen baharu。

---

## 5. Naik taraf binari (mesin kosong)

> **【Disahkan（2026-10-10, mesin ini）】** Latihan sebenar merentas versi utama: binari lama yang dibina daripada tag `v1.1.0` menulis penukar/baris gilir/pengikatan durable + 5 mesej gigih + vhost/akaun/kebenaran/polisi dan menukar kata laluan semasa operasi; selepas proses dihentikan, mula semula dengan binari `1.1.05` semasa + **`data_dir` yang sama + port yang sama** —— proses mula seperti biasa、objek dan bilangan mesej dalam `/api/queues` sama、klien AMQP sebenar mengambil semula kesemua 5 mesej gigih、`/metrics` melaporkan `speedmq_build_info{version="1.1.05"}`。
> Langkah di bawah masih merupakan aliran umum, boleh digunakan pada mana-mana versi bersebelahan; **penurunan** lihat §8。

### 5.1 Langkah

```powershell
$base = "C:\speedmq"
$data = "$base\data"

# 1) Hentikan proses (keluar dengan sopan akan melakukan flush akhir; lihat §4 "konsistensi")
#    Jika berjalan di latar depan: Ctrl+C; jika sebagai perkhidmatan: Stop-Service / Stop-Process
Stop-Process -Name speedmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) Sandarkan direktori data (pastikan selepas proses berhenti)
Copy-Item -Recurse -Force $data "$base\backup-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) Gantikan binari (letakkan speedmqd.exe / speedmqctl.exe versi baharu ke laluan asal)
#    Copy-Item .\new\speedmqd.exe $base\speedmqd.exe -Force

# 4) Mula
& "$base\speedmqd.exe" -config "$base\configs\speedmqd.json" -log-level info

# 5) Sahkan: proses masih hidup + API pengurusan boleh dibaca
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

### 5.2 Senarai semak pengesahan selepas naik taraf

- Log permulaan memaparkan `SpeedMQ 启动中 ... version=<新版本>` dan `管理面已启动`;
- `object_totals` / `queue_totals` dalam `/api/overview` sama seperti sebelum sandaran（banding `backup-restore.md` §5）;
- `messages` / `messages_ready` setiap baris gilir durable dalam `/api/queues` sama seperti sebelum sandaran;
- `/metrics` boleh diambil dan `speedmq_plugin_up{name="amqp091"} 1`、`{name="mqtt"} 1`。

---

## 6. Naik taraf imej (kontena)

Imej berukuran kira-kira 13 MB（binari pautan statik + alpine），**berjalan sebagai bukan root（uid 10001）**，direktori data dipasang pada `/var/lib/speedmq`。

```powershell
# 1) Tarik/bina imej baharu (tag guna nombor versi baharu, elakkan kekeliruan old/new)
docker build -t speedmq:1.0.0 .

# 2) Hentikan kontena lama (compose akan mengekalkan volum bernama speedmq-data)
docker compose down

# 3) Mulakan versi baharu (tukar image dalam fail compose kepada tag baharu)
docker compose up -d

# 4) Status dan log
docker compose ps
docker compose logs -f --tail 100
```

> **Tugasan sekali guna dalam kontena**（contohnya menjalankan `speedmqctl` dalam kontena）: `run` bagi `docker compose ...` mesti menambah `-T` dalam persekitaran bukan interaktif,
> jika tidak akan gagal kerana permohonan TTY:
> ```powershell
> docker compose run -T --rm broker speedmqctl -user guest -pass guest status
> ```

Kegigihan data bergantung pada **volum bernama** `speedmq-data` compose, data tidak hilang apabila kontena dibina semula（sejak M4 benar-benar ditulis ke cakera）。
Jika perlu menyandarkan kandungan volum sebelum naik taraf, ia bersamaan dengan menyandarkan `/var/lib/speedmq`（lihat `backup-restore.md` §3.2）。

> **【Disahkan（2026-10-10, Docker Desktop mesin ini）】** Telah melakukan latihan naik taraf imej sebenar: bina secara tempatan imej lama `speedmq:1.1.0-old`（git tag `v1.1.0`）dan imej baharu `speedmq:1.1.05-new`（sumber semasa）, mulakan imej lama dengan compose mesin tunggal → bina baris gilir durable `persist.q` dan terbitkan 5 mesej gigih（`delivery_mode=2`）→ `docker compose down`（**tanpa `-v`**, volum bernama `speedmq-data` dikekalkan）→ tukar tag imej dalam compose kepada imej baharu kemudian `docker compose up -d`。
> Keputusan: kontena baharu mula seperti biasa、log permulaan memaparkan `已从磁盘恢复队列消息 ... queue=persist.q messages=5`、`/metrics` melaporkan `speedmq_build_info{version="1.1.05"}`、`queue_totals.messages=5` dalam `/api/overview` dan `messages=5` baris gilir `persist.q` sama sebelum dan selepas naik taraf, dan klien AMQP sebenar（`rabbitmq/amqp091-go`）mengambil semula kesemua 5 mesej gigih satu demi satu（`deliveryMode=2`）。
> Nota: `v1.1.0` ialah imej jenama `speedmq` sebelum tukar nama（binari `speedmqd`、metrik `speedmq_build_info`、direktori data lalai `/var/lib/speedmq`）, berbeza penamaan dengan versi semasa; dalam latihan ini berpandukan konfigurasi `data_dir=/var/lib/speedmq` dan menyediakan pemilik untuk volum bernama terlebih dahulu（dalam imej lama tiada laluan boleh tulis ini）。Ini tidak menjejaskan kesimpulan「tukar tag bina semula kontena, data volum bernama tidak hilang」。

---

## 7. Pengeluaran berperingkat dan rollback

### 7.1 Mesin tunggal

- **Pengeluaran berperingkat**: SpeedMQ mesin tunggal tiada keupayaan terbina dalam "dua versi lama/baharu dalam proses sama"。Pengeluaran berperingkat yang boleh dilakukan ialah **bayangan sisi**:
  instans versi baharu mula-mula dipasang pada aliran huluan yang sama menggunakan **penggunaan baca sahaja/baris gilir bayangan** untuk pemerhatian, selepas disahkan betul barulah tukar pihak penulis。
- **Rollback**:
  1. Hentikan proses versi baharu;
  2. Tukar kembali kepada binari lama;
  3. Jika versi baharu pernah menulis data, **mesti memulihkan `data_dir` menggunakan sandaran sebelum naik taraf**（lihat di bawah）。
  **Tidak akan** ada jaminan "versi baharu sudah menulis, versi lama boleh baca terus"—— penurunan merentas versi lihat §8。

### 7.2 Kluster (naik taraf bergilir)

Platform tidak menyediakan "naik taraf bergilir satu klik", perlu mengendalikan nod satu demi satu secara manual mengikut urutan di bawah:

1. **Naik taraf satu nod sahaja pada satu masa**: hentikan nod tersebut → sandarkan `data_dir`nya → tukar binari → mula → tunggu ia menyertai semula dan menyusul
   （`speedmqctl cluster_status` / `GET /api/cluster` lihat `role`、`commit_index`/`last_applied`）。
2. **Cadangan urutan**: naik taraf **learner / ahli bukan pengundi** dahulu（tiada kesan pada majoriti），kemudian **follower**，akhir sekali **leader**
   （naik taraf leader akan mencetuskan satu pemilihan ketua, semasa itu tidak boleh menulis untuk seketika）。
3. **Kesan pemberhentian terhadap majoriti**（penting）:
   - Kluster 3 nod: **paling banyak hentikan 1** ahli pengundi serentak, hentikan 2 bermakna kehilangan majoriti, di bawah `pause_minority` **seluruh kluster menghentikan perkhidmatan**。
   - Kluster 2 nod: hentikan 1 sahaja sudah kehilangan majoriti，**tidak mempunyai keupayaan naik taraf bergilir**（disyorkan sekurang-kurangnya 3 nod）。
   - Oleh itu semasa naik taraf bergilir **dilarang sama sekali menghentikan berbilang ahli pengundi serentak**。
4. **Perubahan ahli dan naik taraf jangan dilakukan serentak**: perubahan ahli **tiada joint consensus**，hanya satu perubahan konfigurasi belum komit dibenarkan pada satu masa;
   semasa naik taraf sila elakkan `add_member` / `remove_member` serentak。
5. Selepas naik taraf selesai, semak `object_totals` `GET /api/cluster` sama seperti sebelum naik taraf。

> **【Disahkan（2026-10-10, Docker Desktop mesin ini, kluster 3 nod）】** Telah melakukan latihan naik taraf bergilir **merentas versi** kluster sebenar: garis dasar ialah `speedmq:1.1.0-old` yang dibina secara tempatan（git tag `v1.1.0`）, naik taraf nod demi nod（n1 → n2 → n3, satu demi satu: hentikan dahulu, kemudian sandarkan volum bernamanya, kemudian tukar imej dan mula）ke `1.1.05` semasa（dibina tempatan `speedmq:1.1.05-new`, compose menggunakan override nod demi nod untuk menukar imej）。Sepanjang masa kluster sentiasa mengekalkan leader dan majoriti —— apabila leader semasa dihentikan, leader baharu dipilih dalam kira-kira 0.6 s、`pause_minority` tidak mencetuskan jeda、tiada gangguan luar semasa pemberhentian; selepas naik taraf `object_totals` `GET /api/cluster` sama seperti sebelum naik taraf（`queues:1`）, 25 mesej gigih baris gilir kuorum 3 replika `qroll` kekal bilangan sama dan boleh dibaca semula satu demi satu, selepas menerbitkan 5 mesej baharu, kesemua 30 boleh diambil semula。
> Urutan di atas masih terpakai di bawah kekangan umum Raft, latihan ini merangkumi senario 3 nod. **【Disahkan（2026-10-10, Docker Desktop mesin ini）kluster 2 nod hentikan 1 sudah kehilangan majoriti】** Membina satu lagi kluster minimum 2 nod（n1/n2, setiap `peers` hanya mengandungi dua ini, `partition_policy=pause_minority`）uji sebenar: apabila kedua nod dalam talian, `GET /api/cluster` ialah `has_quorum=true / paused=false`（n1 leader、n2 follower）; selepas `docker stop` n2, n1 dalam kira-kira 2 s menjadi `has_quorum=false / paused=true` —— iaitu hentikan 1 ahli pengundi sudah kehilangan majoriti, `pause_minority` menjeda perkhidmatan, **membuktikan kluster 2 nod tidak mempunyai keupayaan naik taraf bergilir**; selepas `docker start` memulihkan n2, n1 kembali kepada `has_quorum=true / paused=false` dalam kira-kira 2 s（perkhidmatan pulih automatik）。

> **【Disahkan（2026-10-11, Docker Desktop mesin ini, kluster 3 nod）】Naik taraf bergilir di bawah pemisahan rangkaian telah diuji sebenar**（`v1.1.0` → `1.1.05` merentas versi, pemisahan menggunakan topologi dua rangkaian: RPC Raft antara nod hanya melalui `cluster-net` ber-IP statik, klien/satah pengurusan melalui `client-net`, `docker network disconnect/connect` untuk menukar pemisahan）:
> - **Follower yang dipisahkan**: pihak majoriti mengekalkan quorum sepanjang pemisahan、menterbit/menggunakan seperti biasa, dan dalam keadaan pemisahan berterusan menyelesaikan "henti→tukar imej→mula→menyusul" bagi satu nod majoriti; kluster versi campuran（1.1.0 + 1.1.05）boleh berkhidmat. Selepas pemisahan dipulihkan, nod yang diasingkan（versi lama）menyertai semula dan menyusul, mesej disemak mengikut set ID unik 35/35 tiada kehilangan.
> - **Leader yang dipisahkan**: majoriti memilih leader baharu dalam kira-kira **630 ms**; leader lama yang diasingkan mengekalkan `role=leader` tetapi `has_quorum=false / paused=true`（`pause_minority` berkuat kuasa, tidak menaikkan penggal secara aktif）; naik taraf nod lain di pihak majoriti semasa pemisahan berjaya; selepas pemulihan leader lama menyertai semula, tiada split-brain、tiada pencabangan data.
> - **Tingkah laku yang dijangka tanpa PreVote（direkodkan dengan jujur）**: follower yang diasingkan serta-merta bertanding berulang kali, penggal meningkat kira-kira +2 setiap 2 s（dua kumpulan Raft bagi metadata dan baris gilir menaik secara bergilir）; sebaik sahaja pemulihan, ia **merebut kepemimpinan** dengan penggal tinggi（diuji dipilih dalam 1.4 s）—— ada gangguan pertukaran leader tetapi tiada masalah ketepatan.
> - **Dua perangkap operasi**: ① jika `peers` menggunakan nama hos dan kontena turut dipasang pada rangkaian kedua yang boleh dicapai, trafik Raft akan membelok dan menyebabkan "pemisahan palsu"（nod yang diasingkan masih `has_quorum=true` selepas lebih seminit）—— RPC antara nod mesti hanya melalui satu rangkaian（peers menunjuk terus ke IP statik rangkaian tersebut）; ② semasa pemulihan, `docker network connect` mesti membawa `--ip <IP asal>`, jika tidak nod mendapat IP baharu manakala majoriti masih mendail alamat lama, nod yang diasingkan membawa log yang ketinggalan dan bertanding berulang kali dengan penggal yang melambung（ujian sebenar menyeret term kluster daripada 320 ke 488、komit metadata melonjak daripada 13 ke 97）, dan mungkin muncul keadaan fana di mana leader `quorum=true` tetapi seketika `paused=true`.
> - Semasa pemisahan, API pengurusan di pihak minoriti boleh dibaca（memudahkan pemerhatian）, penerbitan AMQP ditolak（`pause_minority` berkuat kuasa）.

---

## 8. Bahagian yang tidak disokong / belum diverifikasi (disenaraikan secara jelas)

- **Penurunan merentas versi utama: tidak disokong, belum diverifikasi**。Jika versi baharu sudah menulis data dengan format/semantik baharu，**tiada** jaminan "kembali kepada binari lama dan baca seadanya";
  rollback hanya boleh bergantung pada sandaran sebelum naik taraf。
- **Format konfigurasi tidak berubah**: masih JSON + pembolehubah persekitaran `SPEEDMQ_*`。**Konfigurasi YAML belum disokong**（perlu memperkenalkan kebergantungan penghurai, M8-17 menunggu penilaian），
  naik taraf tidak akan membawa YAML。
- **Naik taraf panas pemalam/protokol dalam talian**: pemalam dikompil bersama kernel（bentuk A）atau dilancarkan mengikut konfigurasi `spawn`（bentuk B），
  naik taraf kernel = mula semula proses; **tiada** mekanisme menggantikan binari panas di tempat。
- **Migrasi di tempat enjin penyimpanan**: putaran segmen/pemadatan indeks ialah tingkah laku latar belakang masa operasi，**tiada** arahan "migrasi data/pemadatan" berasingan。
- **Naik taraf kluster di bawah cakera penuh**: **belum diverifikasi**。（Naik taraf bergilir di bawah pemisahan rangkaian telah diuji sebenar pada 2026-10-11, lihat nota di hujung §7。）
- Dokumen ini **tidak mengandungi** sebarang pengesahan pemindahan data antara SpeedMQ dan broker lain（RabbitMQ）。
