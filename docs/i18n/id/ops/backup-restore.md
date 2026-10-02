# SwiftMQ Pencadangan dan Pemulihan

> Kesimpulan "hasil uji nyata" dalam dokumen ini seluruhnya berasal dari satu latihan nyata di **Windows + PowerShell 5.1** (dengan `data_dir` sementara dan port sementara).
> Perintah latihan dan keluaran pentingnya ditempel apa adanya di §6. Bagian yang **【Belum Diverifikasi】** ditandai secara eksplisit (pencadangan/pemulihan klaster, pencadangan volume Docker, dan lain-lain).

---

## 1. Yang perlu dicadangkan

Seluruh isi `data_dir` **wajib dicadangkan sepenuhnya**, yang terpenting adalah hal-hal berikut (tata letak lihat `upgrade.md` §3):

| Path | Fungsi | Akibat jika hilang |
| --- | --- | --- |
| `meta/state.json` | Snapshot metadata mode mandiri: vhost / exchange / antrean / binding / pengguna / izin / policy | Seluruh topologi dan akun hilang |
| `meta/raft.log`, `meta/raft.state`, `meta/snapshot.json` | 【Klaster】 Log Raft / pemungutan suara term / snapshot + tabel anggota | Identitas klaster dan konsistensi metadata hilang |
| `meta/users.seeded`, `meta/vhosts.seeded` | Penanda bootstrap | Jika hilang, users/vhosts di konfigurasi akan **di-seed ulang** (akun/vhost yang sudah dihapus hidup kembali) |
| `msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/0000NN.seg` + `index/0000NN.idx` | Data dan indeks pesan antrean klasik | Pesan persisten hilang |
| `quorum/<safe(vhost)>/<safe(queue)>/` | 【Klaster】 Log/snapshot Raft antrean quorum | Data antrean quorum hilang |
| File sertifikat (PEM yang ditunjuk oleh `cert_file`/`key_file`/`ca_file` di konfigurasi) | Sertifikat TLS | Dicadangkan terpisah dari `data_dir`; setelah restart TLS tidak bisa naik |

> Soft state (pesan yang belum di-ack, konsumen, penghitung prefetch) **hanya ada di memori**, tidak ditulis ke disk, **tidak termasuk** dalam pencadangan dan tidak seharusnya disertakan.

---

## 2. Persyaratan konsistensi: **proses harus dihentikan terlebih dahulu**; hot backup **tidak aman**

### 2.1 Kesimpulan

- ✅ **Cara aman**: **hentikan proses broker** (keluar secara graceful akan melakukan flush penutup), lalu salin `data_dir`.
- ❌ **Hot backup (menyalin file langsung saat proses berjalan): tidak aman, tidak ada jaminan.**

### 2.2 Mengapa hot backup tidak aman

Penyimpanan pesan terdiri dari **dua file** (file segmen `*.seg` dan file indeks `index/*.idx`), keduanya **bukan commit atomik**:

- Saat pemulihan, **indeks yang menjadi acuan** untuk menentukan "pesan mana yang masih hidup", lalu membaca file segmen berdasarkan `(nomor segmen, offset, panjang)` di indeks.
- Hot backup dapat menyalin keadaan antara di mana **indeks sudah merujuk tetapi file segmen belum tertulis lengkap** (atau sebaliknya):
  - Indeks merujuk record yang tidak ada di segmen → pesan tersebut **gagal dibaca dan dilewati** (sama dengan kehilangan pesan persisten yang sudah dikonfirmasi);
  - Segmen punya record tetapi indeks tidak merujuknya → pesan tersebut **tidak dipulihkan**.
- Meskipun pemulihan akan membuang **record setengah tertulis di ekor** dengan CRC32, itu hanya mencakup "kerusakan tulisan di ekor satu file", **tidak dapat memperbaiki ketidaksinkronan antara indeks dan segmen**.

### 2.3 Tentang "kapan tulisan sampai ke disk" (pengamatan hasil uji nyata)

- Default `fsync: os` + `flush_interval_ms: 200`: pesan oleh goroutine flush latar belakang di-`write()` ke sistem operasi dalam **paling lama sekitar 200 ms**
  (tanpa fsync), dan publisher confirm juga dikembalikan setelah itu.
- Hasil uji nyata: setelah memublikasikan pesan persisten, memeriksa ukuran file segmen **segera**, data sudah terlihat (`t=0ms seg=832`); artinya "byte yang dapat dilihat OS" pada dasarnya sinkron dengan confirm.
- **Catatan**: ini hanya berarti "sudah sampai buffer OS", **mematikan proses secara paksa tidak akan kehilangan** (proses dimatikan tidak kehilangan buffer OS), tetapi **mati listrik akan kehilangan**.
  Untuk "begitu menerima confirm berarti sudah fsync ke disk", ubah `storage.fsync` menjadi `batch` / `always`. **【Skenario mati listrik belum diuji】**

---

## 3. Langkah pencadangan

### 3.1 Mandiri (disarankan)

```powershell
# 1) Hentikan proses (foreground: Ctrl+C; background: Stop-Process)
Stop-Process -Name swiftmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) Salin seluruh data_dir (dengan stempel waktu)
$data = "C:\swiftmq\data"
Copy-Item -Recurse -Force $data "C:\backup\swiftmq-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) (opsional) Verifikasi bahwa snapshot metadata di cadangan dapat diparsing
Get-Content "C:\backup\swiftmq-...\meta\state.json" -Raw | ConvertFrom-Json | Select-Object -ExpandProperty VHosts
```

### 3.2 Klaster

- **Setiap node mencadangkan `data_dir`-nya sendiri** (metadata direplikasi melalui Raft ke seluruh node, data pesan berada di node Owner, replika antrean quorum ada di direktori Raft masing-masing).
- Urutan penghentian: **hanya hentikan satu node dalam satu waktu**; jangan menghentikan beberapa anggota voting sekaligus (lihat `upgrade.md` §7.2).
- Untuk mendapatkan **snapshot konsisten seluruh klaster**, perlu menghentikan semua node secara berurutan lalu menyalin masing-masing; di produksi yang lebih umum adalah "hentikan/salin/jalankan per node".
- **【Belum Diverifikasi】** Mesin lokal belum melakukan latihan pencadangan/pemulihan klaster sungguhan.

### 3.3 Docker (named volume)

```powershell
# Setelah menghentikan kontainer, gunakan kontainer sekali pakai untuk mengemas dan menyalin isi volume keluar
docker compose down
docker run --rm -v swiftmq-data:/data -v ${PWD}:/backup alpine `
  tar czf /backup/swiftmq-data.tar.gz -C /data .
```
> **【Belum Diverifikasi】** (mesin lokal tidak menjalankan Docker).

---

## 4. Langkah pemulihan

### 4.1 Mandiri

```powershell
# 1) Pastikan proses sudah berhenti
Get-Process -Name swiftmqd -ErrorAction SilentlyContinue

# 2) Pindahkan (atau hapus) data_dir saat ini, agar file lama dan baru tidak tercampur
Move-Item "C:\swiftmq\data" "C:\swiftmq\data.broken"

# 3) Pulihkan menggunakan cadangan
Copy-Item -Recurse -Force "C:\backup\swiftmq-YYYYMMDD-HHMMSS" "C:\swiftmq\data"

# 4) Jalankan
& "C:\swiftmq\swiftmqd.exe" -config "C:\swiftmq\configs\swiftmqd.json" -log-level info
```

Poin penting:
- **Direktori lama harus dipindahkan terlebih dahulu**, tidak boleh "menimpa file cadangan ke direktori yang masih tersisa sebagian";
- `data_dir` hasil pemulihan harus memiliki **kumpulan vhost/antrean yang sama** dengan saat pencadangan (nama direktori adalah hasil pengodean, dapat dipakai lintas mesin);
- **Jangan** memanfaatkan momen pemulihan untuk mengubah `vhosts`/`users` di file konfigurasi (hanya berlaku saat bootstrap pertama, tidak akan berefek, lihat `upgrade.md` §4.2).

### 4.2 Klaster

- Memulihkan satu node: pulihkan `data_dir` node tersebut sesuai §4.1 lalu jalankan, ia akan bergabung kembali sebagai anggota yang sudah ada dan mengejar log Raft.
- Memulihkan seluruh klaster: **pulihkan dan jalankan node mayoritas terlebih dahulu** (≥ separuh anggota voting), agar klaster dapat memilih leader; baru kemudian pulihkan node lainnya.
- **【Belum Diverifikasi】** Pemulihan klaster belum diuji nyata.

---

## 5. Metode verifikasi setelah pemulihan

Silang-periksa menggunakan API manajemen dan klien sungguhan (disarankan lakukan semua):

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }

# a) Total objek dan total pesan (jumlah antrean/exchange/binding/pengguna; messages/ready/unacked)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# b) Periksa messages / messages_ready per antrean (dapat dibandingkan dengan catatan sebelum pencadangan)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/queues' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# c) Apakah vhost / pengguna / policy semuanya ada
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/vhosts' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/users' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/policies' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# d) Klaster (mode mandiri akan mengembalikan enabled=false / mode=local / role=single)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/cluster' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

- **Lihat log startup**: harus muncul `已从磁盘恢复队列消息 ... messages=N` dan `队列已恢复持久化消息 ... messages=N`; N harus sama dengan sebelum pencadangan.
- **Peringatan dalam log**: jika suatu antrean sebelumnya pernah dikonsumsi/dikosongkan, saat pemulihan mungkin muncul
  `恢复消息失败，已跳过 ... seq=K err="读取记录头失败: EOF"` dan `恢复时清理了无存活消息的段`.
  Ini adalah sisa indeks dari **record yang sudah diselesaikan (ack/purge)**, merupakan **noise log yang diketahui, tidak memengaruhi kebenaran data** (lihat §7).
- **Klien sungguhan**: ambil kembali pesan dari antrean dan periksa jumlah/isi (lihat §6 langkah (7)).

---

## 6. Latihan uji nyata (perintah dan keluaran sungguhan)

> Lingkungan: `data_dir` di direktori sementara, AMQP `127.0.0.1:5676`, bidang manajemen `127.0.0.1:15677`, MQTT `127.0.0.1:1884`,
> akun default `guest/guest`. Log startup:
> ```
> level=INFO msg="SwiftMQ 启动中" version=1.0.0 ... data_dir=...\data ... fsync=os
> level=INFO msg=管理面已启动 component=management addr=127.0.0.1:15677
> ```

**(1) Membuat topologi durable + mengirim 5 pesan persisten (klien sungguhan `amqp091-go`)**

```
OK  已声明 durable 交换机 persist.ex / 队列 persist.q，并绑定 key=k
OK  已发布 5 条持久消息（delivery-mode=2）并收到全部 confirm
```

**(2) Membuat pengguna / vhost / izin / policy (API manajemen)**

```
vhost PUT -> 201
user PUT -> 201
perm PUT -> 204
policy PUT -> 201
```

**(3) Kondisi sebelum pencadangan (API manajemen)**

```
=== /api/overview ===
"object_totals":{"connections":0,"channels":0,"queues":1,"consumers":0,"exchanges":13}
"queue_totals":{"messages":5,"messages_ready":5,"messages_unacknowledged":0}

=== /api/queues/%2F/persist.q ===
"durable":true,"effective_policy_definition":{"max-length":100},"messages":5,"messages_ready":5,
"name":"persist.q","policy":"drillpol","type":"classic","vhost":"/"

=== /api/vhosts ===  名称: ["/","drillvh"]
=== /api/users ===   名称: ["drilluser","guest"]
=== /api/policies === [{"apply-to":"queues","definition":{"max-length":100},"name":"drillpol","pattern":"persist.*","priority":1,"vhost":"/"}]
```

**(4) File di disk sebelum pencadangan**

```
data\meta\state.json                                             (1118 B)
data\meta\users.seeded                                           (37 B)
data\meta\vhosts.seeded                                          (37 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\000001.seg       (1152 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\index\000001.idx (1023 B)
```

**(5) Hentikan proses → cadangkan → kosongkan → pulihkan**

```
listeners still up: 0                       # 5676/1884/15677 均已关闭
=== 备份内容 ===   （与 (4) 完全一致，逐字节复制）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
data 目录是否存在(应为 False): False         # 已删除原 data_dir，模拟数据丢失
=== 恢复后内容 ===   （从备份复制回来，与 (4) 一致）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
```

**(6) Log pemulihan setelah restart (baris kunci)**

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=persist.q messages=5 segments=2
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=persist.q queue=persist.q messages=5
level=INFO msg=元数据层已打开 component=broker mode=local ... queues=1 exchanges=1 bindings=1 users=2
```
> Pada saat yang sama muncul beberapa `level=WARN msg=恢复消息失败，已跳过 ... seq=1..13 err="读取记录头失败: EOF"`:
> ini adalah sisa indeks dari pesan yang **sebelumnya sudah di-purge** dalam latihan ini (sudah diselesaikan, data segmen sudah direklaim), **tidak memengaruhi pemulihan 5 pesan di bawah**.

**(7) Asersi setelah pemulihan: API manajemen + klien sungguhan**

```
=== 恢复后 /api/overview ===
"object_totals":{"queues":1,"exchanges":13,"consumers":0},"queue_totals":{"messages":5,"messages_ready":5,"messages_unacknowledged":0}

=== 恢复后 /api/queues/%2F/persist.q ===
"durable":true,"effective_policy_definition":{"max-length":100},"messages":5,"name":"persist.q","policy":"drillpol","type":"classic"

=== 恢复后 /api/vhosts (name) ===  / , drillvh
=== 恢复后 /api/users (name) ===   drilluser , guest
=== 恢复后 /api/policies ===       [{... "name":"drillpol","pattern":"persist.*" ...}]

=== 真实客户端断言 ===
OK  拓扑仍在：交换机 persist.ex / 队列 persist.q（声明时 message_count=5）
OK  取回 5 条持久消息: [persist-0 persist-1 persist-2 persist-3 persist-4]
```

**Kesimpulan**: topologi durable (exchange+antrean+binding), 5 pesan persisten, pengguna, vhost, izin, policy **semuanya pulih**,
klien sungguhan dapat mengambil kembali seluruh pesan apa adanya. **Latihan lulus.**

### 6.1 Pembanding: antrean yang tidak pernah dikonsumsi/di-purge pulihnya lebih "sunyi"

Untuk membedakan apakah WARN di atas adalah fenomena umum, dilakukan **pembanding terkendali** lain: buat antrean durable baru `clean.q`, kirim 3 pesan persisten,
**tidak dikonsumsi dan tidak dikosongkan**, hentikan proses lalu restart:

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=clean.q messages=3 segments=1
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=clean.q queue=clean.q messages=3
```
**Tidak ada WARN sama sekali**. Ini menunjukkan WARN hanya muncul pada skenario "indeks masih menyimpan record yang sudah diselesaikan" (lihat §7).

---

## 7. Masalah dan keterbatasan yang diketahui (dicatat apa adanya)

1. **Noise log pemulihan (teramati nyata)**: ketika antrean pernah mengalami konsumsi/pengosongan (pesan sudah ack/purge),
   indeksnya masih menyimpan rujukan ke record yang sudah direklaim, saat pemulihan akan **mencetak WARN `恢复消息失败，已跳过` satu per satu untuk setiap record**,
   dan membuat/membersihkan `000000.seg` kosong (log `恢复时清理了无存活消息的段`).
   **Tidak memengaruhi kebenaran data** (pesan belum di-ack yang masih hidup dapat dipulihkan dengan benar), tetapi akan **mencemari log**, pada antrean besar/throughput tinggi bisa membanjiri layar.
   Saran: gunakan `已从磁盘恢复队列消息 ... messages=N` sebagai acuan, abaikan WARN yang ditujukan untuk record yang sudah diselesaikan ini;
   jika volume log tidak dapat diterima, silakan laporkan kepada pemelihara kernel (dokumen ini tidak mengubah kode).
2. **Hot backup tidak aman** (§2): jangan menyalin `data_dir` secara langsung saat proses berjalan.
3. **`fsync: os` tidak menjamin tidak hilang saat mati listrik**: untuk "confirm berarti sudah ke disk" gunakan `batch` / `always`.
4. **Kata sandi plaintext**: kata sandi pengguna di `meta/state.json` adalah **plaintext** (uji nyata terlihat `"password":"drillpass"`) —
   file cadangan karena itu **harus diperlakukan sebagai data sensitif** (kontrol akses, penyimpanan terenkripsi). Lihat `security-baseline.md`.
5. **【Belum Diverifikasi】** Pencadangan/pemulihan klaster, pencadangan/pemulihan volume Docker, skenario mati listrik, penulisan bersamaan selama proses pemulihan.
