# Sandaran dan Pemulihan SpeedMQ

> Semua kesimpulan "ujian sebenar" dalam dokumen ini berasal daripada satu latihan sebenar pada **Windows + PowerShell 5.1**（`data_dir` sementara dan port sementara）.
> Arahan latihan dan output penting dipaparkan seadanya di §6. Bahagian **【Belum diverifikasi】** akan ditandakan secara eksplisit (sandaran/pemulihan kluster, sandaran volum Docker dsb.).

---

## 1. Apa yang perlu disandarkan

Di bawah `data_dir` **seluruh kandungan mesti disandarkan**, yang penting ialah perkara berikut (susun atur lihat `upgrade.md` §3):

| Laluan | Fungsi | Akibat jika hilang |
| --- | --- | --- |
| `meta/state.json` | Syot kilat metadata mesin tunggal: vhost / penukar / baris gilir / pengikatan / pengguna / kebenaran / polisi | Topologi dan akaun semuanya hilang |
| `meta/raft.log`、`meta/raft.state`、`meta/snapshot.json` | 【Kluster】Log Raft / undian penggal / syot kilat + jadual ahli | Identiti kluster dan konsistensi metadata hilang |
| `meta/users.seeded`、`meta/vhosts.seeded` | Penanda but | Jika hilang, users/vhosts dalam konfigurasi akan **diseed semula**（akaun/vhost yang dipadam hidup semula） |
| `msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/0000NN.seg` + `index/0000NN.idx` | Data mesej dan indeks baris gilir klasik | Mesej gigih hilang |
| `quorum/<safe(vhost)>/<safe(queue)>/` | 【Kluster】Log/syot kilat Raft baris gilir kuorum | Data baris gilir kuorum hilang |
| Fail sijil (PEM yang ditunjuk oleh `cert_file`/`key_file`/`ca_file` dalam konfigurasi) | Sijil TLS | Disandarkan berasingan daripada `data_dir`, selepas mula semula TLS gagal dihidupkan |

> Keadaan lembut (mesej belum diakui, pengguna, kiraan prefetch) **hanya dalam memori**、tidak ditulis ke cakera, sandaran **tidak mengandungi** dan tidak sepatutnya mengandungi perkara tersebut.

---

## 2. Keperluan konsistensi: **proses mesti dihentikan dahulu**; sandaran panas **tidak selamat**

### 2.1 Kesimpulan

- ✅ **Cara selamat**: **Hentikan proses broker**（keluar dengan sopan akan melakukan flush akhir）, kemudian salin `data_dir`.
- ❌ **Sandaran panas (menyalin fail secara langsung semasa proses berjalan): tidak selamat, tiada jaminan.**

### 2.2 Mengapa sandaran panas tidak selamat

Penyimpanan mesej ialah **dua fail**（fail segmen `*.seg` dan fail indeks `index/*.idx`）, kedua-duanya **bukan komit atomik**:

- Semasa pemulihan，**indeks dijadikan rujukan** untuk menentukan "mesej mana yang masih hidup", kemudian membaca fail segmen mengikut `(nombor segmen, offset, panjang)` dalam indeks.
- Sandaran panas mungkin menyalin keadaan perantaraan di mana **indeks sudah merujuk, fail segmen belum ditulis lengkap**（atau sebaliknya）:
  - Indeks merujuk rekod yang tidak wujud dalam segmen → mesej tersebut **gagal dibaca dan dilangkau**（bersamaan kehilangan mesej gigih yang telah diakui）;
  - Segmen ada rekod tetapi indeks tidak merujuk → mesej tersebut **tidak dipulihkan**.
- Walaupun pemulihan akan menggunakan CRC32 untuk membuang **rekod separuh tulis di hujung**, itu hanya meliputi "penulisan rosak di hujung fail tunggal", **tidak dapat membaiki ketakselarasan antara indeks dan segmen**.

### 2.3 Tentang "bila penulisan sampai ke cakera"（pemerhatian ujian sebenar）

- Lalai `fsync: os` + `flush_interval_ms: 200`: mesej ditulis `write()` oleh goroutine flush latar belakang ke sistem pengendalian dalam **paling lama kira-kira 200 ms**
  （tanpa fsync）, dan publisher confirm juga dikembalikan selepas itu.
- Ujian sebenar: selepas menerbitkan mesej gigih **serta-merta** menyemak saiz fail segmen, sudah dapat melihat data（`t=0ms seg=832`）; iaitu "bait yang dapat dilihat oleh OS" pada asasnya selaras dengan confirm.
- **Perhatian**: ini hanya bermakna "sampai ke penimbal OS", **membunuh proses tidak akan kehilangan**（penimbal OS kekal walaupun proses dibunuh）, tetapi **kehilangan kuasa akan kehilangan**.
  Untuk "menerima confirm bermakna sudah fsync ke cakera", sila tukar `storage.fsync` kepada `batch` / `always`.**【Senario kehilangan kuasa belum diuji】**

---

## 3. Langkah sandaran

### 3.1 Mesin tunggal (disyorkan)

```powershell
# 1) Hentikan proses (latar depan: Ctrl+C; latar belakang: Stop-Process)
Stop-Process -Name speedmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) Salin seluruh data_dir (dengan cap masa)
$data = "C:\speedmq\data"
Copy-Item -Recurse -Force $data "C:\backup\speedmq-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) (Pilihan) Sahkan syot kilat metadata dalam sandaran boleh dihuraikan
Get-Content "C:\backup\speedmq-...\meta\state.json" -Raw | ConvertFrom-Json | Select-Object -ExpandProperty VHosts
```

### 3.2 Kluster

- **Setiap nod menyandarkan `data_dir` sendiri**（metadata direplikasi kepada semua melalui Raft, data mesej berada pada nod Owner, replika baris gilir kuorum dalam direktori Raft masing-masing）.
- Urutan pemberhentian: **hentikan satu nod pada satu masa**; jangan hentikan berbilang ahli pengundi serentak（lihat `upgrade.md` §7.2）.
- Untuk mendapatkan **syot kilat konsisten seluruh kluster**, perlu hentikan semua nod mengikut urutan kemudian salin masing-masing; dalam pengeluaran lebih biasa ialah "henti/salin/mula mengikut nod".
- **【Belum diverifikasi】** Mesin ini belum melakukan latihan sandaran/pemulihan kluster sebenar.

### 3.3 Docker（volum bernama）

```powershell
# Selepas menghentikan kontena, bungkus dan salin kandungan volum keluar menggunakan kontena sekali guna
docker compose down
docker run --rm -v speedmq-data:/data -v ${PWD}:/backup alpine `
  tar czf /backup/speedmq-data.tar.gz -C /data .
```
> **【Belum diverifikasi】**（mesin ini tidak menjalankan Docker）.

---

## 4. Langkah pemulihan

### 4.1 Mesin tunggal

```powershell
# 1) Sahkan proses telah berhenti
Get-Process -Name speedmqd -ErrorAction SilentlyContinue

# 2) Pindahkan (atau padam) data_dir semasa, elakkan fail lama dan baharu bercampur
Move-Item "C:\speedmq\data" "C:\speedmq\data.broken"

# 3) Pulihkan menggunakan sandaran
Copy-Item -Recurse -Force "C:\backup\speedmq-YYYYMMDD-HHMMSS" "C:\speedmq\data"

# 4) Mula
& "C:\speedmq\speedmqd.exe" -config "C:\speedmq\configs\speedmqd.json" -log-level info
```

Perkara penting:
- **Direktori lama mesti dipindahkan dahulu**, tidak boleh "fail sandaran ditindih ke atas direktori separuh tinggal";
- `data_dir` yang dipulihkan mesti **sama set vhost/baris gilir** seperti semasa sandaran（nama direktori ialah hasil pengekodan, boleh digunakan merentas mesin）;
- **Jangan** menggunakan peluang pemulihan untuk mengubah `vhosts`/`users` dalam fail konfigurasi（hanya berkesan semasa but pertama kali, ubah pun tidak berkesan, lihat `upgrade.md` §4.2）.

### 4.2 Kluster

- Pulihkan satu nod: pulihkan `data_dir` nod tersebut mengikut §4.1 kemudian mula; ia akan menyertai semula sebagai ahli sedia ada dan menyusul log Raft.
- Pulihkan seluruh kluster: **pulihkan dan mulakan nod majoriti dahulu**（≥ separuh ahli pengundi）, kluster baru boleh memilih leader; kemudian pulihkan nod selebihnya.
- **【Belum diverifikasi】** Pemulihan kluster belum diuji.

---

## 5. Kaedah pengesahan selepas pemulihan

Gunakan API pengurusan dan klien sebenar untuk silang semak（disyorkan buat semua）:

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }

# a) Jumlah objek dan jumlah mesej (bilangan baris gilir/penukar/pengikatan/pengguna; messages/ready/unacked)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# b) Semak messages / messages_ready bagi setiap baris gilir (boleh banding dengan rekod sebelum sandaran)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/queues' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# c) vhost / pengguna / polisi semuanya ada?
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/vhosts' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/users' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/policies' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# d) Kluster (mesin tunggal akan mengembalikan enabled=false / mode=local / role=single)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/cluster' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

- **Lihat log permulaan**: sepatutnya muncul `已从磁盘恢复队列消息 ... messages=N` dan `队列已恢复持久化消息 ... messages=N`; N sepatutnya sama seperti sebelum sandaran.
- **Amaran dalam log**: jika sesuatu baris gilir sebelum ini pernah mengalami mesej dimakan/dikosongkan, semasa pemulihan mungkin muncul
  `恢复消息失败，已跳过 ... seq=K err="读取记录头失败: EOF"` dan `恢复时清理了无存活消息的段`.
  Ini ialah sisa indeks bagi **rekod yang telah diselesaikan (ack/purge)**, tergolong dalam **bunyi log yang diketahui, tidak menjejaskan ketepatan data**（lihat §7）.
- **Klien sebenar**: ambil semula mesej dari baris gilir dan semak kiraan/kandungan（lihat §6 langkah (7)）.

---

## 6. Latihan ujian sebenar (arahan dan output sebenar)

> Persekitaran: `data_dir` dalam direktori sementara, AMQP `127.0.0.1:5676`、satah pengurusan `127.0.0.1:15677`、MQTT `127.0.0.1:1884`,
> akaun lalai `guest/guest`. Log permulaan:
> ```
> level=INFO msg="SpeedMQ 启动中" version=1.0.0 ... data_dir=...\data ... fsync=os
> level=INFO msg=管理面已启动 component=management addr=127.0.0.1:15677
> ```

**(1) Bina topologi durable + hantar 5 mesej gigih（klien sebenar `amqp091-go`）**

```
OK  已声明 durable 交换机 persist.ex / 队列 persist.q，并绑定 key=k
OK  已发布 5 条持久消息（delivery-mode=2）并收到全部 confirm
```

**(2) Bina pengguna / vhost / kebenaran / polisi（API pengurusan）**

```
vhost PUT -> 201
user PUT -> 201
perm PUT -> 204
policy PUT -> 201
```

**(3) Keadaan sebelum sandaran（API pengurusan）**

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

**(4) Fail cakera sebelum sandaran**

```
data\meta\state.json                                             (1118 B)
data\meta\users.seeded                                           (37 B)
data\meta\vhosts.seeded                                          (37 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\000001.seg       (1152 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\index\000001.idx (1023 B)
```

**(5) Hentikan proses → sandaran → kosongkan → pemulihan**

```
listeners still up: 0                       # 5676/1884/15677 semuanya telah ditutup
=== 备份内容 ===   （与 (4) 完全一致，逐字节复制）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
data 目录是否存在(应为 False): False         # data_dir asal telah dipadam, mensimulasikan kehilangan data
=== 恢复后内容 ===   （从备份复制回来，与 (4) 一致）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
```

**(6) Log pemulihan selepas mula semula (baris penting)**

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=persist.q messages=5 segments=2
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=persist.q queue=persist.q messages=5
level=INFO msg=元数据层已打开 component=broker mode=local ... queues=1 exchanges=1 bindings=1 users=2
```
> Pada masa yang sama muncul beberapa baris `level=WARN msg=恢复消息失败，已跳过 ... seq=1..13 err="读取记录头失败: EOF"`:
> ini ialah sisa indeks yang ditinggalkan oleh mesej yang **telah dipurge sebelum ini** dalam latihan ini（telah diselesaikan, data segmen telah dikitar semula）, **tidak menjejaskan pemulihan 5 mesej di bawah**.

**(7) Penegasan selepas pemulihan: API pengurusan + klien sebenar**

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

**Kesimpulan**: topologi durable（penukar+baris gilir+pengikatan）、5 mesej gigih、pengguna、vhost、kebenaran、polisi **semuanya dipulihkan**,
klien sebenar boleh mengambil semula semua mesej seperti asal.**Latihan lulus.**

### 6.1 Perbandingan: pemulihan baris gilir yang tidak melalui penggunaan/purge lebih "senyap"

Untuk membezakan sama ada WARN di atas ialah fenomena umum, buat satu lagi **perbandingan terkawal**: bina baris gilir durable baharu `clean.q`、hantar 3 mesej gigih、
**tanpa penggunaan tanpa pengosongan**, hentikan proses kemudian mula semula:

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=clean.q messages=3 segments=1
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=clean.q queue=clean.q messages=3
```
**Tiada sebarang WARN**. Ini menunjukkan WARN hanya muncul dalam senario "indeks masih menyimpan rekod yang telah diselesaikan"（lihat §7）.

---

## 7. Isu dan had yang diketahui (direkodkan dengan jujur)

1. **Bunyi log pemulihan（diperhatikan secara nyata）**: apabila baris gilir pernah mengalami penggunaan/pengosongan dalam sejarahnya（mesej telah ack/purge）,
   indeksnya masih menyimpan rujukan kepada rekod yang telah dikitar semula, semasa pemulihan akan mencetak **`恢复消息失败，已跳过` WARN untuk setiap satu baris demi baris**,
   dan mencipta/membersihkan satu `000000.seg` kosong（log `恢复时清理了无存活消息的段`）.
   **Tidak menjejaskan ketepatan data**（mesej belum diakui yang masih hidup dapat dipulihkan dengan betul）, tetapi akan **mencemarkan log**、dan pada baris gilir besar/throughput tinggi mungkin membanjiri skrin.
   Cadangan: gunakan `已从磁盘恢复队列消息 ... messages=N` sebagai rujukan, abaikan WARN yang menyasarkan rekod yang telah diselesaikan ini;
   jika jumlah log tidak boleh diterima, sila maklumkan kepada penyelenggara kernel（dokumen ini tidak mengubah kod）.
2. **Sandaran panas tidak selamat**（§2）: jangan salin `data_dir` secara langsung semasa proses berjalan.
3. **`fsync: os` tidak menjamin tiada kehilangan semasa putus kuasa**: untuk "confirm bermakna ke cakera" sila guna `batch` / `always`.
4. **Kata laluan teks biasa**: kata laluan pengguna dalam `meta/state.json` ialah **teks biasa**（ujian sebenar dapat melihat `"password":"drillpass"`）——
   fail sandaran oleh itu **mesti dikendalikan sebagai data sensitif**（kawalan akses, penyimpanan disulitkan）. Lihat `security-baseline.md` untuk butiran.
5. **【Belum diverifikasi】** Sandaran/pemulihan kluster、sandaran/pemulihan volum Docker、senario putus kuasa、penulisan serentak semasa proses pemulihan.
