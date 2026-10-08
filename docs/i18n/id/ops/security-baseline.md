# Baseline Pengerasan Keamanan SpeedMQ (Checklist yang Dapat Dicentang)

> Prinsip: **hanya menuliskan kemampuan yang benar-benar dimiliki repositori ini**. Setiap poin memberikan "mengapa perlu dilakukan + bagaimana memverifikasi sudah dilakukan", perintah verifikasinya semua dapat dijalankan.
> Yang ditandai **【Terverifikasi】** berarti benar-benar **pernah dieksekusi** di mesin lokal (Windows + PowerShell 5.1, `1.0.0`);
> **【Belum Diverifikasi】** berarti belum dieksekusi atau saat ini tidak dapat dilakukan, tidak pernah berpura-pura.
> Semua perintah diberikan dalam versi curl bergaya `/bin/sh`, disertai versi PowerShell (untuk PowerShell 5.1 gunakan
> `Invoke-WebRequest ... -UseBasicParsing`).

---

## A. Autentikasi dan kontrol akses

### A-1. Ubah akun default `guest/guest` 【Terverifikasi】

- **Mengapa**: bawaan memiliki `guest/guest` (tag `administrator`), membuka ke luar sama dengan membuka pintu lebar-lebar.
- **Bagaimana**: ubah kata sandi / hapus akun saat runtime, **jangan** mengubah file konfigurasi (`users` hanya berlaku saat bootstrap pertama).

```bash
# Ubah kata sandi
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/users/guest \
     -H 'Content-Type: application/json' -d '{"password":"<新口令>","tags":["administrator"]}'
# Atau langsung hapus akun default (pastikan admin baru sudah dibuat terlebih dahulu)
curl -u guest:guest -X DELETE http://127.0.0.1:15672/api/users/guest
```

- **Bagaimana memverifikasi**: setelah diubah, kata sandi lama harus 401, kata sandi baru 200.
  **【Terverifikasi】** Keluaran uji nyata mesin lokal:

  ```
  --- 改密前 guest/guest ---          HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  PUT /api/users/guest -> 204
  --- 改密后 guest/guest（应 401）---  HTTP 401
  --- 改密后 guest/s3cret（应 200）--- HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  ```
- [ ] Akun default sudah diubah/dihapus

### A-2. Hak minimal: regex `configure` / `write` / `read` per vhost 【Terverifikasi】

- **Mengapa**: tiga klasifikasi selaras dengan RabbitMQ —— `configure` mengatur deklarasi/penghapusan topologi, `write` mengatur publikasi dan binding, `read` mengatur konsumsi dan pengambilan;
  pelanggaran wewenang mengembalikan 403. Berikan akun bisnis hanya yang dibutuhkannya.
- **Bagaimana**: `PUT /api/permissions/{vhost}/{user}`, misalnya konsumsi baca-saja: `{"configure":"^$","write":"^$","read":".*"}`.

```bash
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/permissions/%2F/appuser \
     -H 'Content-Type: application/json' -d '{"configure":"^$","write":"^$","read":".*"}'
```

- **Bagaimana memverifikasi**: gunakan akun terbatas untuk mencoba operasi yang melampaui wewenang, harus mendapat 403 `ACCESS_REFUSED`.
  **【Terverifikasi】** Mesin lokal menggunakan klien AMQP sungguhan dengan pengguna ber-`configure="^$"` untuk mendeklarasikan exchange, hasil uji nyata:

  ```
  FAIL 声明 durable 交换机失败: Exception (403) Reason: "ACCESS_REFUSED - access to configure 'r.ex' refused for user 'restricted'"
  ```
- [ ] Setiap akun bisnis hanya diberi regex yang diperlukan, dan tidak diberikan tag `administrator`/`management`

### A-3. Ketentuan tag `administrator` (izin penuh implisit) —— berikan dengan hati-hati 【Terverifikasi】

- **Mengapa**: **pengguna dengan tag `administrator` memiliki izin penuh atas semua vhost yang terlihat olehnya, tanpa memerlukan record izin**
  (selaras dengan ketentuan uji nyata RabbitMQ, lihat README / desain M8-7). Artinya, selama tag ini diberikan,
  regex izin tidak lagi berlaku — ini izin tertinggi.
- **Bagaimana**: hanya akun bidang manajemen/operasional yang diberi `administrator`; akun bisnis tidak diberi tag sama sekali, hanya lewat regex izin.

- **Bagaimana memverifikasi (mendemonstrasikan izin implisit)**: untuk vhost baru yang **tidak memiliki record izin sama sekali**, pengguna `administrator` harus langsung dapat digunakan.
  **【Terverifikasi】** Uji nyata mesin lokal: setelah membuat vhost `drillvh` (tanpa record izin apa pun), `guest` (administrator) berhasil mendeklarasikan topologi di atasnya:

  ```
  --- administrator 隐式权限：guest 在 drillvh 上声明拓扑 ---
  OK  已声明 durable 交换机 a.ex / 队列 a.q，并绑定 key=k   (exit=0)
  ```
- [ ] Tag `administrator` hanya diberikan ke sangat sedikit akun operasional

### A-4. `remote_access`: batasi akun hanya dapat login dari mesin lokal 【Belum Diverifikasi (tidak dapat menyimulasikan sumber jarak jauh di mesin yang sama)】

- **Mengapa**: selaras dengan RabbitMQ, `guest` bawaan secara default hanya mengizinkan login dari mesin lokal; saat deployment ke luar harus memastikan sumber akun istimewa dibatasi.
- **Bagaimana / ketentuan (batasan penting)**:
  - `remote_access` hanya dapat ditulis di `users.<name>.remote_access` pada **file konfigurasi**, **hanya berlaku saat bootstrap pertama**;
  - **akun yang dibuat melalui API manajemen / `speedmqctl` selalu `remote_access=true`** (mengizinkan login dari sumber mana pun) ——
    dasar komentar `UpsertUser` di `internal/broker/observe.go` dan uji nyata `"remote_access":true` di `meta/state.json`.
    Artinya **API saat ini tidak dapat membatasi suatu akun hanya untuk mesin lokal**.
- **Bagaimana memverifikasi**: dari **host lain** (bukan `127.0.0.1`) menggunakan akun tersebut untuk terhubung, harus ditolak 403; koneksi dari mesin lokal harus berhasil.
  **【Belum Diverifikasi】**: lingkungan mesin lokal tidak dapat menyusun sumber jarak jauh yang nyata, belum diuji.
- [ ] Batasan sumber akun istimewa sudah dievaluasi sesuai ketentuan di atas (perhatikan akun yang dibuat API secara default membuka akses jarak jauh)

---

## B. Keamanan transmisi (TLS)

Item konfigurasi TLS (lapisan akses dan bidang manajemen **berbagi** set field yang sama): `cert_file` / `key_file` / `ca_file` / `client_auth` / `min_version`.

### B-1. Mengaktifkan TLS dan menolak start jika salah konfigurasi 【Terverifikasi】

- **Mengapa**: sertifikat dibaca dan divalidasi **saat startup** —— salah konfigurasi langsung menolak start, bukan menunggu klien pertama terhubung baru terungkap.
- **Bagaimana**: sediakan `cert_file` + `key_file` di `listeners.<plugin>[].tls` atau `management.tls` (**diberikan bersamaan** baru aktif).

- **Bagaimana memverifikasi**: jalankan dengan konfigurasi salah, harus langsung gagal.
  **【Terverifikasi】** Tiga konfigurasi salah diuji nyata di mesin lokal, semuanya `exit=1`, menolak start:

  ```
  badtls1: speedmqd 启动失败: listeners.amqp091[0].tls 需要同时提供 cert_file 与 key_file
  badtls2: speedmqd 启动失败: listeners.amqp091[0].tls.min_version 取值非法: "1.0"（可选 1.2 / 1.3）
  badtls3: speedmqd 启动失败: listeners.amqp091[0].tls 无效: 加载服务端证书失败（cert=... key=...）: open ...: The system cannot find the path specified.
  ```
- **Bagaimana memverifikasi (positif/negatif)**: klien TLS dapat terhubung, klien plaintext yang terhubung ke port TLS akan ditolak.
  **【Terverifikasi】** Mesin lokal menjalankan instans TLS (`amqp091` lewat TLS), dengan probe klien sungguhan:

  ```
  === 正向：TLS 客户端跑全量探针 ===      全部通过（25/25）: ...
  === 反向：明文客户端连 TLS 端口（应失败）===  FAIL ... 拨号失败: Exception (501) Reason: "EOF"   (exit=-1)
  ```
- [ ] Port protokol yang terbuka ke luar sudah mengaktifkan TLS

### B-2. `min_version` minimal 1.2 【Terverifikasi】

- **Mengapa**: menonaktifkan versi TLS yang terlalu lama; default adalah `1.2`, pilihan `1.2` / `1.3`.
- **Bagaimana memverifikasi**: tulis `min_version` menjadi `1.0`, startup harus melaporkan error (lihat keluaran `badtls2` di B-1).
- [ ] `min_version` adalah `1.2` atau `1.3`

### B-3. Autentikasi dua arah `client_auth: require_and_verify` (mTLS) 【Terverifikasi sebagian】

- **Mengapa**: mewajibkan klien menunjukkan dan memverifikasi sertifikat, mencegah klien tidak berwenang masuk ke port protokol.
- **Bagaimana**: konfigurasikan `ca_file` + `client_auth: require_and_verify` (dua yang terakhir mewajibkan penyediaan `ca_file` bersamaan).
- **【Terverifikasi】**: jalur TLS end-to-end dan jalur penolakan sudah diverifikasi dengan klien sungguhan (B-1). **mTLS (mewajibkan dan memverifikasi sertifikat klien) belum dilatih secara terpisah di mesin lokal**.
- [ ] Port yang memerlukan mTLS sudah dikonfigurasi `require_and_verify` + `ca_file`

### B-4. TLS bidang manajemen 【Belum Diverifikasi】

- **Mengapa**: bidang manajemen melalui Basic Auth mengirimkan kata sandi, harus dienkripsi.
- **Bagaimana**: `management.tls` menggunakan field yang sama dengan listener protokol.
- **【Belum Diverifikasi】**: latihan mesin lokal mengikat bidang manajemen ke port plaintext mesin lokal, belum menjalankan HTTPS bidang manajemen secara terpisah.
- [ ] Bidang manajemen sudah mengaktifkan TLS (atau dibatasi ketat dalam jaringan tepercaya)

---

## C. Penyempitan permukaan paparan

### C-1. Penyempitan jangkauan listen bidang manajemen 【Terverifikasi (alamat listen diuji nyata)】

- **Mengapa**: bidang manajemen default `:15672` (semua NIC). Deployment ke luar harus diikat ke alamat intranet/loopback, atau batasi sumber dengan firewall.
- **Bagaimana**: konfigurasikan `management.addr` menjadi `127.0.0.1:15672` atau alamat intranet; atau matikan sepenuhnya dengan `management.enabled=false`
  (setelah dimatikan tidak ada port manajemen, tetapi `speedmqctl` juga menjadi tidak tersedia).
- **Bagaimana memverifikasi**:
  **【Terverifikasi】** Mesin lokal mengonfigurasi bidang manajemen menjadi `127.0.0.1:15677`, alamat listen uji nyata memang loopback:

  ```
  LocalAddress LocalPort
  ------------ ---------
  127.0.0.1        15677
  ```
- [ ] Alamat binding bidang manajemen sudah disempitkan (atau sudah dinonaktifkan)

### C-2. Hanya buka port protokol yang diperlukan 【Belum Diverifikasi】

- **Mengapa**: default membuka AMQP `5672` dan MQTT `1883` sekaligus; jika tidak menggunakan MQTT matikan saja, kurangi permukaan serangan.
- **Bagaimana**: `plugins.mqtt.enabled=false` (atau hapus dari `listeners`); penonaktifan yang dilakukan adalah **menutup port sebenarnya**, bukan sekadar mengubah bit status.
- **Bagaimana memverifikasi**: setelah dinonaktifkan port terkait tidak lagi listening (`Get-NetTCPConnection -State Listen` tidak melihatnya).
  **【Belum Diverifikasi】**: latihan mesin lokal membuka kedua protokol, tidak memverifikasi secara terpisah hilangnya port setelah dimatikan.
- [ ] Plugin protokol yang tidak digunakan sudah dinonaktifkan

---

## D. Pengerasan operasi kontainer

Fakta image repositori (`Dockerfile`): biner statis + alpine, **berjalan sebagai non-root (uid 10001, pengguna `speedmq`)**,
direktori data `/var/lib/speedmq` sebagai volume. `docker-compose.yml` menggunakan **named volume** untuk persistensi, konfigurasi **dipasang read-only**, rotasi log.

### D-1. Berjalan sebagai non-root 【Belum Diverifikasi (mesin lokal tidak menjalankan Docker)】

- **Mengapa**: hak minimal, mengurangi dampak setelah escape kontainer.
- **Bagaimana**: image default sudah uid 10001; **jangan** menimpanya dengan `--user root`.
- **Bagaimana memverifikasi**: `docker compose run -T --rm broker id` harus menampilkan `uid=10001`. (`run` di lingkungan non-interaktif harus menambahkan `-T`)
- [ ] Kontainer berjalan sebagai non-root (tidak ditimpa root)

### D-2. Filesystem root read-only + kuota sumber daya + pemangkasan capability (saran, compose repositori belum mengaktifkan secara default) 【Belum Diverifikasi】

- **Mengapa**: filesystem root read-only dapat mencegah pengubahan biner saat runtime; kuota sumber daya mencegah satu kontainer melumpuhkan host; pemangkasan capabilities mengurangi permukaan serangan kernel.
- **Bagaimana** (contoh, gabungkan ke layanan `broker` di compose sesuai kebutuhan):

```yaml
services:
  broker:
    read_only: true
    tmpfs:
      - /tmp
    security_opt:
      - no-new-privileges:true
    cap_drop: ["ALL"]
    deploy:
      resources:
        limits:
          cpus: "4"
          memory: 8g
```

- **Bagaimana memverifikasi**: mencoba menulis ke path root di dalam kontainer harus gagal (read-only); `docker inspect` akan terlihat kuota sumber daya.
  **【Belum Diverifikasi】** (mesin lokal tidak menjalankan Docker); dan **filesystem root read-only perlu memastikan `data_dir` berada di volume yang dapat ditulis**, jika tidak kernel tidak dapat menulis ke disk.
- [ ] Filesystem root read-only dan kuota sumber daya sudah dievaluasi (perhatikan `data_dir` harus di volume yang dapat ditulis)

---

## E. Keterbatasan yang diketahui (saat ini memang tidak bisa dilakukan, jangan berharap)

Berikut semuanya **celah faktual**, harap diakui secara eksplisit dalam desain keamanan, jangan mengasumsikan keberadaannya:

1. **Kata sandi disimpan dan disalin secara plaintext**. Field `password` di `meta/state.json` plaintext (**【Terverifikasi】** uji nyata terlihat
   `"password":"drillpass"`); di file konfigurasi juga plaintext. **Tidak ada** hash kata sandi (hash dan backend autentikasi eksternal diserahkan ke plugin autentikasi).
   → Akibat: **direktori data dan file cadangan setara dengan kredensial sensitif**, harus dilindungi dengan izin file dan enkripsi.
2. **Tidak ada log audit**. Penambahan/penghapusan/perubahan di bidang manajemen akan mencetak log biasa (seperti `管理面更新用户 actor=... user=...`),
   tetapi **tidak ada** aliran audit yang berdiri sendiri dan tidak dapat diubah, juga tidak ada catatan tingkat kepatuhan "siapa mengubah apa kapan".
3. **Tidak ada autentikasi eksternal seperti LDAP / OAuth2 / JWT**. v1 bawaan hanya `PLAIN` / `AMQPLAIN`
   (`auth.Store.Mechanisms()` uji nyata hanya mengembalikan keduanya).
4. **SASL `EXTERNAL` belum diimplementasikan**: meskipun mTLS dikonfigurasi, lapisan protokol **tetap memakai autentikasi kata sandi PLAIN**
   (langkah "bebas kata sandi dengan sertifikat klien" tidak ada). Sertifikat hanya divalidasi di lapisan transport.
5. **`remote_access` tidak dapat diatur melalui API**: akun yang dibuat API/CLI manajemen selalu mengizinkan login jarak jauh (lihat A-4),
   tidak dapat membatasi satu akun hanya untuk mesin lokal.
6. **Bidang manajemen tidak memiliki whitelist sumber / tidak ada rate limit**: hanya bisa mengandalkan alamat binding, firewall, TLS untuk menyempitkan permukaan paparan.
7. **Tidak ada sandbox plugin**: plugin bentuk A satu proses dengan kernel; plugin eksternal bentuk B meskipun memiliki isolasi proses, tetapi **bidang data melalui proksi koneksi mesin lokal**,
   dan plugin dapat memanggil semantik kernel (dibatasi oleh validasi vhost dan izin), **bukan** sandbox keamanan.
8. **Tag bidang manajemen hanya ada tiga tingkat `administrator`/`management`/`monitoring`**, tidak ada RBAC per-resource yang lebih halus.

---

## F. Daftar ringkasan

- [ ] A-1 Akun default sudah diubah/dihapus 【alur Terverifikasi】
- [ ] A-2 Hak minimal akun bisnis (regex), tanpa tag admin 【jalur 403 Terverifikasi】
- [ ] A-3 Tag `administrator` hanya diberikan ke akun operasional 【ketentuan izin implisit Terverifikasi】
- [ ] A-4 Batasan sumber akun istimewa sudah dievaluasi (perhatikan akun yang dibuat API membuka akses jarak jauh secara default)
- [ ] B-1 Port yang terbuka ke luar mengaktifkan TLS, salah konfigurasi langsung menolak start 【Terverifikasi】
- [ ] B-2 `min_version` ≥ 1.2 【Terverifikasi】
- [ ] B-3 Port yang memerlukan mTLS dikonfigurasi `require_and_verify` + `ca_file`
- [ ] B-4 Bidang manajemen mengaktifkan TLS
- [ ] C-1 Alamat binding bidang manajemen disempitkan 【alamat listen Terverifikasi】
- [ ] C-2 Plugin protokol yang tidak digunakan dinonaktifkan
- [ ] D-1 Kontainer berjalan sebagai non-root
- [ ] D-2 Filesystem root read-only / kuota sumber daya / pemangkasan capability sudah dievaluasi
- [ ] E Keterbatasan yang diketahui (kata sandi plaintext, tanpa audit, tanpa LDAP/OAuth2, SASL EXTERNAL belum diimplementasikan) sudah diakui dalam desain keamanan
