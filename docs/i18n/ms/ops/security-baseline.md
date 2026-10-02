# Garis Dasar Pengukuhan Keselamatan SwiftMQ (senarai semak boleh tanda)

> Prinsip: **hanya tulis keupayaan yang benar-benar dimiliki oleh repositori ini**。Setiap item memberikan "mengapa perlu buat + bagaimana sahkan sudah dibuat"，dan arahan pengesahan semuanya boleh dijalankan。
> Yang ditandakan **【Telah diverifikasi】** bermakna **benar-benar telah dilaksanakan** pada mesin ini（Windows + PowerShell 5.1，`1.0.0`）;
> **【Belum diverifikasi】** bermakna belum dilaksanakan atau pada masa ini tidak dapat dilakukan, sama sekali tidak berpura-pura。
> Semua arahan diberikan dalam gaya `/bin/sh` versi curl, disertakan versi PowerShell（PowerShell 5.1 sila guna
> `Invoke-WebRequest ... -UseBasicParsing`）。

---

## A. Pengesahan dan kawalan akses

### A-1. Ubah akaun lalai `guest/guest` 【Telah diverifikasi】

- **Mengapa**: lalai terbina dalam `guest/guest`（tag `administrator`），terdedah ke luar bermakna pintu terbuka luas。
- **Bagaimana**: ubah kata laluan / padam akaun semasa operasi，**jangan** ubah fail konfigurasi（`users` hanya berkesan semasa but pertama kali）。

```bash
# Ubah kata laluan
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/users/guest \
     -H 'Content-Type: application/json' -d '{"password":"<新口令>","tags":["administrator"]}'
# Atau padam terus akaun lalai (pastikan pentadbir baharu telah dibina dahulu)
curl -u guest:guest -X DELETE http://127.0.0.1:15672/api/users/guest
```

- **Cara sahkan**: selepas diubah, kata laluan lama mesti 401、kata laluan baharu 200。
  **【Telah diverifikasi】** Output ujian sebenar mesin ini:

  ```
  --- 改密前 guest/guest ---          HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  PUT /api/users/guest -> 204
  --- 改密后 guest/guest（应 401）---  HTTP 401
  --- 改密后 guest/s3cret（应 200）--- HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  ```
- [ ] Akaun lalai telah diubah/dipadam

### A-2. Kebenaran minimum: regex `configure` / `write` / `read` mengikut vhost 【Telah diverifikasi】

- **Mengapa**: tiga klasifikasi selaras RabbitMQ —— `configure` mengurus pengisytiharan/pemadaman topologi、`write` mengurus penerbitan dan pengikatan、`read` mengurus penggunaan dan pengambilan;
  melebihi kebenaran mengembalikan 403。Beri akaun perniagaan hanya apa yang diperlukan。
- **Bagaimana**: `PUT /api/permissions/{vhost}/{user}`，contohnya penggunaan baca sahaja: `{"configure":"^$","write":"^$","read":".*"}`。

```bash
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/permissions/%2F/appuser \
     -H 'Content-Type: application/json' -d '{"configure":"^$","write":"^$","read":".*"}'
```

- **Cara sahkan**: gunakan akaun terhad untuk cuba operasi melebihi kebenaran, sepatutnya mendapat 403 `ACCESS_REFUSED`。
  **【Telah diverifikasi】** Mesin ini menggunakan klien AMQP sebenar dengan pengguna `configure="^$"` untuk mengisytiharkan penukar, ujian sebenar:

  ```
  FAIL 声明 durable 交换机失败: Exception (403) Reason: "ACCESS_REFUSED - access to configure 'r.ex' refused for user 'restricted'"
  ```
- [ ] Setiap akaun perniagaan hanya diberikan regex yang diperlukan, dan tidak diberikan tag `administrator`/`management`

### A-3. Skop tag `administrator`（kebenaran penuh tersirat）—— beri dengan berhati-hati 【Telah diverifikasi】

- **Mengapa**: **Pengguna dengan tag `administrator` memiliki kebenaran penuh ke atas semua vhost yang dapat dilihatnya, tanpa memerlukan rekod kebenaran**
  （selaras dengan skop ujian sebenar RabbitMQ，lihat README / reka bentuk M8-7）。Maksudnya, asalkan tag ini diberikan，
  regex kebenaran tidak lagi berkesan—— ia ialah kebenaran tertinggi。
- **Bagaimana**: hanya akaun satah pengurusan/operasi diberikan `administrator`; akaun perniagaan sama sekali tidak diberi tag, hanya melalui regex kebenaran。

- **Cara sahkan (demonstrasi kebenaran tersirat)**: untuk vhost baharu yang **tiada sebarang rekod kebenaran**，pengguna `administrator` sepatutnya boleh terus digunakan。
  **【Telah diverifikasi】** Ujian mesin ini: selepas membina vhost baharu `drillvh`（tiada rekod kebenaran dibina），`guest`（administrator）berjaya mengisytiharkan topologi di atasnya:

  ```
  --- administrator 隐式权限：guest 在 drillvh 上声明拓扑 ---
  OK  已声明 durable 交换机 a.ex / 队列 a.q，并绑定 key=k   (exit=0)
  ```
- [ ] Tag `administrator` hanya diberikan kepada sangat sedikit akaun operasi

### A-4. `remote_access`: hadkan akaun hanya log masuk dari mesin tempatan 【Belum diverifikasi（mesin sama tidak dapat mensimulasikan sumber jauh）】

- **Mengapa**: selaras RabbitMQ，`guest` terbina dalam secara lalai hanya membenarkan log masuk mesin tempatan; semasa penggunaan luar, pastikan sumber akaun istimewa terhad。
- **Bagaimana / skop（kekangan penting）**:
  - `remote_access` hanya boleh ditulis dalam **fail konfigurasi** `users.<name>.remote_access`，**hanya berkesan semasa but pertama kali**;
  - **Akaun yang dibina melalui API pengurusan / `swiftmqctl` semuanya `remote_access=true`**（membenarkan log masuk dari mana-mana sumber）——
    asas komen `UpsertUser` dalam `internal/broker/observe.go` dan ujian sebenar `"remote_access":true` dalam `meta/state.json`。Maksudnya **API pada masa ini tidak dapat mengehadkan sesuatu akaun agar hanya mesin tempatan**。
- **Cara sahkan**: sambung dari **hos lain**（bukan `127.0.0.1`）menggunakan akaun tersebut, sepatutnya 403; sambungan mesin tempatan sepatutnya berjaya。
  **【Belum diverifikasi】**: persekitaran mesin ini tidak dapat membina sumber jauh sebenar, belum diuji。
- [ ] Had sumber akaun istimewa telah dinilai mengikut skop di atas（perhatikan akaun yang dibina melalui API secara lalai membuka akses jauh）

---

## B. Keselamatan pengangkutan (TLS)

Item konfigurasi TLS（lapisan akses dan satah pengurusan **berkongsi** set medan yang sama）: `cert_file` / `key_file` / `ca_file` / `client_auth` / `min_version`。

### B-1. Hidupkan TLS dan tolak permulaan jika salah konfigurasi 【Telah diverifikasi】

- **Mengapa**: sijil dibaca dan disahkan **semasa permulaan**—— salah konfigurasi terus tolak permulaan, bukan menunggu klien pertama menyambung baru terdedah。
- **Bagaimana**: sediakan `cert_file` + `key_file` dalam `listeners.<plugin>[].tls` atau `management.tls`（**hanya apabila kedua-duanya diberi** baru dihidupkan）。

- **Cara sahkan**: mulakan dengan konfigurasi salah, sepatutnya gagal serta-merta。
  **【Telah diverifikasi】** Tiga jenis konfigurasi salah diuji pada mesin ini, semuanya `exit=1`、tolak permulaan:

  ```
  badtls1: swiftmqd 启动失败: listeners.amqp091[0].tls 需要同时提供 cert_file 与 key_file
  badtls2: swiftmqd 启动失败: listeners.amqp091[0].tls.min_version 取值非法: "1.0"（可选 1.2 / 1.3）
  badtls3: swiftmqd 启动失败: listeners.amqp091[0].tls 无效: 加载服务端证书失败（cert=... key=...）: open ...: The system cannot find the path specified.
  ```
- **Cara sahkan (positif/negatif)**: klien TLS boleh menyambung、klien teks biasa menyambung ke port TLS akan ditolak。
  **【Telah diverifikasi】** Mesin ini memulakan instans TLS（`amqp091` melalui TLS），menggunakan klien sebenar sebagai prob:

  ```
  === 正向：TLS 客户端跑全量探针 ===      全部通过（25/25）: ...
  === 反向：明文客户端连 TLS 端口（应失败）===  FAIL ... 拨号失败: Exception (501) Reason: "EOF"   (exit=-1)
  ```
- [ ] Port protokol luaran telah menghidupkan TLS

### B-2. `min_version` sekurang-kurangnya 1.2 【Telah diverifikasi】

- **Mengapa**: nyahaktifkan versi TLS yang terlalu lama; lalai sudah `1.2`，boleh pilih `1.2` / `1.3`。
- **Cara sahkan**: tulis `min_version` sebagai `1.0`，permulaan sepatutnya melaporkan ralat（lihat output `badtls2` dalam B-1）。
- [ ] `min_version` ialah `1.2` atau `1.3`

### B-3. Pengesahan dua hala `client_auth: require_and_verify`（mTLS）【Sebahagian diverifikasi】

- **Mengapa**: memerlukan klien menunjukkan dan mengesahkan sijil, menghalang klien tidak dibenarkan menyambung ke port protokol。
- **Bagaimana**: konfigurasikan `ca_file` + `client_auth: require_and_verify`（dua yang terakhir memerlukan `ca_file` diberikan pada masa yang sama）。
- **【Telah diverifikasi】**: Laluan hujung ke hujung TLS dan laluan ditolak telah disahkan dengan klien sebenar（B-1）。**mTLS（memerlukan dan mengesahkan sijil klien）tidak dilatih secara berasingan pada mesin ini**。
- [ ] Port yang memerlukan mTLS telah dikonfigurasi dengan `require_and_verify` + `ca_file`

### B-4. TLS satah pengurusan 【Belum diverifikasi】

- **Mengapa**: satah pengurusan menggunakan Basic Auth untuk menghantar kata laluan, mesti disulitkan。
- **Bagaimana**: `management.tls` menggunakan medan yang sama seperti pendengar protokol。
- **【Belum diverifikasi】**: Latihan mesin ini mengikat satah pengurusan pada port teks biasa mesin tempatan, tidak menghidupkan HTTPS satah pengurusan secara berasingan。
- [ ] Satah pengurusan telah menghidupkan TLS（atau dihadkan dengan ketat dalam rangkaian dipercayai）

---

## C. Penumpuan permukaan terdedah

### C-1. Penumpuan skop pendengaran satah pengurusan 【Telah diverifikasi（ujian sebenar alamat pendengaran）】

- **Mengapa**: satah pengurusan lalai `:15672`（semua kad rangkaian）。Penggunaan luaran sepatutnya diikat kepada alamat intranet/loopback, atau hadkan sumber dengan firewall。
- **Bagaimana**: konfigurasikan `management.addr` sebagai `127.0.0.1:15672` atau alamat intranet; atau matikan sepenuhnya dengan `management.enabled=false`
  （selepas ditutup tiada port pengurusan, tetapi `swiftmqctl` juga tidak boleh digunakan）。
- **Cara sahkan**:
  **【Telah diverifikasi】** Mesin ini mengkonfigurasikan satah pengurusan sebagai `127.0.0.1:15677`，ujian sebenar alamat pendengaran memang loopback:

  ```
  LocalAddress LocalPort
  ------------ ---------
  127.0.0.1        15677
  ```
- [ ] Alamat pengikatan satah pengurusan telah ditumpukan（atau telah dinyahaktifkan）

### C-2. Hanya buka port protokol yang diperlukan 【Belum diverifikasi】

- **Mengapa**: lalai membuka AMQP `5672` dan MQTT `1883` serentak; jika tidak menggunakan MQTT, tutupnya untuk mengurangkan permukaan serangan。
- **Bagaimana**: `plugins.mqtt.enabled=false`（atau keluarkan dari `listeners`）; penyahaktifan ialah **menutup port sebenar**, bukan sekadar menukar bit status。
- **Cara sahkan**: selepas dinyahaktifkan port berkenaan tidak lagi mendengar（`Get-NetTCPConnection -State Listen` tidak dapat melihatnya）。
  **【Belum diverifikasi】**: Latihan mesin ini membuka kedua-dua protokol, tidak mengesahkan kehilangan port selepas ditutup secara berasingan。
- [ ] Pemalam protokol yang tidak digunakan telah dinyahaktifkan

---

## D. Pengukuhan operasi kontena

Fakta imej repositori（`Dockerfile`）: binari pautan statik + alpine，**berjalan sebagai bukan root（uid 10001, pengguna `swiftmq`）**，
direktori data `/var/lib/swiftmq` sebagai volum。`docker-compose.yml` menggunakan **volum bernama** untuk kegigihan、konfigurasi **dipasang baca sahaja**、putaran log。

### D-1. Berjalan sebagai bukan root 【Belum diverifikasi（mesin ini tidak menjalankan Docker）】

- **Mengapa**: kebenaran minimum, mengurangkan kesan selepas kontena melarikan diri。
- **Bagaimana**: imej lalai sudah uid 10001; **jangan** guna `--user root` untuk menimpa。
- **Cara sahkan**: `docker compose run -T --rm broker id` sepatutnya memaparkan `uid=10001`。（`run` dalam persekitaran bukan interaktif mesti menambah `-T`）
- [ ] Kontena berjalan sebagai bukan root（tidak ditimpa dengan root）

### D-2. Sistem fail akar baca sahaja + had sumber + pemangkasan keupayaan（cadangan, compose repositori tidak dihidupkan secara lalai）【Belum diverifikasi】

- **Mengapa**: sistem fail akar baca sahaja boleh menghalang pengubahan binari semasa operasi; had sumber menghalang satu kontena membebankan hos; pemangkasan capabilities mengurangkan permukaan serangan kernel。
- **Bagaimana**（contoh, gabungkan ke perkhidmatan `broker` compose mengikut keperluan）:

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

- **Cara sahkan**: cubaan menulis ke laluan akar dalam kontena sepatutnya gagal（baca sahaja）; `docker inspect` dapat melihat had sumber。
  **【Belum diverifikasi】**（mesin ini tidak menjalankan Docker）; dan **sistem fail akar baca sahaja perlu memastikan `data_dir` berada pada volum boleh tulis**，jika tidak kernel tidak dapat menulis ke cakera。
- [ ] Sistem fail akar baca sahaja dan had sumber telah dinilai（perhatikan `data_dir` mesti berada pada volum boleh tulis）

---

## E. Had yang diketahui (pada masa ini memang tidak dapat dilakukan, jangan harap)

Yang berikut semuanya **jurang fakta**，sila akui secara eksplisit dalam reka bentuk keselamatan, jangan andaikan ia wujud:

1. **Kata laluan disimpan dan disalin dalam teks biasa**。Medan `password` dalam `meta/state.json` ialah teks biasa（**【Telah diverifikasi】** ujian sebenar dapat melihat
   `"password":"drillpass"`）; fail konfigurasi juga teks biasa。**Tiada** cincang kata laluan（cincang dan backend pengesahan luaran ditinggalkan untuk pemalam pengesahan）。
   → Akibat: **direktori data dan fail sandaran setara dengan bukti kelayakan sensitif**，mesti dilindungi dengan kebenaran fail dan penyulitan。
2. **Tiada log audit**。Penambahan/pemadaman/pengubahsuaian satah pengurusan akan mencetak log biasa（seperti `管理面更新用户 actor=... user=...`），
   tetapi **tiada** aliran audit bebas yang tidak boleh diusik, dan tiada rekod peringkat pematuhan "siapa mengubah apa dan bila"。
3. **Tiada pengesahan luaran seperti LDAP / OAuth2 / JWT**。v1 terbina dalam hanya `PLAIN` / `AMQPLAIN`
   （`auth.Store.Mechanisms()` ujian sebenar hanya mengembalikan dua ini）。
4. **SASL `EXTERNAL` belum dilaksanakan**: walaupun mTLS dikonfigurasi, lapisan protokol **masih melalui pengesahan kata laluan PLAIN**
   （langkah "menggunakan sijil klien untuk bebas kata laluan" tiada）。Sijil hanya pengesahan lapisan pengangkutan。
5. **`remote_access` tidak dapat ditetapkan melalui API**: akaun yang dibina oleh API/CLI pengurusan semuanya membenarkan log masuk jauh（lihat A-4），
   tidak dapat mengehadkan satu akaun agar hanya mesin tempatan。
6. **Satah pengurusan tiada senarai putih sumber bebas / tiada had kadar**: hanya boleh bergantung pada penumpuan permukaan terdedah melalui alamat pengikatan、firewall、TLS。
7. **Tiada kotak pasir pemalam**: pemalam bentuk A berada dalam proses yang sama dengan kernel; pemalam luaran bentuk B walaupun mempunyai pengasingan proses, tetapi
   **satah data melalui proksi sambungan mesin tempatan**、dan pemalam boleh memanggil semantik kernel（tertakluk kepada pengesahan vhost dan kebenaran），**bukan** kotak pasir keselamatan。
8. **Tag satah pengurusan hanya ada tiga peringkat `administrator`/`management`/`monitoring`**，tiada RBAC per-sumber yang lebih halus。

---

## F. Senarai ringkasan

- [ ] A-1 Akaun lalai telah diubah/dipadam 【aliran telah diverifikasi】
- [ ] A-2 Kebenaran minimum akaun perniagaan（regex），tiada tag pentadbir 【laluan 403 telah diverifikasi】
- [ ] A-3 Tag `administrator` hanya diberikan kepada akaun operasi 【skop kebenaran tersirat telah diverifikasi】
- [ ] A-4 Had sumber akaun istimewa telah dinilai（perhatikan akaun yang dibina melalui API secara lalai membuka akses jauh）
- [ ] B-1 Port luaran menghidupkan TLS, salah konfigurasi terus tolak permulaan 【telah diverifikasi】
- [ ] B-2 `min_version` ≥ 1.2 【telah diverifikasi】
- [ ] B-3 Port yang memerlukan mTLS dikonfigurasi dengan `require_and_verify` + `ca_file`
- [ ] B-4 Satah pengurusan menghidupkan TLS
- [ ] C-1 Alamat pengikatan satah pengurusan ditumpukan 【alamat pendengaran telah diverifikasi】
- [ ] C-2 Pemalam protokol yang tidak digunakan dinyahaktifkan
- [ ] D-1 Kontena berjalan sebagai bukan root
- [ ] D-2 Sistem fail akar baca sahaja / had sumber / pemangkasan keupayaan telah dinilai
- [ ] E Had yang diketahui（kata laluan teks biasa、tiada audit、tiada LDAP/OAuth2、SASL EXTERNAL belum dilaksanakan）telah diakui dalam reka bentuk keselamatan
