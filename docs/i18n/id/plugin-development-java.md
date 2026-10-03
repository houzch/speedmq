# Panduan Pengembangan Plugin Proses Eksternal SwiftMQ — Java

> **Untuk**: pengembang yang menulis plugin proses eksternal (sidecar) untuk SwiftMQ dengan Java.
> **Baca dulu**: [Panduan Pengembangan Plugin Proses Eksternal (sidecar)](plugin-development.md) (model mental / field konfigurasi / tabel lengkap protokol kabel).
> **Proyek contoh**: workspace `swiftmq-plugin/java/SidecarPlugin.java` (satu file, hanya pustaka standar JDK, tanpa Maven/Gradle).

---

## 1. Tampilannya Saat Berjalan

```
内核进程 ──dial(tcp 本机地址)──► 你的 Java 进程（ServerSocket 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Tiga poin penting: **proses Anda adalah server** (menunggu kernel tersambung); **port eksternal dibuka oleh kernel** (`protocols[].listeners`);
**`prefix` harus tidak kosong** (prefiks kosong = tidak ikut sniffing, koneksi tidak akan diserahkan ke Anda; terbukti akan langsung diputus, ≤8 byte ASCII).

---

## 2. Menjalankannya dalam Tiga Langkah

### Langkah 1: Konfigurasi

`swiftmqd.json` (**konfigurasi sebenarnya adalah JSON standar dan tidak boleh berisi komentar**):

```json
{
  "plugins": {
    "java-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19031",
        "spawn": ["java", "-cp", "/opt/swiftmq/classes", "SidecarPlugin",
                  "--addr", "0.0.0.0:19031", "--name", "java-sidecar"],
        "protocols": [
          { "name": "javaecho", "prefix": "JV",
            "listeners": [{ "name": "javaecho", "addr": ":19032" }] }
        ]
      }
    }
  }
}
```

### Langkah 2: Kompilasi dan Jalankan

```bash
# JDK 11+ 可直接跑单文件源码（生产上建议先编译成 jar）
javac -encoding UTF-8 -d classes SidecarPlugin.java
java -Dfile.encoding=UTF-8 -cp classes SidecarPlugin --addr 0.0.0.0:19031 --name java-sidecar --session-demo
```

### Langkah 3: Verifikasi

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'JVhello\n' | nc 127.0.0.1 19032                    # 应回显 JVhello
```

---

## 3. Poin Implementasi

### 3.1 Pembingkaian

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（包含 kind 字节）；单帧上限 16 MiB。
```

Java paling praktis dengan `DataInputStream`/`DataOutputStream` —— `readInt`/`writeInt` mereka sudah **big-endian**:

```java
len = in.readInt();              // 大端 u32
int kind = in.readUnsignedByte();
byte[] payload = new byte[len - 1];
in.readFully(payload);           // 读满，否则抛 EOFException
```

Sisi tulis harus **serial** (heartbeat, balasan, dan blok data datang dari thread yang berbeda):

```java
void send(int kind, byte[] payload) throws IOException {
    synchronized (writeLock) {
        out.writeInt(1 + payload.length);
        out.write(kind);
        out.write(payload);
        out.flush();
    }
}
```

Payload frame data = `4 byte nomor stream big-endian + byte mentah`.

### 3.2 Handshake dan Heartbeat

Kernel **mengirim Hello lebih dulu**, Anda membalas `HelloAck`; kernel memvalidasi `name` dan `api_version` (saat ini `v1`) lalu tersambung.
Setelah itu ada `Ping` setiap 2s, dan Anda membalas `Pong`.

### 3.3 Model Konkurensi (Versi Java)

| Peran | Thread |
| --- | --- |
| Loop baca frame | Satu per koneksi kernel |
| Penanganan stream | Satu per stream (beberapa koneksi klien dapat berjalan konkuren) |
| Penanganan forward call | Satu per panggilan |

**Di dalam loop baca tidak boleh menunggu balasan reverse call secara sinkron** (akan deadlock): penanganan `session.deliver` harus dilempar ke thread terpisah,
karena di dalamnya ia masih perlu `session.settle` (satu reverse call lagi). Itulah yang dilakukan contoh.

### 3.4 Jembatan Semantik (Harus Autentikasi Lebih Dulu)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` tidak boleh dilewatkan: permukaan operasi kernel pada koneksi tidak memiliki identitas sebelum autentikasi, dan `session.open` secara langsung akan ditolak
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`).

```java
Map<String, Object> auth = new LinkedHashMap<>();
auth.put("stream", streamId);
auth.put("mechanism", "PLAIN");
// SASL PLAIN 响应：NUL + user + NUL + password，字节在 JSON 里走 base64
auth.put("response", Base64.getEncoder().encodeToString(plainResponse(user, password)));
Object ident = call("core.authenticate", auth, 10_000);

call("session.open", Map.of("stream", streamId, "vhost", "/"), 10_000);
Map<String, Object> q = asMap(call("session.declare_queue",
        Map.of("stream", streamId, "exclusive", true, "auto_delete", true), 10_000));
call("session.consume", Map.of("stream", streamId, "queue", q.get("name"), "prefetch", 32), 10_000);
```

Pengiriman **didorong kembali secara forward** oleh kernel (`method = "session.deliver"`); setelah diproses, `session.settle`
(`ack` / `requeue` / `reject`; nomor pengiriman unik secara global, tanpa nomor stream).

---

## 4. Penelusuran Kode (Proyek Contoh)

`swiftmq-plugin/java/SidecarPlugin.java` sekitar 470 baris (termasuk JSON minimal):

| Lokasi | Fungsi |
| --- | --- |
| `Conn.readFrame()` / `Conn.send()` | Baca/tulis frame (`DataInputStream` + kunci tulis) |
| `Conn.serve()` | Loop baca frame dan pendistribusian |
| `Conn.call()` | Reverse call (tabel `pending` + blocking queue, dengan perlindungan timeout) |
| `Conn.handleHello()` | Memvalidasi dan membalas HelloAck |
| `StreamState` | Sisi baca stream (`BlockingQueue`, `STREAM_END` menandakan akhir) |
| `Conn.handleForwardCall()` / `handleMethod()` | Forward call (`session.deliver` + settle, `stats`) |
| `Conn.sessionDemo()` | Autentikasi + deklarasi + publikasi + konsumsi |
| `Json` (akhir file) | Baca/tulis JSON minimal, hanya agar contoh tanpa dependensi |

> **Saran produksi**: ganti `Json` dengan pustaka yang biasa Anda pakai (Jackson / Gson), atau gunakan stack yang sudah ada selain `java.net.http` ——
> ini tidak berkaitan dengan hal yang dibahas contoh ini (protokol kabel).

---

## 5. Uji Empiris (Direproduksi Lokal)

Windows + JDK 25; kernel di Docker (`swiftmq:1.1.01`), plugin di host (`tcp://host.docker.internal:19031`).

```
javac -encoding UTF-8 -d classes SidecarPlugin.java   → 退出码 0
plugin=java-sidecar state=enabled          # /api/plugins
echo=[JVhello]                             # 客户端连内核端口 19032 发 "JVhello\n"

--- 插件 stdout ---
sidecar 已启动 plugin=java-sidecar addr=0.0.0.0:19031 version=0.1.0
内核已接入 plugin=java-sidecar peer=/127.0.0.1:51039
握手完成 plugin=java-sidecar kernel=1.1.01
认证通过 user=guest
session 演示完成 queue=amq.gen-4ce088d93638eafe830191
流已打开 plugin=java-sidecar stream=1 remote=172.17.0.1:34112 local=172.17.0.2:19032
收到投递（session.deliver） queue=amq.gen-4ce088d93638eafe830191 delivery_id=1 body=hello from java sidecar
```

Cakupan: **handshake → autentikasi → jembatan semantik → dorong-balik pengiriman → penyelesaian → echo aliran byte**.

---

## 6. Catatan Khusus Java

- **`\uXXXX` di dalam sumber diproses oleh kompiler di mana pun** (termasuk komentar!). Komentar contoh sengaja ditulis
  "NUL + nama pengguna + NUL + sandi" alih-alih menulis `\u0000` langsung, jika tidak javac akan melaporkan karakter ilegal.
- **Sumber berbahasa Mandarin harus memakai `javac -encoding UTF-8`**, jika tidak pada default Windows (GBK) akan muncul "unmappable character for encoding GBK".
  Saat runtime, jika ingin mencetak karakter Mandarin dengan benar, tambahkan `-Dfile.encoding=UTF-8`.
- **Variabel lokal yang ditangkap lambda harus effectively final**: pada contoh, `name` ditugaskan ulang saat parsing argumen,
  sehingga di dalam lambda dipakai `opts.name` (field yang hanya ditugaskan sekali).
- **`DataInputStream` bersifat blocking**: saat koneksi terputus ia melempar `EOFException`/`IOException`, yang Anda gunakan untuk merapikan.
- **base64**: `message.body` dan `core.authenticate.response` adalah string base64 di JSON
  (`Base64.getEncoder()/getDecoder()`).
- **Pustaka standar JDK tidak memiliki JSON**: contoh menyertakan implementasi minimal; `Json.parse` mendekode integer sebagai `Long` dan float sebagai `Double`,
  dan saat mengambil `id` ia memakai `((Number) m.get("id")).longValue()`.

---

## 7. Tingkat Lanjut

- Kemas menjadi jar yang dapat dieksekusi (`Main-Class: SidecarPlugin`) atau runtime yang dipangkas dengan `jlink`,
  lalu ubah `spawn` menjadi `["java", "-jar", "/opt/swiftmq/sidecar.jar", …]`.
- Plugin dengan antarmuka admin sendiri: tambahkan `console_url` di konfigurasi (dokumen utama §5.8), halaman "Manajemen Plugin" di konsol admin akan menampilkan entri langsung.
- Deployment mandiri: `spawn: []` + `address: "tcp://<nama layanan>:19031"`, di dalam kontainer mendengarkan `0.0.0.0`.
