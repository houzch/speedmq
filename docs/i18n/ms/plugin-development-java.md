# Panduan Pembangunan Pemalam Proses Luaran SwiftMQ —— Java

> **Sasaran**: pembangun yang menulis pemalam proses luaran (sidecar) untuk SwiftMQ dengan Java.
> **Baca dahulu**: [Panduan Pembangunan Pemalam Proses Luaran (sidecar)](plugin-development.md) (model mental / medan konfigurasi / jadual penuh protokol wayar).
> **Projek contoh**: ruang kerja `swiftmq-plugin/java/SidecarPlugin.java` (fail tunggal, hanya pustaka piawai JDK, tanpa Maven/Gradle).

---

## 1. Rupanya semasa berjalan

```
内核进程 ──dial(tcp 本机地址)──► 你的 Java 进程（ServerSocket 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Tiga perkara penting: **proses anda ialah pelayan** (menunggu kernel datang menyambung); **port luaran dibuka oleh kernel** (`protocols[].listeners`);
**`prefix` mesti tidak kosong** (awalan kosong = tidak menyertai pengendusan, sambungan tidak akan diserahkan kepada anda; ujian sebenar akan diputuskan serta-merta, <=8 bait ASCII).

---

## 2. Menjalankannya dalam tiga langkah

### Langkah pertama: konfigurasi

`swiftmqd.json` (**konfigurasi sebenar ialah JSON standard, tidak boleh mengandungi komen**):

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

### Langkah kedua: kompil dan mula

```bash
# JDK 11+ 可直接跑单文件源码（生产上建议先编译成 jar）
javac -encoding UTF-8 -d classes SidecarPlugin.java
java -Dfile.encoding=UTF-8 -cp classes SidecarPlugin --addr 0.0.0.0:19031 --name java-sidecar --session-demo
```

### Langkah ketiga: pengesahan

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'JVhello\n' | nc 127.0.0.1 19032                    # 应回显 JVhello
```

---

## 3. Perkara penting pelaksanaan

### 3.1 Pembingkaian

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（包含 kind 字节）；单帧上限 16 MiB。
```

Java menggunakan `DataInputStream`/`DataOutputStream` paling mudah —— `readInt`/`writeInt` mereka memang **big-endian**:

```java
len = in.readInt();              // 大端 u32
int kind = in.readUnsignedByte();
byte[] payload = new byte[len - 1];
in.readFully(payload);           // 读满，否则抛 EOFException
```

Sisi penulisan mesti **bersiri** (denyutan jantung, respons, blok data datang daripada benang berbeza):

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

Muatan bingkai data = `4 bait nombor strim big-endian + bait mentah`.

### 3.2 Jabat tangan dan denyutan jantung

Kernel **menghantar Hello dahulu**, anda mengembalikan `HelloAck`; kernel mengesahkan `name` dan `api_version` (pada masa ini `v1`) kemudian menyambung.
Selepas itu satu `Ping` setiap 2s, kembalikan `Pong`.

### 3.3 Model konkurensi (versi Java)

| Peranan | Benang |
| --- | --- |
| Gelung baca bingkai | Satu untuk setiap sambungan kernel |
| Pemprosesan strim | Satu untuk setiap strim (berbilang sambungan klien boleh serentak) |
| Pemprosesan panggilan hadapan | Satu untuk setiap panggilan |

**Dalam gelung baca tidak boleh menunggu respons panggilan terbalik secara segerak** (akan berlaku kebuntuan): pemprosesan `session.deliver` mesti dilemparkan ke benang bebas,
kerana di dalamnya masih perlu `session.settle` (satu lagi panggilan terbalik). Contoh berbuat begitu.

### 3.4 Jambatan semantik (mesti sahkan dahulu)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` tidak boleh dilangkau: satah operasi kernel sambungan tiada identiti sebelum pengesahan, terus `session.open` akan ditolak
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

Penghantaran ditolak balik oleh kernel secara **hadapan** (`method = "session.deliver"`), selepas diproses `session.settle`
(`ack` / `requeue` / `reject`; nombor penghantaran unik global, tanpa nombor strim).

---

## 4. Walkthrough kod (projek contoh)

`swiftmq-plugin/java/SidecarPlugin.java` kira-kira 470 baris (termasuk JSON minimum):

| Lokasi | Fungsi |
| --- | --- |
| `Conn.readFrame()` / `Conn.send()` | Baca tulis pembingkaian (`DataInputStream` + kunci tulis) |
| `Conn.serve()` | Gelung baca bingkai dan pengedaran |
| `Conn.call()` | Panggilan terbalik (jadual `pending` + baris gilir menyekat, perlindungan tamat masa) |
| `Conn.handleHello()` | Sahkan dan kembalikan HelloAck |
| `StreamState` | Sisi baca strim (`BlockingQueue`, `STREAM_END` bermaksud tamat) |
| `Conn.handleForwardCall()` / `handleMethod()` | Panggilan hadapan (`session.deliver` + settle, `stats`) |
| `Conn.sessionDemo()` | Pengesahan + isytihar + terbit + guna |
| `Json` (hujung fail) | Baca tulis JSON minimum, hanya untuk menjadikan contoh sifar kebergantungan |

> **Cadangan pengeluaran**: gantikan `Json` dengan pustaka kebiasaan anda (Jackson / Gson), atau gunakan tindanan sedia ada selain `java.net.http` ——
> tidak berkaitan dengan perkara yang ingin disampaikan contoh ini (protokol wayar).

---

## 5. Ujian sebenar (reproduksi setempat)

Windows + JDK 25; kernel dalam Docker (`swiftmq:1.1.01`), pemalam pada hos (`tcp://host.docker.internal:19031`).

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

Diliputi: **jabat tangan -> pengesahan -> jambatan semantik -> penghantaran ditolak balik -> penyelesaian -> gema strim bait**.

---

## 6. Perkara yang perlu diberi perhatian khusus dalam Java

- **`\uXXXX` dalam kod sumber akan diproses oleh pengkompil pada mana-mana kedudukan** (termasuk komen!). Dalam komen contoh ia sengaja ditulis
  "NUL + nama pengguna + NUL + kata laluan" dan bukan terus menulis `\u0000`, jika tidak javac akan melaporkan aksara tidak sah.
- **Kod sumber berbahasa Cina mesti `javac -encoding UTF-8`**, jika tidak di bawah lalai Windows (GBK) ia akan melaporkan "aksara tidak boleh dipetakan bagi pengekodan GBK".
  Semasa berjalan jika ingin mencetak aksara Cina dengan betul, tambah `-Dfile.encoding=UTF-8`.
- **Pemboleh ubah setempat yang ditangkap lambda mesti effectively final**: dalam contoh `name` ditetapkan semula semasa penghuraian parameter,
  justeru dalam lambda menggunakan `opts.name` (medan yang hanya ditetapkan sekali).
- **`DataInputStream` bersifat menyekat**: apabila sambungan terputus ia melontar `EOFException`/`IOException`, berdasarkan itu menutup.
- **base64**: `message.body`, `core.authenticate.response` dalam JSON ialah rentetan base64
  (`Base64.getEncoder()/getDecoder()`).
- **Pustaka piawai JDK tiada JSON**: contoh membawa pelaksanaan minimum sendiri; `Json.parse` menghurai integer sebagai `Long`, titik terapung sebagai `Double`,
  semasa mengambil `id` gunakan `((Number) m.get("id")).longValue()`.

---

## 7. Lanjutan

- Bungkus menjadi jar boleh laksana (`Main-Class: SidecarPlugin`) atau `jlink` masa jalan yang diringkaskan,
  kemudian tukar `spawn` kepada `["java", "-jar", "/opt/swiftmq/sidecar.jar", ...]`.
- Pemalam membawa antara muka pengurusan sendiri: tambah `console_url` dalam konfigurasi (dokumen utama §5.8), halaman "pengurusan pemalam" backend pengurusan akan memaparkan pintu masuk terus.
- Penggunaan bebas: `spawn: []` + `address: "tcp://<nama perkhidmatan>:19031"`, dalam kontena mendengar `0.0.0.0`.
