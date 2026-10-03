# Panduan Pembangunan Pemalam Proses Luaran SwiftMQ —— Node.js

> **Sasaran**: pembangun yang menulis pemalam proses luaran (sidecar) untuk SwiftMQ dengan Node.js.
> **Baca dahulu**: [Panduan Pembangunan Pemalam Proses Luaran (sidecar)](plugin-development.md) (model mental / medan konfigurasi / jadual penuh protokol wayar).
> **Projek contoh**: ruang kerja `swiftmq-plugin/nodejs/index.js` (pustaka piawai Node sahaja, **tanpa kebergantungan npm**).

---

## 1. Rupanya semasa berjalan

```
内核进程 ──dial(tcp 本机地址)──► 你的 Node 进程（net.createServer 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Tiga perkara penting: **proses anda ialah pelayan** (menunggu kernel datang menyambung); **port luaran dibuka oleh kernel** (konfigurasi `protocols[].listeners`);
**`prefix` mesti tidak kosong** (awalan kosong = tidak menyertai pengendusan, sambungan tidak akan diserahkan kepada anda; ujian sebenar akan diputuskan serta-merta, <=8 bait ASCII).

---

## 2. Menjalankannya dalam tiga langkah

### Langkah pertama: konfigurasi

`swiftmqd.json` (**konfigurasi sebenar ialah JSON standard, tidak boleh mengandungi komen**):

```json
{
  "plugins": {
    "node-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19011",
        "spawn": ["node", "/opt/swiftmq/index.js", "--addr", "0.0.0.0:19011", "--name", "node-sidecar"],
        "protocols": [
          { "name": "nodeecho", "prefix": "ND",
            "listeners": [{ "name": "nodeecho", "addr": ":19012" }] }
        ]
      }
    }
  }
}
```

### Langkah kedua: mula

```bash
node index.js --addr 0.0.0.0:19011 --name node-sidecar --session-demo
```

### Langkah ketiga: pengesahan

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'NDhello\n' | nc 127.0.0.1 19012                    # 应回显 NDhello
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

Dalam Node gunakan `Buffer`: kumpulkan bait yang diterima, apabila cukup satu bingkai potong keluar untuk diproses.

```js
drain() {
  while (this.buf.length >= 5) {
    const len = this.buf.readUInt32BE(0)
    if (this.buf.length < 4 + len) return      // 帧还没收全
    const kind = this.buf.readUInt8(4)
    const payload = this.buf.subarray(5, 4 + len)
    this.buf = this.buf.subarray(4 + len)
    this.dispatch(kind, payload)
  }
}
```

Satu strim = satu sambungan klien; muatan bingkai data ialah `4 bait nombor strim big-endian + bait mentah`.

### 3.2 Jabat tangan dan denyutan jantung

Kernel **menghantar Hello dahulu**, anda mengembalikan `HelloAck`; kernel mengesahkan `name` dan `api_version` (pada masa ini `v1`) kemudian menyambung.
Selepas itu satu `Ping` setiap 2s, cukup mengembalikan `Pong` (dikendalikan sambil oleh gelung baca, tidak memerlukan pemasa).

### 3.3 Model tak segerak (versi Node)

Gelung peristiwa satu benang, secara semula jadi mengelakkan masalah "penulisan berselang-seli" —— tetapi perhatikan **jangan biarkan gelung baca await panggilan terbalik**:

```js
case KIND_CALL:                 // 内核 → 插件（如 session.deliver）
  this.handleForwardCall(payload)   // 异步处理，不阻塞读循环
  break
```

`handleForwardCall` ialah `async`: ia mungkin `await call('session.settle', ...)` semula, justeru sama sekali tidak boleh ditulis sebagai menunggu segerak.

### 3.4 Jambatan semantik (mesti sahkan dahulu)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` tidak boleh dilangkau: satah operasi kernel sambungan tiada identiti sebelum pengesahan, terus `session.open` akan ditolak
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`).

```js
const ident = await call('core.authenticate', {
  stream, mechanism: 'PLAIN',
  response: Buffer.concat([Buffer.from(`\x00${user}\x00`), Buffer.from(password)]).toString('base64'),
})
await call('session.open', { stream, vhost: '/' })
const q = await call('session.declare_queue', { stream, exclusive: true, auto_delete: true })
await call('session.publish', { stream, routing_key: q.name,
  message: { body: Buffer.from('hi').toString('base64') } })
await call('session.consume', { stream, queue: q.name, prefetch: 32 })
```

Penghantaran ditolak balik oleh kernel secara **hadapan** (`method = "session.deliver"`), selepas diproses `session.settle`
(`ack` / `requeue` / `reject`; nombor penghantaran unik global, tanpa nombor strim).

---

## 4. Walkthrough kod (projek contoh)

`swiftmq-plugin/nodejs/index.js` kira-kira 330 baris:

| Lokasi | Fungsi |
| --- | --- |
| `u32()` / `Conn.send()` | Baca tulis pembingkaian |
| `Conn.drain()` / `dispatch()` | Hurai dan edarkan mengikut bingkai |
| `Conn.call()` | Panggilan terbalik (`Promise` + jadual `pending`, padan mengikut `reverse=true` dan `id`) |
| `Stream` | Sisi baca strim: `push/end/read` membentuk baris gilir tak segerak |
| `handleHello` | Sahkan dan kembalikan HelloAck |
| `handleForwardCall` / `handleMethod` | Panggilan hadapan (`session.deliver` + settle, `stats`) |
| `sessionDemo` | Pengesahan + isytihar + terbit + guna |

---

## 5. Ujian sebenar (reproduksi setempat)

Windows + Node v24; kernel dalam Docker (`swiftmq:1.1.01`), pemalam pada hos (`tcp://host.docker.internal:19011`).

```
plugin=node-sidecar state=enabled         # /api/plugins
echo=[NDhello]                            # 客户端连内核端口 19012 发 "NDhello\n"

--- 插件 stdout ---
sidecar 已启动 plugin=node-sidecar addr=0.0.0.0:19011 version=0.1.0
内核已接入 plugin=node-sidecar peer=127.0.0.1:50987
握手完成 plugin=node-sidecar kernel=1.1.01
认证通过 user=guest
收到投递（session.deliver） queue=amq.gen-893688b01c62b2ad5763a7 delivery_id=1 body=hello from node sidecar
session 演示完成 queue=amq.gen-893688b01c62b2ad5763a7
流已打开 plugin=node-sidecar stream=1 remote=172.17.0.1:40450 local=172.17.0.2:19012
```

Diliputi: **jabat tangan -> pengesahan -> jambatan semantik -> penghantaran ditolak balik -> penyelesaian -> gema strim bait**.

---

## 6. Perkara yang perlu diberi perhatian khusus dalam Node.js

- **`socket.write` menulis satu bingkai pada satu masa**: contoh menyatukan keseluruhan bingkai menjadi satu `Buffer` kemudian menulis, justeru tidak perlu mengunci tambahan;
  jika anda memecahkan satu bingkai kepada berbilang `write`, anda perlu memastikan urutannya sendiri.
- **Sempadan chunk `stream.on('data')` tidak berkaitan dengan bingkai**: anda mesti mengumpulkan penimbal sendiri (lihat `drain()`).
- **base64**: `message.body`, `core.authenticate.response` dalam JSON ialah rentetan base64
  (`Buffer.from(x).toString('base64')` / `Buffer.from(x, 'base64')`).
- **Jangan `await` dalam `drain()`**: ia ialah fungsi pemotongan bingkai segerak; serahkan pemprosesan tak segerak kepada `handleForwardCall`.
- **ESM vs CJS**: contoh menggunakan CommonJS (`require`) supaya `node index.js` boleh terus dijalankan; menukar kepada ESM hanya perlu ganti `import`.

---

## 7. Lanjutan

- Pemalam membawa antara muka pengurusan sendiri: tambah `console_url` dalam konfigurasi (dokumen utama §5.8), halaman "pengurusan pemalam" backend pengurusan akan memaparkan pintu masuk terus.
- Penggunaan bebas (K8s / systemd): `spawn: []` + `address: "tcp://<nama perkhidmatan>:19011"`, dalam kontena mendengar `0.0.0.0`.
- Penggunaan semula port: berbilang protokol masing-masing diberikan `prefix` berbeza, kernel mengedarkan sambungan kepada pemalam masing-masing mengikut awalan.
