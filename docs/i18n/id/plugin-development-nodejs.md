# Panduan Pengembangan Plugin Proses Eksternal SpeedMQ — Node.js

> **Untuk**: pengembang yang menulis plugin proses eksternal (sidecar) untuk SpeedMQ dengan Node.js.
> **Baca dulu**: [Panduan Pengembangan Plugin Proses Eksternal (sidecar)](plugin-development.md) (model mental / field konfigurasi / tabel lengkap protokol kabel).
> **Proyek contoh**: workspace `speedmq-plugin/nodejs/index.js` (hanya pustaka standar Node, **tanpa dependensi npm**).

---

## 1. Tampilannya Saat Berjalan

```
内核进程 ──dial(tcp 本机地址)──► 你的 Node 进程（net.createServer 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Tiga poin penting: **proses Anda adalah server** (menunggu kernel tersambung); **port eksternal dibuka oleh kernel** (konfigurasi `protocols[].listeners`);
**`prefix` harus tidak kosong** (prefiks kosong = tidak ikut sniffing, koneksi tidak akan diserahkan ke Anda; terbukti akan langsung diputus, ≤8 byte ASCII).

---

## 2. Menjalankannya dalam Tiga Langkah

### Langkah 1: Konfigurasi

`speedmqd.json` (**konfigurasi sebenarnya adalah JSON standar dan tidak boleh berisi komentar**):

```json
{
  "plugins": {
    "node-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19011",
        "spawn": ["node", "/opt/speedmq/index.js", "--addr", "0.0.0.0:19011", "--name", "node-sidecar"],
        "protocols": [
          { "name": "nodeecho", "prefix": "ND",
            "listeners": [{ "name": "nodeecho", "addr": ":19012" }] }
        ]
      }
    }
  }
}
```

### Langkah 2: Menjalankan

```bash
node index.js --addr 0.0.0.0:19011 --name node-sidecar --session-demo
```

### Langkah 3: Verifikasi

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'NDhello\n' | nc 127.0.0.1 19012                    # 应回显 NDhello
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

Di Node, gunakan `Buffer`: akumulasikan byte yang diterima, dan potong satu frame begitu jumlahnya cukup.

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

Satu stream = satu koneksi klien; payload frame data adalah `4 byte nomor stream big-endian + byte mentah`.

### 3.2 Handshake dan Heartbeat

Kernel **mengirim Hello lebih dulu**, Anda membalas `HelloAck`; kernel memvalidasi `name` dan `api_version` (saat ini `v1`) lalu tersambung.
Setelah itu ada `Ping` setiap 2s, dan Anda cukup membalas `Pong` (ditangani sekalian oleh loop baca, tidak perlu timer).

### 3.3 Model Asinkron (Versi Node)

Loop event single-thread secara alami menghindari masalah "penulisan berselang-seling" —— tetapi perhatikan **jangan biarkan loop baca await reverse call**:

```js
case KIND_CALL:                 // 内核 → 插件（如 session.deliver）
  this.handleForwardCall(payload)   // 异步处理，不阻塞读循环
  break
```

`handleForwardCall` bersifat `async`: ia bisa jadi `await call('session.settle', …)` lagi, karena itu sama sekali tidak boleh ditulis sebagai penungguan sinkron.

### 3.4 Jembatan Semantik (Harus Autentikasi Lebih Dulu)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` tidak boleh dilewatkan: permukaan operasi kernel pada koneksi tidak memiliki identitas sebelum autentikasi, dan `session.open` secara langsung akan ditolak
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

Pengiriman **didorong kembali secara forward** oleh kernel (`method = "session.deliver"`); setelah diproses, `session.settle`
(`ack` / `requeue` / `reject`; nomor pengiriman unik secara global, tanpa nomor stream).

---

## 4. Penelusuran Kode (Proyek Contoh)

`speedmq-plugin/nodejs/index.js` sekitar 330 baris:

| Lokasi | Fungsi |
| --- | --- |
| `u32()` / `Conn.send()` | Baca/tulis frame |
| `Conn.drain()` / `dispatch()` | Parsing dan pendistribusian per frame |
| `Conn.call()` | Reverse call (`Promise` + tabel `pending`, dicocokkan dengan `reverse=true` dan `id`) |
| `Stream` | Sisi baca stream: `push/end/read` membentuk antrean async |
| `handleHello` | Memvalidasi dan membalas HelloAck |
| `handleForwardCall` / `handleMethod` | Forward call (`session.deliver` + settle, `stats`) |
| `sessionDemo` | Autentikasi + deklarasi + publikasi + konsumsi |

---

## 5. Uji Empiris (Direproduksi Lokal)

Windows + Node v24; kernel di Docker (`speedmq:1.1.01`), plugin di host (`tcp://host.docker.internal:19011`).

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

Cakupan: **handshake → autentikasi → jembatan semantik → dorong-balik pengiriman → penyelesaian → echo aliran byte**.

---

## 6. Catatan Khusus Node.js

- **`socket.write` menulis satu frame sekaligus**: contoh menyusun seluruh frame menjadi satu `Buffer` lalu menulisnya, sehingga tidak perlu penguncian tambahan;
  jika Anda memecah satu frame menjadi beberapa `write`, Anda harus menjamin urutannya sendiri.
- **Batas chunk dari `stream.on('data')` tidak berkaitan dengan frame**: Anda harus mengakumulasi buffer sendiri (lihat `drain()`).
- **base64**: `message.body` dan `core.authenticate.response` adalah string base64 di JSON
  (`Buffer.from(x).toString('base64')` / `Buffer.from(x, 'base64')`).
- **Jangan `await` di dalam `drain()`**: ia adalah fungsi pemotongan frame yang sinkron; serahkan pemrosesan asinkron ke `handleForwardCall`.
- **ESM vs CJS**: contoh memakai CommonJS (`require`) agar `node index.js` dapat dijalankan langsung; untuk beralih ke ESM cukup ganti dengan `import`.

---

## 7. Tingkat Lanjut

- Plugin dengan antarmuka admin sendiri: tambahkan `console_url` di konfigurasi (dokumen utama §5.8), halaman "Manajemen Plugin" di konsol admin akan menampilkan entri langsung.
- Deployment mandiri (K8s / systemd): `spawn: []` + `address: "tcp://<nama layanan>:19011"`, di dalam kontainer mendengarkan `0.0.0.0`.
- Penggunaan ulang port: berikan `prefix` berbeda untuk masing-masing dari beberapa protokol, dan kernel mendistribusikan koneksi ke plugin masing-masing berdasarkan prefiks.
