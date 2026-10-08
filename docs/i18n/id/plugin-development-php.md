# Panduan Pengembangan Plugin Proses Eksternal SpeedMQ — PHP

> **Untuk**: pengembang yang menulis plugin proses eksternal (sidecar) untuk SpeedMQ dengan PHP.
> **Baca dulu**: [Panduan Pengembangan Plugin Proses Eksternal (sidecar)](plugin-development.md) (model mental / field konfigurasi / tabel lengkap protokol kabel).
> **Proyek contoh**: workspace `speedmq-plugin/php/sidecar_plugin.php` (hanya pustaka standar, **tanpa dependensi composer**).

---

## 1. Tampilannya Saat Berjalan

```
内核进程 ──dial(tcp 本机地址)──► 你的 PHP 进程（stream_socket_server 监听本机地址）
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

`speedmqd.json` (**konfigurasi sebenarnya adalah JSON standar dan tidak boleh berisi komentar**):

```json
{
  "plugins": {
    "php-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19021",
        "spawn": ["php", "/opt/speedmq/sidecar_plugin.php", "--addr", "0.0.0.0:19021", "--name", "php-sidecar"],
        "protocols": [
          { "name": "phpecho", "prefix": "PH",
            "listeners": [{ "name": "phpecho", "addr": ":19022" }] }
        ]
      }
    }
  }
}
```

### Langkah 2: Menjalankan

```bash
php sidecar_plugin.php --addr 0.0.0.0:19021 --name php-sidecar --session-demo
```

### Langkah 3: Verifikasi

```bash
php -l sidecar_plugin.php                                  # 先过语法检查
curl -u guest:guest http://127.0.0.1:15672/api/plugins      # state 应为 enabled
printf 'PHhello\n' | nc 127.0.0.1 19022                     # 应回显 PHhello
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

PHP memakai `pack`/`unpack`:

```php
// 写：pack('N', …) 就是大端 u32
fwrite($sock, pack('N', 1 + strlen($payload)) . chr($kind) . $payload);

// 读：先读 5 字节头，再读 payload
$head = read_exact($sock, 5);
$len  = unpack('N', substr($head, 0, 4))[1];
$kind = ord($head[4]);
$payload = read_exact($sock, $len - 1);
```

Payload frame data = `pack('N', $streamId) . byte mentah`.

### 3.2 Handshake dan Heartbeat

Kernel **mengirim Hello lebih dulu**, Anda membalas `HelloAck`; kernel memvalidasi `name` dan `api_version` (saat ini `v1`).
Setelah itu ada `Ping` setiap 2s, dan Anda membalas `Pong`.

### 3.3 Model Konkurensi: Pompa Frame yang Dapat Reentrant (PHP Tidak Memiliki Thread)

PHP CLI bersifat single-thread dan blocking, jadi alih-alih "satu thread per stream", di sini dipakai:

- **Loop baca** (`serve()`) bertanggung jawab atas handshake, heartbeat, membuka stream, echo data, dan menangani forward call;
- **Echo** tidak memerlukan state machine tambahan: begitu menerima `kindData`, langsung tulis kembali sebagai `kindData`;
- **Reverse call** memakai `callAndWait()`: setelah mengirim `kindCall`, ia membaca dan mendistribusikan frame
  sampai membaca balasan **miliknya sendiri** (`reverse=true` dan `id` cocok), lalu kembali.

```php
public function callAndWait(string $method, $params = null)
{
    $id = ++$this->nextId;
    $this->sendJson(KIND_CALL, ['id' => $id, 'method' => $method, 'reverse' => true, 'params' => $params]);
    for (;;) {
        [$kind, $payload] = $this->readFrame();
        if ($kind === KIND_REPLY) {
            $reply = json_decode($payload, true);
            if (!empty($reply['reverse']) && $reply['id'] === $id) {
                if (empty($reply['ok'])) throw new RuntimeException($reply['error'] ?? '调用失败');
                return $reply['data'] ?? null;
            }
            continue;
        }
        $this->dispatchOther($kind, $payload);   // 心跳/数据/正向调用照常处理
    }
}
```

Ini berarti `dispatchOther()` **harus dapat reentrant**: ia bisa dipanggil lagi di dalam sebuah `callAndWait`
(misalnya saat menangani `session.deliver` yang pada gilirannya memerlukan `session.settle`). Persis itulah yang dilakukan contoh.

### 3.4 Jembatan Semantik (Harus Autentikasi Lebih Dulu)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` tidak boleh dilewatkan: permukaan operasi kernel pada koneksi tidak memiliki identitas sebelum autentikasi, dan `session.open` secara langsung akan ditolak
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`).

```php
$ident = $this->callAndWait('core.authenticate', [
    'stream'    => $streamId,
    'mechanism' => 'PLAIN',
    // SASL PLAIN 响应：\x00<user>\x00<password>，字节在 JSON 里走 base64
    'response'  => base64_encode("\x00{$user}\x00{$password}"),
]);
$this->callAndWait('session.open', ['stream' => $streamId, 'vhost' => '/']);
$q = $this->callAndWait('session.declare_queue', ['stream' => $streamId, 'exclusive' => true, 'auto_delete' => true]);
$this->callAndWait('session.consume', ['stream' => $streamId, 'queue' => $q['name'], 'prefetch' => 32]);
```

Pengiriman **didorong kembali secara forward** oleh kernel (`method = "session.deliver"`); setelah diproses, `session.settle`
(`ack` / `requeue` / `reject`; nomor pengiriman unik secara global, tanpa nomor stream).

---

## 4. Penelusuran Kode (Proyek Contoh)

`speedmq-plugin/php/sidecar_plugin.php` sekitar 320 baris:

| Lokasi | Fungsi |
| --- | --- |
| `read_exact()` / `Conn::readFrame()` / `sendFrame()` | Baca/tulis frame |
| `Conn::serve()` | Loop baca utama |
| `Conn::dispatchOther()` | Mendistribusikan frame non-handshake (reentrant) |
| `Conn::callAndWait()` | Reverse call (pompa frame reentrant) |
| `Conn::handleHello()` | Memvalidasi dan membalas HelloAck |
| `Conn::handleForwardCall()` / `handleMethod()` | Forward call (`session.deliver` + settle, `stats`) |
| `Conn::sessionDemo()` | Autentikasi + deklarasi + publikasi + konsumsi |

---

## 5. Uji Empiris (Direproduksi Lokal)

Windows + PHP 7.4; kernel di Docker (`speedmq:1.1.01`), plugin di host (`tcp://host.docker.internal:19021`).

```
php -l sidecar_plugin.php  → No syntax errors detected
plugin=php-sidecar state=enabled          # /api/plugins
echo=[PHhello]                            # 客户端连内核端口 19022 发 "PHhello\n"

--- 插件 stdout ---
sidecar 已启动 plugin=php-sidecar addr=0.0.0.0:19021 version=0.1.0
内核已接入 plugin=php-sidecar peer=127.0.0.1:51006
握手完成 plugin=php-sidecar kernel=1.1.01
认证通过 user=guest
session 演示完成 queue=amq.gen-955afebb58d6b5307ba36e
流已打开 plugin=php-sidecar stream=1 remote=172.17.0.1:59664 local=172.17.0.2:19022
收到投递（session.deliver） queue=amq.gen-955afebb58d6b5307ba36e delivery_id=1 body=hello from php sidecar
```

Cakupan: **handshake → autentikasi → jembatan semantik → dorong-balik pengiriman → penyelesaian → echo aliran byte**.

---

## 6. Catatan Khusus PHP

- **PHP 7.4 tidak memiliki tipe kembalian `mixed`** (baru ada di PHP 8.0): pada contoh, reverse call mengembalikan "tipe apa pun",
  karena itu **tidak ditulis deklarasi tipe kembalian** (memakai komentar `@return mixed`). Menulis `: mixed` pada 7.4 akan langsung menjadi syntax error.
- **Tipe angka JSON**: `json_decode($s, true)` secara default mendekode integer sebagai `int`, dan integer besar bisa menjadi `float`;
  nomor pengiriman aman pada skala contoh ini, tetapi jika nomor Anda sangat besar, pertimbangkan `JSON_BIGINT_AS_STRING`.
- **base64 wajib**: `message.body` dan `core.authenticate.response` adalah string base64 di JSON
  (`base64_encode` / `base64_decode($s, true)`).
- **Jangan pakai `pcntl_fork` untuk konkurensi**: pcntl tidak ada di Windows, dan setelah fork akan merusak asumsi "satu penulis per koneksi";
  single-thread + pompa frame yang reentrant sudah cukup (kecuali Anda perlu komputasi yang sangat berat pada stream, itu lebih cocok ditempatkan di layanan eksternal).
- **`stream_socket_accept` bersifat blocking**: siklus hidup proses dikelola oleh kernel (`spawn`) atau supervisor;
  ingat tangani `fread` yang mengembalikan `''` (EOF) → akhiri koneksi tersebut dan kembali ke accept.
- **Buffering keluaran**: catat log dengan `fwrite(STDOUT, …)` diikuti newline, agar kernel dapat meneruskannya baris per baris ke log kernel.

---

## 7. Tingkat Lanjut

- Plugin dengan antarmuka admin sendiri: tambahkan `console_url` di konfigurasi (dokumen utama §5.8), halaman "Manajemen Plugin" di konsol admin akan menampilkan entri langsung.
- Deployment mandiri: `spawn: []` + `address: "tcp://<nama layanan>:19021"`, di dalam kontainer mendengarkan `0.0.0.0`.
- Bila memerlukan konkurensi yang lebih tinggi, plugin dapat diubah menjadi Swoole / RoadRunner yang berjalan lama, tetapi **protokol kabelnya tidak berubah**; cukup pastikan:
  penulisan frame serial, loop baca tidak memblokir, dan reverse call dicocokkan berdasarkan `id` + `reverse`.
