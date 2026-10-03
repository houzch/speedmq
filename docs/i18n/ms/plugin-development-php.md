# Panduan Pembangunan Pemalam Proses Luaran SwiftMQ —— PHP

> **Sasaran**: pembangun yang menulis pemalam proses luaran (sidecar) untuk SwiftMQ dengan PHP.
> **Baca dahulu**: [Panduan Pembangunan Pemalam Proses Luaran (sidecar)](plugin-development.md) (model mental / medan konfigurasi / jadual penuh protokol wayar).
> **Projek contoh**: ruang kerja `swiftmq-plugin/php/sidecar_plugin.php` (pustaka piawai sahaja, **tanpa kebergantungan composer**).

---

## 1. Rupanya semasa berjalan

```
内核进程 ──dial(tcp 本机地址)──► 你的 PHP 进程（stream_socket_server 监听本机地址）
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
    "php-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19021",
        "spawn": ["php", "/opt/swiftmq/sidecar_plugin.php", "--addr", "0.0.0.0:19021", "--name", "php-sidecar"],
        "protocols": [
          { "name": "phpecho", "prefix": "PH",
            "listeners": [{ "name": "phpecho", "addr": ":19022" }] }
        ]
      }
    }
  }
}
```

### Langkah kedua: mula

```bash
php sidecar_plugin.php --addr 0.0.0.0:19021 --name php-sidecar --session-demo
```

### Langkah ketiga: pengesahan

```bash
php -l sidecar_plugin.php                                  # 先过语法检查
curl -u guest:guest http://127.0.0.1:15672/api/plugins      # state 应为 enabled
printf 'PHhello\n' | nc 127.0.0.1 19022                     # 应回显 PHhello
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

PHP menggunakan `pack`/`unpack`:

```php
// 写：pack('N', …) 就是大端 u32
fwrite($sock, pack('N', 1 + strlen($payload)) . chr($kind) . $payload);

// 读：先读 5 字节头，再读 payload
$head = read_exact($sock, 5);
$len  = unpack('N', substr($head, 0, 4))[1];
$kind = ord($head[4]);
$payload = read_exact($sock, $len - 1);
```

Muatan bingkai data = `pack('N', $streamId) . bait mentah`.

### 3.2 Jabat tangan dan denyutan jantung

Kernel **menghantar Hello dahulu**, anda mengembalikan `HelloAck`; kernel mengesahkan `name` dan `api_version` (pada masa ini `v1`).
Selepas itu satu `Ping` setiap 2s, kembalikan `Pong`.

### 3.3 Model konkurensi: pam bingkai boleh masuk semula (PHP tiada benang)

PHP CLI bersifat satu benang menyekat, justeru di sini tidak menggunakan "satu benang setiap strim", sebaliknya:

- **Gelung baca** (`serve()`) bertanggungjawab jabat tangan, denyutan jantung, buka strim, gema data, kendalikan panggilan hadapan;
- **Gema** tidak memerlukan mesin keadaan tambahan: menerima `kindData` terus tulis kembali `kindData` seadanya;
- **Panggilan terbalik** menggunakan `callAndWait()`: selepas menghantar `kindCall`, sambil membaca bingkai sambil mengedarkan,
  sehingga membaca respons **miliknya sendiri** (`reverse=true` dan `id` padan) barulah kembali.

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

Ini bermakna `dispatchOther()` **mesti boleh masuk semula**: ia mungkin dipanggil semula dalam satu `callAndWait`
(contohnya semasa mengendalikan `session.deliver` perlu `session.settle` semula). Contoh memang berbuat begitu.

### 3.4 Jambatan semantik (mesti sahkan dahulu)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` tidak boleh dilangkau: satah operasi kernel sambungan tiada identiti sebelum pengesahan, terus `session.open` akan ditolak
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

Penghantaran ditolak balik oleh kernel secara **hadapan** (`method = "session.deliver"`), selepas diproses `session.settle`
(`ack` / `requeue` / `reject`; nombor penghantaran unik global, tanpa nombor strim).

---

## 4. Walkthrough kod (projek contoh)

`swiftmq-plugin/php/sidecar_plugin.php` kira-kira 320 baris:

| Lokasi | Fungsi |
| --- | --- |
| `read_exact()` / `Conn::readFrame()` / `sendFrame()` | Baca tulis pembingkaian |
| `Conn::serve()` | Gelung baca utama |
| `Conn::dispatchOther()` | Edarkan bingkai bukan jabat tangan (boleh masuk semula) |
| `Conn::callAndWait()` | Panggilan terbalik (pam bingkai boleh masuk semula) |
| `Conn::handleHello()` | Sahkan dan kembalikan HelloAck |
| `Conn::handleForwardCall()` / `handleMethod()` | Panggilan hadapan (`session.deliver` + settle, `stats`) |
| `Conn::sessionDemo()` | Pengesahan + isytihar + terbit + guna |

---

## 5. Ujian sebenar (reproduksi setempat)

Windows + PHP 7.4; kernel dalam Docker (`swiftmq:1.1.01`), pemalam pada hos (`tcp://host.docker.internal:19021`).

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

Diliputi: **jabat tangan -> pengesahan -> jambatan semantik -> penghantaran ditolak balik -> penyelesaian -> gema strim bait**.

---

## 6. Perkara yang perlu diberi perhatian khusus dalam PHP

- **PHP 7.4 tiada jenis pulangan `mixed`** (hanya PHP 8.0 ada): dalam contoh panggilan terbalik mengembalikan "sebarang jenis",
  justeru **tidak menulis pengisytiharan jenis pulangan** (gunakan komen `@return mixed`). Menulis `: mixed` pada 7.4 akan terus ralat sintaks.
- **Jenis nombor JSON**: `json_decode($s, true)` secara lalai menghurai integer sebagai `int`, integer besar mungkin menjadi `float`;
  nombor penghantaran dalam skala contoh ini tiada masalah, jika nombor anda sangat besar, pertimbangkan `JSON_BIGINT_AS_STRING`.
- **base64 adalah wajib**: `message.body` dan `core.authenticate.response` dalam JSON ialah rentetan base64
  (`base64_encode` / `base64_decode($s, true)`).
- **Jangan gunakan `pcntl_fork` untuk konkurensi**: Windows tiada pcntl, dan selepas fork ia akan merosakkan andaian "satu sambungan satu penulis";
  satu benang + pam bingkai boleh masuk semula sudah cukup (kecuali anda perlu melakukan pengiraan yang sangat berat pada strim, itu lebih sesuai diletakkan dalam perkhidmatan luar).
- **`stream_socket_accept` bersifat menyekat**: kitaran hayat proses diurus oleh kernel (`spawn`) atau supervisor;
  ingat untuk mengendalikan `fread` mengembalikan `''` (EOF) -> tamatkan sambungan tersebut dan kembali ke accept.
- **Penimbal output**: log menggunakan `fwrite(STDOUT, ...)` dan ikut satu baris baharu, memudahkan kernel memajukannya ke log kernel mengikut baris.

---

## 7. Lanjutan

- Pemalam membawa antara muka pengurusan sendiri: tambah `console_url` dalam konfigurasi (dokumen utama §5.8), halaman "pengurusan pemalam" backend pengurusan akan memaparkan pintu masuk terus.
- Penggunaan bebas: `spawn: []` + `address: "tcp://<nama perkhidmatan>:19021"`, dalam kontena mendengar `0.0.0.0`.
- Apabila memerlukan konkurensi yang lebih tinggi, pemalam boleh diubah menjadi jenis Swoole / RoadRunner yang bermastautin, tetapi **protokol wayar tidak berubah**, hanya perlu memastikan:
  penulisan bingkai bersiri, gelung baca tidak menyekat, panggilan terbalik dipadankan mengikut `id`+`reverse`.
