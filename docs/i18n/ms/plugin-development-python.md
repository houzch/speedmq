# Panduan Pembangunan Pemalam Proses Luaran SpeedMQ —— Python

> **Sasaran**: pembangun yang menulis pemalam proses luaran (sidecar) untuk SpeedMQ dengan Python.
> **Baca dahulu**: [Panduan Pembangunan Pemalam Proses Luaran (sidecar)](plugin-development.md) —— di sana diterangkan model mental, medan konfigurasi dan jadual penuh protokol wayar;
> dokumen ini hanya membincangkan **cara melaksanakannya dalam Python**, serta langkah dan hasil yang telah diuji pada mesin ini.
> **Projek contoh**: ruang kerja `speedmq-plugin/python/sidecar_plugin.py` (pustaka piawai sahaja, sifar kebergantungan pihak ketiga).

---

## 1. Rupanya semasa berjalan

```
内核进程 ──dial(tcp 本机地址)──► 你的 Python 进程（sidecar.NewServer 的等价物：一个 TCP 服务端）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Tiga perkara penting (mudah tersalah, hafal dahulu):

1. **Proses anda ialah pelayan**: mendengar satu alamat setempat, menunggu kernel datang menyambung (`plugins.<nama>.sidecar.address`).
2. **Port perniagaan luaran dibuka oleh kernel**: klien menyambung ke port kernel, bait diproksikan kepada anda (`protocols[].listeners`).
3. **`prefix` mesti tidak kosong**: kernel menentukan "sambungan ini diserahkan kepada siapa" melalui pengendusan awalan. `prefix` kosong bermaksud **tidak menyertai pengendusan**,
   sambungan pada pendengarnya sendiri juga tidak akan diserahkan kepada anda (ujian sebenar: sambungan akan diputuskan serta-merta). Panjang awalan <= 8 bait, ASCII.

---

## 2. Menjalankannya dalam tiga langkah

### Langkah pertama: mengisytiharkan pemalam dalam konfigurasi

`speedmqd.json` (**konfigurasi sebenar ialah JSON standard, tidak boleh mengandungi komen**):

```json
{
  "plugins": {
    "py-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19001",
        "spawn": ["python", "/opt/speedmq/sidecar_plugin.py", "-addr", "0.0.0.0:19001"],
        "protocols": [
          { "name": "pyecho", "prefix": "PY",
            "listeners": [{ "name": "pyecho", "addr": ":19002" }] }
        ]
      }
    }
  }
}
```

- `address` ialah **alamat kernel pergi menyambung anda** (kernel ialah klien, pemalam ialah pelayan).
- `spawn` kosong = kernel hanya menyambung tidak melancarkan, proses diurus oleh anda sendiri (systemd / supervisor / compose).
- `prefix` **mesti tidak kosong**: bait pertama yang dihantar klien mesti bermula dengannya (kernel menentukan sambungan diserahkan kepada siapa melalui pengendusan awalan).
- `listeners` ialah port luaran, dibuka oleh kernel (klien menyambung ke kernel, bukan anda).
- Semasa penggunaan kontena silang `address` menggunakan **nama perkhidmatan** (seperti `tcp://py-sidecar:19001`), dan pemalam mesti mendengar `0.0.0.0`.

### Langkah kedua: mula

```bash
# 由内核 spawn 时不用手起；手动调试：
python sidecar_plugin.py -addr 0.0.0.0:19001 -name py-sidecar -session-demo
```

### Langkah ketiga: pengesahan

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins | python -m json.tool   # state 应为 enabled
printf 'PYhello\n' | nc 127.0.0.1 19002                                       # 应回显 PYhello
```

---

## 3. Perkara penting pelaksanaan

### 3.1 Pembingkaian (satu-satunya lapisan bait yang wajib anda tulis dengan betul)

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（长度字段**包含** kind 字节）；单帧上限 16 MiB。
```

Muatan bingkai data = `4 bait nombor strim big-endian + bait mentah`; muatan satah kawalan ialah JSON.

```python
def read_frame(sock):
    head = read_exact(sock, 5)
    if len(head) < 5:
        return None
    (length,) = struct.unpack(">I", head[:4])
    payload = read_exact(sock, length - 1)
    return head[4], payload          # (kind, payload)
```

### 3.2 Jabat tangan dan denyutan jantung

Selepas kernel tersambung ia **menghantar Hello dahulu**, anda mesti mengembalikan satu bingkai `HelloAck`; kernel akan mengesahkan
`name == nama pemalam dalam konfigurasi` dan `api_version == APIVersion kernel` (pada masa ini `v1`).
Selepas itu kernel menghantar `Ping` setiap 2s, anda hanya perlu mengembalikan `Pong` (jika tidak mengembalikan ia akan dianggap mati).

### 3.3 Aliran logik

`kindOpen` tiba -> **kembalikan `OpenAck` dahulu**, kemudian mula berkhidmat; `kindData` tiba -> tulis kembali `kindData` seadanya (atau selepas dihurai mengikut protokol anda);
pemprosesan tamat -> hantar `kindClose`. Satu strim = satu sambungan klien.

### 3.4 Panggilan terbalik dan jambatan semantik kernel

Panggilan pemalam -> kernel melalui `kindCall` dengan `"reverse": true`, kernel mengembalikan `kindReply` pada sambungan yang sama.
**Urutan sangat penting**:

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` **tidak boleh dilangkau**: satah operasi kernel sambungan tiada identiti sebelum pengesahan, terus `session.open` akan ditolak
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`). Parameter ialah respons SASL yang dihurai daripada protokol anda:

```python
plain = b"\x00" + user.encode() + b"\x00" + password.encode()
ident = call("core.authenticate", {
    "stream": stream_id, "mechanism": "PLAIN",
    "response": base64.b64encode(plain).decode(),   # 字节在 JSON 里是 base64
})
# 拿到的会话与进程内协议插件完全同一套语义
call("session.open", {"stream": stream_id, "vhost": "/"})
q = call("session.declare_queue", {"stream": stream_id, "exclusive": True, "auto_delete": True})
call("session.publish", {"stream": stream_id, "routing_key": q["name"],
                         "message": {"body": base64.b64encode(b"hi").decode()}})
call("session.consume", {"stream": stream_id, "queue": q["name"], "prefetch": 32})
```

Penghantaran penggunaan ditolak balik oleh kernel secara **hadapan** (`kindCall`, `method = "session.deliver"`), selepas diproses selesaikan dengan
`session.settle` (`ack` / `requeue` / `reject`, nombor penghantaran unik global, tidak memerlukan nombor strim).

### 3.5 Model konkurensi (versi Python)

| Peranan | Benang |
| --- | --- |
| Gelung baca bingkai | Satu untuk setiap sambungan kernel |
| Pemprosesan strim | Satu untuk setiap strim (justeru berbilang sambungan klien boleh serentak) |
| Pemprosesan panggilan hadapan | Satu untuk setiap panggilan |

**Mesti perhatikan**: dalam gelung baca **tidak boleh** menunggu respons panggilan terbalik secara segerak (akan berlaku kebuntuan) —— panggilan hadapan (`session.deliver`)
mesti dilemparkan ke benang bebas untuk diproses, kerana semasa pemprosesan ia mungkin perlu memulakan `session.settle` semula. Penulisan bingkai mesti dikunci untuk pensirilan.

---

## 4. Walkthrough kod (projek contoh)

`speedmq-plugin/python/sidecar_plugin.py` kira-kira 320 baris, fungsi utama:

| Lokasi | Fungsi |
| --- | --- |
| `read_frame` / `Conn.send` | Baca tulis pembingkaian (awalan panjang + kind) |
| `Conn.call` | Panggilan terbalik: nombor -> hantar -> tunggu respons (padan mengikut `reverse=true` dan `id`) |
| `Conn.serve` | Gelung baca bingkai dan pengedaran |
| `Conn._handle_hello` | Sahkan nama pemalam/versi API dan kembalikan HelloAck |
| `Conn._handle_open` / `_serve_stream` / `_echo` | Kitaran hayat strim dan gema |
| `Conn._dispatch` | Kendalikan panggilan hadapan: `session.deliver` (termasuk settle), `stats` |
| `Conn._session_demo` | Pengesahan + isytihar baris gilir + terbit + guna |

---

## 5. Ujian sebenar (reproduksi setempat)

Persekitaran: Windows + Python 3.12; kernel berjalan dalam Docker (`speedmq:1.1.01`), pemalam berjalan pada hos,
kernel menyambungnya menggunakan `tcp://host.docker.internal:19001`.

```
plugin=py-sidecar state=enabled           # /api/plugins
echo=[PYhello]                            # 客户端连内核端口 19002 发 "PYhello\n"

--- 插件 stdout ---
sidecar 已启动 plugin=py-sidecar addr=0.0.0.0:19001 version=0.1.0
内核已接入 plugin=py-sidecar peer=('127.0.0.1', 52864)
握手完成 plugin=py-sidecar kernel=1.1.01
认证通过 user=guest
session 演示完成 queue=amq.gen-9e5d0d695639bad12ce919
收到投递（session.deliver） queue=amq.gen-9e5d0d695639bad12ce919 delivery_id=1 body=hello from python sidecar
流已打开 plugin=py-sidecar stream=1 remote=172.17.0.1:43856 local=172.17.0.2:19002
```

Rantaian yang diliputi: **jabat tangan -> pengesahan -> jambatan semantik (isytihar/terbit/guna) -> penghantaran ditolak balik -> penyelesaian -> gema strim bait**.

---

## 6. Perkara yang perlu diberi perhatian khusus dalam Python

- **Jangan gunakan `time.sleep` untuk menunggu denyutan jantung**: gelung baca bersifat menyekat, cukup bergantung pada Ping kernel untuk mengekalkan sambungan;
  jika anda menetapkan tamat masa baca pada socket, ingat untuk mengendalikan tamat masa sebagai "sambungan tamat" (apabila kernel `kill -9`, socket mungkin tidak ditutup tepat pada masanya).
- **`json.dumps` menambah ruang secara lalai**: contoh menggunakan `separators=(",", ":")` hanya untuk log yang lebih kemas, protokol itu sendiri tidak memerlukannya.
- **Bait ialah base64**: `message.body`, `core.authenticate.response` dalam JSON kedua-duanya ialah rentetan base64,
  jangan lupa `base64.b64encode/decode`.
- **Penulisan bingkai mesti dikunci**: denyutan jantung, respons, blok data datang daripada benang berbeza, penulisan berselang-seli akan mencemarkan keseluruhan sambungan (contoh menggunakan `threading.Lock`).
- Menggunakan `asyncio` juga boleh, tetapi pastikan "penulisan bersiri + gelung baca tidak menyekat", idea sama seperti versi benang.

---

## 7. Lanjutan

- Ingin memberikan pemalam antara muka pengurusan sendiri: tambah `console_url` dalam konfigurasi, halaman "pengurusan pemalam" backend pengurusan akan memaparkan pintu masuk terus
  (lihat dokumen utama §5.8).
- Pemalam menjadikan dirinya perkhidmatan bebas, diurus oleh systemd / K8s: `spawn: []` + `restart: "never"`, dilancarkan dari luar.
- Perlu kewujudan bersama berbilang protokol: pada pendengar yang sama, biarkan berbilang pemalam menggunakan `prefix` berbeza, atau buka port khusus masing-masing.
