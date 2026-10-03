# Panduan Pengembangan Plugin Proses Eksternal SwiftMQ — Python

> **Untuk**: pengembang yang menulis plugin proses eksternal (sidecar) untuk SwiftMQ dengan Python.
> **Baca dulu**: [Panduan Pengembangan Plugin Proses Eksternal (sidecar)](plugin-development.md) —— di sana dibahas model mental, field konfigurasi, dan tabel lengkap protokol kabel;
> dokumen ini hanya membahas **bagaimana menerapkannya di Python**, serta langkah dan hasil yang telah diuji di mesin lokal.
> **Proyek contoh**: workspace `swiftmq-plugin/python/sidecar_plugin.py` (hanya pustaka standar, tanpa dependensi pihak ketiga).

---

## 1. Tampilannya Saat Berjalan

```
内核进程 ──dial(tcp 本机地址)──► 你的 Python 进程（sidecar.NewServer 的等价物：一个 TCP 服务端）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Tiga poin penting (mudah keliru, ingat dulu):

1. **Proses Anda adalah server**: ia mendengarkan satu alamat lokal dan menunggu kernel tersambung (`plugins.<nama>.sidecar.address`).
2. **Port bisnis eksternal dibuka oleh kernel**: klien tersambung ke port kernel, dan byte diproksi ke Anda (`protocols[].listeners`).
3. **`prefix` harus tidak kosong**: kernel memutuskan "koneksi ini diserahkan ke siapa" lewat sniffing prefiks. `prefix` kosong berarti **tidak ikut sniffing**,
   dan koneksi tidak akan diserahkan ke Anda bahkan pada listener-nya sendiri (terbukti: koneksi akan langsung diputus). Panjang prefiks ≤ 8 byte, ASCII.

---

## 2. Menjalankannya dalam Tiga Langkah

### Langkah 1: Mendeklarasikan Plugin di Konfigurasi

`swiftmqd.json` (**konfigurasi sebenarnya adalah JSON standar dan tidak boleh berisi komentar**):

```json
{
  "plugins": {
    "py-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19001",
        "spawn": ["python", "/opt/swiftmq/sidecar_plugin.py", "-addr", "0.0.0.0:19001"],
        "protocols": [
          { "name": "pyecho", "prefix": "PY",
            "listeners": [{ "name": "pyecho", "addr": ":19002" }] }
        ]
      }
    }
  }
}
```

- `address` adalah **alamat yang dihubungi kernel** (kernel klien, plugin server).
- `spawn` kosong = kernel hanya menyambung dan tidak menjalankan, proses Anda kelola sendiri (systemd / supervisor / compose).
- `prefix` **harus tidak kosong**: byte pertama yang dikirim klien harus diawali dengannya (kernel memutuskan siapa yang menerima koneksi lewat sniffing prefiks).
- `listeners` adalah port eksternal, dibuka oleh kernel (klien tersambung ke kernel, bukan ke Anda).
- Untuk deployment lintas kontainer, gunakan **nama layanan** pada `address` (mis. `tcp://py-sidecar:19001`), dan plugin harus mendengarkan `0.0.0.0`.

### Langkah 2: Menjalankan

```bash
# 由内核 spawn 时不用手起；手动调试：
python sidecar_plugin.py -addr 0.0.0.0:19001 -name py-sidecar -session-demo
```

### Langkah 3: Verifikasi

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins | python -m json.tool   # state 应为 enabled
printf 'PYhello\n' | nc 127.0.0.1 19002                                       # 应回显 PYhello
```

---

## 3. Poin Implementasi

### 3.1 Pembingkaian (Satu-satunya Lapisan Byte yang Harus Anda Tulis Sendiri dengan Benar)

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（长度字段**包含** kind 字节）；单帧上限 16 MiB。
```

Payload frame data = `4 byte nomor stream big-endian + byte mentah`; payload control plane adalah JSON.

```python
def read_frame(sock):
    head = read_exact(sock, 5)
    if len(head) < 5:
        return None
    (length,) = struct.unpack(">I", head[:4])
    payload = read_exact(sock, length - 1)
    return head[4], payload          # (kind, payload)
```

### 3.2 Handshake dan Heartbeat

Setelah kernel tersambung, ia **mengirim Hello lebih dulu**, dan Anda harus membalas satu frame `HelloAck`; kernel akan memvalidasi
`name == nama plugin di konfigurasi` dan `api_version == APIVersion kernel` (saat ini `v1`).
Setelah itu kernel mengirim `Ping` setiap 2s, dan Anda cukup membalas `Pong` (tidak membalas akan dianggap mati).

### 3.3 Stream Logis

`kindOpen` tiba → **balas `OpenAck` lebih dulu**, lalu mulai melayani; `kindData` tiba → tulis kembali ke
`kindData` apa adanya (atau setelah diparse sesuai protokol Anda); pemrosesan selesai → kirim `kindClose`. Satu stream = satu koneksi klien.

### 3.4 Reverse Call dan Jembatan Semantik Kernel

Panggilan dari plugin → kernel melalui `kindCall` dengan `"reverse": true`, dan kernel membalas `kindReply` pada koneksi yang sama.
**Urutan itu penting**:

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` **tidak boleh dilewatkan**: permukaan operasi kernel pada koneksi tidak memiliki identitas sebelum autentikasi, dan `session.open` secara langsung akan ditolak
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`). Parameternya adalah respons SASL yang diparse dari protokol Anda:

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

Pengiriman konsumsi **didorong kembali secara forward** oleh kernel (`kindCall`, `method = "session.deliver"`); setelah selesai diproses, selesaikan dengan
`session.settle` (`ack` / `requeue` / `reject`; nomor pengiriman unik secara global dan tidak membutuhkan nomor stream).

### 3.5 Model Konkurensi (Versi Python)

| Peran | Thread |
| --- | --- |
| Loop baca frame | Satu per koneksi kernel |
| Penanganan stream | Satu per stream (sehingga beberapa koneksi klien dapat berjalan konkuren) |
| Penanganan forward call | Satu per panggilan |

**Harus diperhatikan**: di dalam loop baca **tidak boleh** menunggu balasan reverse call secara sinkron (akan deadlock) —— forward call (`session.deliver`)
harus dilempar ke thread terpisah, karena saat pemrosesan ia bisa jadi perlu memulai `session.settle` lagi. Penulisan frame harus diserialisasi dengan lock.

---

## 4. Penelusuran Kode (Proyek Contoh)

`swiftmq-plugin/python/sidecar_plugin.py` sekitar 320 baris, fungsi penting:

| Lokasi | Fungsi |
| --- | --- |
| `read_frame` / `Conn.send` | Baca/tulis frame (prefiks panjang + kind) |
| `Conn.call` | Reverse call: nomor → kirim → tunggu balasan (dicocokkan dengan `reverse=true` dan `id`) |
| `Conn.serve` | Loop baca frame dan pendistribusian |
| `Conn._handle_hello` | Memvalidasi nama plugin/versi API dan membalas HelloAck |
| `Conn._handle_open` / `_serve_stream` / `_echo` | Siklus hidup stream dan echo |
| `Conn._dispatch` | Menangani forward call: `session.deliver` (termasuk settle), `stats` |
| `Conn._session_demo` | Autentikasi + deklarasi antrean + publikasi + konsumsi |

---

## 5. Uji Empiris (Direproduksi Lokal)

Lingkungan: Windows + Python 3.12; kernel berjalan di Docker (`swiftmq:1.1.01`), plugin berjalan di host,
dan kernel menyambung ke sana via `tcp://host.docker.internal:19001`.

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

Jalur yang tercakup: **handshake → autentikasi → jembatan semantik (deklarasi/publikasi/konsumsi) → dorong-balik pengiriman → penyelesaian → echo aliran byte**.

---

## 6. Catatan Khusus Python

- **Jangan pakai `time.sleep` untuk menunggu heartbeat**: loop baca bersifat blocking, cukup andalkan Ping dari kernel untuk menjaga koneksi;
  jika Anda menyetel timeout baca pada socket, ingat perlakukan timeout sebagai "koneksi berakhir" (saat kernel di-`kill -9`, socket mungkin tidak segera tertutup).
- **`json.dumps` secara default menambahkan spasi**: contoh memakai `separators=(",", ":")` hanya agar log lebih rapi; protokolnya sendiri tidak mengharuskannya.
- **Byte adalah base64**: `message.body` dan `core.authenticate.response` di JSON keduanya adalah string base64,
  jangan lupa `base64.b64encode/decode`.
- **Penulisan frame harus dikunci**: heartbeat, balasan, dan blok data datang dari thread yang berbeda, dan penulisan yang berselang-seling akan mengotori seluruh koneksi (contoh memakai `threading.Lock`).
- `asyncio` juga bisa, tetapi Anda harus memastikan "penulisan serial + loop baca tidak memblokir"; pendekatannya sama dengan versi thread.

---

## 7. Tingkat Lanjut

- Ingin plugin punya antarmuka admin sendiri: tambahkan `console_url` di konfigurasi, dan halaman "Manajemen Plugin" di konsol admin akan menampilkan entri langsung
  (lihat dokumen utama §5.8).
- Membuat plugin menjadi layanan mandiri yang dikelola systemd / K8s: `spawn: []` + `restart: "never"`, dijalankan dari luar.
- Perlu beberapa protokol hidup bersama: biarkan beberapa plugin memakai `prefix` berbeda pada listener yang sama, atau buka port khusus untuk masing-masing.
