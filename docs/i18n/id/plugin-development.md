# Panduan Pengembangan Plugin Proses Eksternal (sidecar) SwiftMQ

> **Untuk**: pengembang yang tidak ingin mem-fork / mengompilasi ulang kernel, tetapi ingin memperluas kemampuan SwiftMQ dengan **bahasa apa pun**.
> **Cakupan**: dokumen ini hanya membahas satu bentuk plugin —— **plugin proses eksternal** (istilah kernel `sidecar`). Plugin protokol bawaan kernel (AMQP 0-9-1 / MQTT) tidak termasuk cakupan dokumen ini.
> **Cara membaca**: Bagian 1–2 membangun model mental, Bagian 3 tentang menulis kode, dan **Bagian 5 membahas "setelah selesai dikembangkan, bagaimana menyambungkannya agar ikut berjalan dan melayani klien"**;
> **untuk bahasa lain (Python / Node.js / PHP / Java), lihat panduan per bahasa di §4** (masing-masing disertai proyek contoh lengkap yang sudah diuji).
> Kode dalam dokumen ini adalah kerangka minimal yang dapat dijalankan dan bisa langsung disalin sebagai titik awal. Bahasa Mandarin Sederhana adalah bahasa sumbernya.

---

## 1. Apa Itu

Sebuah **proses mandiri** yang mengimplementasikan suatu "protokol" (mem-parsing aliran byte klien) di dalam prosesnya sendiri,
dan kernel meng-host-nya sesuai konfigurasi: **port dibuka oleh kernel, koneksi diproksi oleh kernel**, sedangkan pendaftaran / start-stop / audit / isolasi semuanya memakai kembali mekanisme kernel yang sudah ada.

Pertama, bangun tiga model mental yang benar (titik yang paling mudah keliru):

1. **Proses plugin adalah "layanan lokal"**: ia hanya mendengarkan satu **alamat lokal** (TCP atau unix socket) dan menunggu **kernel tersambung**.
   Arah koneksinya adalah **kernel (klien) → plugin (server)**, dan handshake juga dikirim kernel terlebih dahulu.
2. **Port bisnis eksternal tidak dibuka oleh plugin**: port tersebut dibuat oleh **kernel** berdasarkan `protocols[].listeners` di konfigurasi, lalu dipetakan ke luar untuk klien.
   Klien tersambung ke **port kernel**, dan byte diproksi oleh kernel ke proses plugin. Proses plugin **tidak perlu** membuka port bisnis sendiri.
3. **Semantik bersifat opsional**: plugin boleh hanya "memindahkan byte" (protokol sepenuhnya Anda implementasikan sendiri),
   atau dapat menjangkau semantik kernel (antrean / routing / izin / konfirmasi) melalui **reverse call** ke `session.*`,
   dengan **semantik yang persis sama** seperti plugin protokol bawaan (sehingga vhost, izin, routing, dan perilaku antrean tidak akan bercabang).

| Manfaat | Biaya |
| --- | --- |
| Dapat diperluas tanpa mengubah atau mengompilasi ulang kernel | Ada satu salinan byte lokal tambahan di data plane (proksi kernel, tanpa fd passing, konsisten lintas platform) |
| Implementasi dalam bahasa apa pun (cukup dapat mengimplementasikan protokol kabel) | Setiap reverse call menambah satu RPC lokal (encoding/decoding JSON + salinan) |
| Plugin dapat dirilis / ditingkatkan / di-restart secara mandiri | Sniffing tetap di sisi kernel: hanya dapat dikenali berdasarkan "prefiks" atau "port khusus" |
| Crash hanya memengaruhi plugin tersebut: kernel menandainya `down`, tanpa keluar atau crash | Hanya kemampuan `net.listen` yang benar-benar berlaku; nilai kemampuan lain bersifat cadangan (lihat §7) |

---

## 2. Cara Kerjanya

Pembentukan koneksi (**kernel adalah klien, plugin adalah server**):

```
内核：读配置 plugins.<名>.sidecar
      ├─（可选）spawn 拉起子进程
      └─ dial(address)
插件：sidecar.NewServer 监听 address（Accept）
```

Handshake:

```
内核 ──Hello(kind=1)─────►  插件 Handler.Hello
内核 ◄─HelloAck(kind=2)──   插件（拒绝接入时回 error，内核据此隔离该插件）
```

Mulai melayani:

```
内核：按配置 protocols 注册协议 + 创建对外监听（对外端口由内核打开）

客户端 ─► 内核端口 ─嗅探(prefix)─► 建流 Open ─► 插件 Handler.Open(Stream)
                                                  │
  插件 ─反向调用 session.*（BridgeFromContext）─► 内核（队列 / 路由 / 权限 / 确认）
  内核 ─投递回推 session.deliver ──────────────► 插件 Handler.Call
  插件 ─结算 session.settle ───────────────────► 内核
```

**Siklus hidup** (host `internal/plugin/sidecar` di sisi kernel):

```
Load → Init：spawn(可选) → dial(address) → 握手 → 按配置注册 protocols（此时才起对外监听）
     → Start：起监督协程（断线重连 / 退避）
     → [运行期] 断线 → 状态 down → 退避重连（restart=always）或等待运维（restart=never）
     → Stop（内核退出）：断开连接、回收桥上会话与未结算投递、终止由内核拉起的子进程
```

**Semantik status** (terlihat via `swiftmqctl plugins show`):

| Status | Arti | Tindakan operator |
| --- | --- | --- |
| `enabled` | Sudah tersambung dan melayani | — |
| `failed` | **Gagal sejak awal start** (konfigurasi salah / handshake ditolak / proses tidak dapat dijalankan) | Periksa `RuntimeNote` dan log kernel, perbaiki konfigurasi atau plugin; kernel hanya mencoba ulang setelah restart |
| `down` | **Dulu pernah berjalan, sekarang tidak ada** (proses crash / koneksi terputus) | Jalankan lagi proses plugin; kernel akan memulihkan otomatis sesuai kebijakan `restart` |
| `disabled` | `enabled=false` di konfigurasi, atau dinonaktifkan saat runtime oleh operator | Pulihkan dengan `plugins enable <nama>` |

---

## 3. Pengembangan (Go)

### 3.1 Membuat Proyek

Plugin adalah **Go module mandiri** yang hanya bergantung pada dua paket kontrak publik:

- `github.com/houzch/swiftmq/pkg/sidecar` —— protokol kabel dan implementasi sisi plugin (**wajib**)
- `github.com/houzch/swiftmq/pkg/plugin` —— hanya jika Anda membutuhkan tipe seperti `plugin.Message` / `plugin.Error` (opsional)

```
my-sidecar/
├── go.mod          # module my-sidecar；require github.com/houzch/swiftmq（或 replace 指到本地源码）
├── main.go         # 启动 sidecar.Server
└── handler.go      # 实现 sidecar.Handler
```

```bash
go mod init my-sidecar
go get github.com/houzch/swiftmq@v1.1.03
# 本地联调时可改用 replace 指向源码：
#   go mod edit -replace github.com/houzch/swiftmq=../swiftmq
```

> Saat memakai `replace` untuk debugging bersama, plugin dan kernel harus memakai **sumber yang sama**; jika tidak, versi API (`v1`) memang sama tetapi tipenya bisa berbeda.

### 3.2 Mengimplementasikan `Handler` (Tiga Metode)

Seluruh permukaan bisnis proses plugin adalah `Hello` / `Call` / `Open` dari `sidecar.Handler`.
Handshake, heartbeat, multiplexing, dan chunking semuanya ditangani oleh `pkg/sidecar`; Anda tidak perlu menyentuh frame.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/houzch/swiftmq/pkg/sidecar"
)

const (
	pluginName = "my-sidecar" // 必须与内核配置里的插件名一致
	version    = "0.1.0"
	apiVersion = "v1" // 必须等于 sidecar / plugin 的 APIVersion
	protocol   = "myproto" // 应与配置里 protocols[].name 一致
)

func main() {
	addr := ":19001" // 内核来连的本机地址；可用 -addr 覆盖
	handler := &handler{}

	srv, err := sidecar.NewServer(handler, sidecar.ServerOptions{
		Address: "tcp://" + addr,
		Logger:  stdLogger{log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "my-sidecar 监听失败: %v\n", err)
		os.Exit(1)
	}
	// Address() 能读回实际地址（配置里写 :0 时有用）。
	log.Printf("my-sidecar 已启动 name=%s version=%s addr=%s", pluginName, version, srv.Address())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := srv.Serve(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "my-sidecar 服务退出: %v\n", err)
		os.Exit(1)
	}
}

type handler struct {
	streams atomic.Int64
}

var _ sidecar.Handler = (*handler)(nil)

// Hello 处理握手：返回的 HelloAck 发给内核。返回 error 即拒绝接入（内核会明确隔离该插件）。
func (h *handler) Hello(_ context.Context, hello sidecar.Hello) (sidecar.HelloAck, error) {
	// 内核版本过旧时可以拒绝，避免带着不兼容跑起来。
	if hello.Plugin != pluginName {
		return sidecar.HelloAck{}, fmt.Errorf("插件名不匹配：内核声明 %q，本插件是 %q", hello.Plugin, pluginName)
	}
	if hello.APIVersion != apiVersion {
		return sidecar.HelloAck{}, fmt.Errorf("插件 API 版本不匹配：内核 %q，插件 %q", hello.APIVersion, apiVersion)
	}
	return sidecar.HelloAck{
		Name:         pluginName,
		Version:      version,
		APIVersion:   apiVersion,
		Capabilities: []string{"net.listen"},
		Protocols:    []string{protocol}, // 展示用；真正生效的是内核配置里的 protocols
		Methods:      []string{"stats"},  // 展示用；内核不提供通用调用入口
	}, nil
}

// Call 处理方法调用。约定的系统方法 session.deliver 是"内核把消费投递回推给插件"，
// 必须在实现里处理（见 §3.4）。
func (h *handler) Call(ctx context.Context, method string, params json.RawMessage) (any, error) {
	switch method {
	case sidecar.MethodSessionDeliver:
		return h.handleDeliver(ctx, params)
	case "stats":
		return map[string]any{"streams": h.streams.Load()}, nil
	default:
		// 未知方法必须明确报错：静默成功会让调用方以为生效了。
		return nil, fmt.Errorf("未知方法 %q", method)
	}
}

// Open 处理一条新打开的流：一条流 = 内核侧的一条客户端连接。
// 通常**阻塞处理到流结束**再返回；返回后该流即结束（内核会关闭对应的客户端连接）。
func (h *handler) Open(ctx context.Context, stream *sidecar.Stream, meta sidecar.Open) error {
	h.streams.Add(1)
	// meta.Remote / meta.Local 是两端地址，meta.Peek 是嗅探阶段读到的前缀字节，可用于更细的分支判断。
	return h.serve(stream)
}

// stdLogger 把 pkg/sidecar 的最小日志接口接到标准库日志。
type stdLogger struct{ l *log.Logger }

func (s stdLogger) Info(msg string, args ...any) {
	s.l.Printf("%s %s", msg, strings.TrimRight(fmt.Sprintln(args...), "\n"))
}
func (s stdLogger) Warn(msg string, args ...any) {
	s.l.Printf("WARN %s %s", msg, strings.TrimRight(fmt.Sprintln(args...), "\n"))
}
```

### 3.3 Data Plane: Membaca dan Menulis `Stream`

`sidecar.Stream` mengimplementasikan `io.ReadWriteCloser`, jadi cukup perlakukan sebagai "satu koneksi":

```go
func (h *handler) serve(stream *sidecar.Stream) error {
	buf := make([]byte, 4096)
	for {
		n, err := stream.Read(buf)
		if n > 0 {
			// 你的协议解析在这里；示例先原样回显。
			if _, werr := stream.Write(buf[:n]); werr != nil {
				return werr
			}
		}
		if err != nil {
			return nil // 对端关闭
		}
	}
}
```

Poin penting:

- Setiap koneksi klien = satu stream; plugin dapat langsung mulai mengirim/menerima di dalam `Handler.Open`.
- Pesan besar dipotong otomatis oleh library (setiap frame ≤ 64 KiB), dan **penggunaan memori tidak bergantung pada ukuran pesan**.
- Backpressure: buffer penerima satu stream memiliki batas atas; saat buffer penuh, **goroutine dispatch** koneksi tersebut diblokir (semua stream menunggu bersama) ——
  ini adalah trade-off antara "memori yang dapat diprediksi" dan "pembatasan laju per stream"; lihat §7 untuk detail.

### 3.4 Menggunakan Jembatan Semantik Kernel (`session.*`)

Jika Anda ingin plugin memakai kembali semantik antrean / routing / izin / konfirmasi kernel (alih-alih membangunnya sendiri), gunakan **reverse call**.
Pada sebuah stream, gunakan dengan urutan `session.open` → `session.*` lainnya → (`session.close`):

```go
import (
	"errors"

	"github.com/houzch/swiftmq/pkg/plugin"
	"github.com/houzch/swiftmq/pkg/sidecar"
)

func (h *handler) runDemo(ctx context.Context, stream *sidecar.Stream) error {
	br, ok := sidecar.BridgeFromContext(ctx) // 当前流的内核桥（内核在建流时注入 ctx）
	if !ok {
		return errors.New("ctx 中没有内核桥")
	}
	streamID := stream.ID()

	// 1) 认证：连接的内核操作面在认证前没有身份，会话一定打不开。
	//    response 就是你自己协议里的凭据（这里以 SASL PLAIN 为例）。
	plain := append([]byte("\x00guest\x00"), []byte("guest")...)
	var ident sidecar.AuthIdentityDTO
	if err := br.Call(ctx, sidecar.MethodCoreAuthenticate, sidecar.CoreAuthenticateParams{
		Stream: streamID, Mechanism: "PLAIN", Response: plain,
	}, &ident); err != nil {
		return err
	}

	// 2) 打开会话（内核会做与内置协议插件相同的权限校验）
	if err := br.Call(ctx, sidecar.MethodSessionOpen,
		sidecar.SessionOpenParams{Stream: streamID, VHost: "/"}, nil); err != nil {
		return err
	}

	// 3) 声明一个临时队列
	var q sidecar.QueueInfoResult
	if err := br.Call(ctx, sidecar.MethodSessionDeclareQueue, sidecar.QueueDeclareParams{
		Stream: streamID, Exclusive: true, AutoDelete: true,
	}, &q); err != nil {
		return err
	}

	// 4) 发布一条消息（持久化等待在内核应答前已完成：调用返回即已按 fsync 档位落盘）
	if err := br.Call(ctx, sidecar.MethodSessionPublish, sidecar.PublishParams{
		Stream: streamID, RoutingKey: q.Name,
		Message: sidecar.MessageDTO{Body: []byte("hello")},
	}, nil); err != nil {
		return err
	}

	// 5) 注册消费者；投递随后以正向调用 session.deliver 到达 Handler.Call
	var c sidecar.ConsumeResult
	return br.Call(ctx, sidecar.MethodSessionConsume, sidecar.ConsumeParams{
		Stream: streamID, Queue: q.Name, Prefetch: 32,
	}, &c)
}

// 处理内核回推的投递并结算
func (h *handler) handleDeliver(ctx context.Context, params json.RawMessage) (any, error) {
	var p sidecar.DeliverParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	br, ok := sidecar.BridgeFromContext(ctx)
	if !ok {
		return nil, errors.New("ctx 中没有内核桥")
	}
	// 结算不需要流号：投递编号在整条连接上全局唯一。
	// 动作三选一：Ack（消费完成）/ Requeue（重新入队）/ Reject（丢弃，可能进死信）。
	return nil, br.Call(ctx, sidecar.MethodSessionSettle, sidecar.SettleParams{
		DeliveryID: p.DeliveryID, Action: sidecar.SettleActionAck,
	}, nil)
}
```

**Daftar metode reverse call** (konstanta di `pkg/sidecar/bridge.go` → nama kabel):

| Grup | Nama kabel (konstanta) | Keterangan |
| --- | --- | --- |
| Autentikasi | `core.authenticate`（`MethodCoreAuthenticate`） | **Harus dilakukan lebih dulu**: serahkan kredensial dari protokol Anda ke kernel untuk diverifikasi; parameter `{stream, mechanism, response}`, mengembalikan `{user}` |
| Sesi | `session.open`（`MethodSessionOpen`） | Membuka sesi untuk suatu vhost pada stream; hanya dapat dilakukan **setelah autentikasi** |
| | `session.close`（`MethodSessionClose`） | Melepas sesi pada stream tersebut (membatalkan konsumen, menghapus antrean eksklusif) |
| Exchange | `session.declare_exchange` / `session.delete_exchange` | Menambah/menghapus (deklarasi pasif untuk yang tidak ada → `KindNotFound`) |
| | `session.bind_exchange` / `session.unbind_exchange` | Binding exchange ke exchange |
| Antrean | `session.declare_queue` / `session.delete_queue` | Menambah/menghapus; saat `name` kosong, server yang membuat |
| | `session.bind_queue` / `session.unbind_queue` | Binding antrean ke exchange |
| | `session.purge_queue` | Membersihkan pesan siap (tidak termasuk yang belum dikonfirmasi) |
| Publikasi | `session.publish` | Mengembalikan `{routed, rejected}`; persistensi selesai sebelum balasan |
| Get | `session.get` | Menarik satu pesan secara aktif; `found=false` berarti antrean kosong |
| Konsumsi | `session.consume` / `session.cancel` | Mendaftarkan / membatalkan konsumen |
| Penyelesaian | `session.settle` | Menyelesaikan satu pengiriman (`ack` / `requeue` / `reject`) |
| **Forward** | `session.deliver` | **Kernel → plugin**: mendorong pengiriman kembali (ditangani di `Handler.Call` Anda) |

**Empat ketentuan yang wajib dipatuhi**:

1. **`core.authenticate` lebih dulu**: permukaan operasi kernel pada koneksi tidak memiliki identitas sebelum autentikasi,
   sehingga saat itu `session.open` akan ditolak (`ACCESS_REFUSED - access to vhost '/' refused for user ''`).
   Plugin bertanggung jawab mengambil kredensial dari protokolnya sendiri; logika autentikasi dan tabel pengguna tetap berada di kernel, dan plugin tidak menyentuh basis data sandi.
2. **Lalu `session.open`**: memanggil metode lain tanpa membuka sesi membuat kernel mengembalikan `KindPreconditionFailed` ("stream N belum membuka sesi").
3. **Setiap pengiriman diselesaikan tepat sekali**: pilih salah satu dari `Ack` / `Requeue` / `Reject`.
   Baik `Ack` maupun `Reject` membuang pesan, dan **hanya `Reject` yang masuk ke jalur dead-letter**.
4. **Pengiriman yang belum diselesaikan tidak akan hilang**: saat stream berakhir (klien terputus / `Handler.Open` kembali) atau koneksi plugin terputus,
   kernel menangani semua pengiriman yang belum diselesaikan **sebagai "requeue"**, sehingga pesan tidak tertahan.

**Pemulihan error**: `*plugin.Error` dari kernel tiba melalui jembatan sebagai `*sidecar.RPCError` (field `Kind` / `Text`),
dan dapat dipulihkan menjadi `plugin.Error` yang terkategorikan, alih-alih membuang kategorinya ke dalam sebuah string:

```go
if err := br.Call(ctx, sidecar.MethodSessionPublish, params, nil); err != nil {
	var rpc *sidecar.RPCError
	if errors.As(err, &rpc) {
		return plugin.Errorf(plugin.ErrorKind(rpc.Kind), "%s", rpc.Text)
	}
	return err
}
```

Pemetaan `Kind` umum ke protokol bawaan (untuk membantu Anda memutuskan cara mengembalikan error ke klien):

| `plugin.ErrorKind` | Semantik | Pemetaan AMQP 0-9-1 |
| --- | --- | --- |
| `KindNotFound` | Objek tidak ada | 404 NOT_FOUND (tutup Channel) |
| `KindPreconditionFailed` | Parameter tidak konsisten dengan objek yang sudah ada / sesi belum dibuka | 406 PRECONDITION_FAILED (tutup Channel) |
| `KindAccessRefused` | Izin tidak cukup / nama yang dicadangkan | 403 ACCESS_REFUSED (tutup Channel) |
| `KindResourceLocked` | Sumber daya eksklusif sedang dipakai | 405 RESOURCE_LOCKED (tutup Channel) |
| `KindInvalidPath` | vhost tidak ada | 402 INVALID_PATH (tutup koneksi) |
| `KindNotImplemented` | Kemampuan tidak diimplementasikan | 540 NOT_IMPLEMENTED (tutup koneksi) |
| `KindInternal` | Error internal kernel | 541 INTERNAL_ERROR (tutup koneksi) |

**Batas ketepatan tipe pesan**: tabel properti (`MessageDTO.Properties.Headers`) diteruskan melalui JSON,
sehingga **informasi tipe numerik yang membedakan `int32` / `double` seperti pada field-table AMQP tidak dapat diperoleh**.
Bila ketepatan tipe yang ketat diperlukan, bawa sendiri informasi semacam itu di dalam body pesan (byte mentah).

### 3.5 Status dan Log Sisi Plugin

- **Status plugin eksternal ditentukan oleh "apakah koneksi masih hidup"**, plugin tidak perlu melaporkannya sendiri (`StateReporter` milik plugin bawaan tidak berlaku untuk proses eksternal).
- Log: `sidecar.ServerOptions.Logger` menulis ke stdout/stderr proses plugin;
  **saat dijalankan oleh kernel via `spawn`, keluaran ini diteruskan kernel ke log kernel** (dengan tag `plugin`), sehingga memudahkan pengumpulan terpusat.
- Pada deployment mandiri (bukan spawn), kumpulkan log plugin dengan cara Anda sendiri.

### 3.6 Uji Mandiri Tanpa Kernel

`Handler` adalah interface Go biasa; dalam unit test Anda cukup meng-instansiasinya langsung dan memanggil `Hello` / `Call` / `Open` untuk menguji logika bisnis, tanpa menjalankan jaringan:

```go
func TestHelloRejectsWrongName(t *testing.T) {
	h := &handler{}
	if _, err := h.Hello(context.Background(), sidecar.Hello{Plugin: "other", APIVersion: "v1"}); err == nil {
		t.Fatal("插件名不一致时应当拒绝")
	}
}
```

Verifikasi end-to-end ada di §5.

---

## 4. Mengembangkan dengan Bahasa Lain (Spesifikasi Protokol Kabel)

`pkg/sidecar` adalah kontrak publik tanpa dependensi; protokol kabelnya sendiri sederhana dan dapat diimplementasikan di bahasa apa pun.
Untuk berintegrasi, Anda perlu mengimplementasikan konvensi "tingkat byte" berikut (lihat sumber di `pkg/sidecar/frame.go`, `proto.go`).

> **Tersedia panduan per bahasa dengan proyek contoh lengkap** (semua contoh telah diuji: handshake → autentikasi → jembatan semantik → pengiriman/penyelesaian → aliran byte):
>
> | Bahasa | Panduan | Proyek contoh (workspace `swiftmq-plugin/`) |
> | --- | --- | --- |
> | Python | [plugin-development-python.md](plugin-development-python.md) | `python/sidecar_plugin.py` (hanya pustaka standar) |
> | Node.js | [plugin-development-nodejs.md](plugin-development-nodejs.md) | `nodejs/index.js` (hanya pustaka standar) |
> | PHP | [plugin-development-php.md](plugin-development-php.md) | `php/sidecar_plugin.php` (hanya pustaka standar) |
> | Java | [plugin-development-java.md](plugin-development-java.md) | `java/SidecarPlugin.java` (satu file, hanya JDK) |
>
> Untuk implementasi referensi Go yang lengkap, lihat proyek uji mandiri `swiftmq-test/test/integration/echosidecar/` (proyek ini langsung memakai `pkg/sidecar.Server`,
> sehingga Anda tidak perlu memikirkan detail tingkat byte di bawah).

**Format frame** (seragam untuk semua frame):

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)，即长度字段**包含** kind 字节；单帧上限 16 MiB。
```

**Tipe frame `kind`**:

| kind | Nama | Arah | Payload |
| --- | --- | --- | --- |
| 1 | Hello | kernel → plugin | JSON `Hello` |
| 2 | HelloAck | plugin → kernel | JSON `HelloAck` |
| 3 | Ping | kernel → plugin | kosong |
| 4 | Pong | plugin → kernel | kosong |
| 5 | Call | dua arah | JSON `Call` |
| 6 | Reply | dua arah | JSON `Reply` |
| 7 | Open | kernel → plugin | JSON `Open` |
| 8 | OpenAck | plugin → kernel | JSON `OpenAck` |
| 9 | Data | dua arah | `u32 BE stream` + byte mentah |
| 10 | Close | dua arah | JSON `Close` |

**Struktur JSON control plane** (nama field sesuai `proto.go`).

> Bagian ini adalah **contoh pesan protokol kabel** (beberapa pesan diberikan berurutan dalam satu blok, karena itu dipisahkan dengan `//` sebagai penjelas),
> dan **bukan konfigurasi yang dapat langsung ditulis ke `swiftmqd.json`**.

```jsonc
// Hello（内核 → 插件）
{ "plugin": "my-sidecar", "protocol_version": "v1", "kernel_version": "1.1.01", "api_version": "v1" }
// HelloAck（插件 → 内核）；deny 非空表示拒绝服务
{ "name": "my-sidecar", "version": "0.1.0", "api_version": "v1",
  "capabilities": ["net.listen"], "protocols": ["myproto"], "methods": ["stats"], "deny": "" }
// Call / Reply：方向字段 reverse 区分"内核→插件"（false）与"插件→内核"（true）
{ "id": 1, "method": "session.open", "reverse": false, "params": { } }
{ "id": 1, "reverse": false, "ok": true, "error": "", "data": { } }
// Open（内核 → 插件）／OpenAck（插件 → 内核）／Close（双向）
{ "stream": 7, "remote": "1.2.3.4:5000", "local": "0.0.0.0:19002", "peek": "" }
{ "stream": 7, "ok": true, "error": "" }
{ "stream": 7, "reason": "closed by peer" }
```

**Semantik yang wajib dipatuhi**:

- **Handshake**: kernel mengirim `Hello` lebih dulu, dan plugin harus membalas dengan satu frame `HelloAck`.
  Kernel memvalidasi `HelloAck.name == nama plugin di konfigurasi` dan `HelloAck.api_version == APIVersion kernel`;
  `deny` yang tidak kosong dianggap menolak koneksi (plugin diisolasi).
- **Heartbeat**: secara default kernel mengirim `Ping` setiap 2s, dan plugin harus membalas `Pong` dalam 8s; di sisi plugin, jika tidak ada frame yang diterima dalam 24s, ia boleh menutup koneksi sendiri.
- **Dua ruang ID**: `reverse` pada `Call` membedakan arah, dan masing-masing arah bertambah dari 1 secara independen,
  sehingga `Reply` **harus menyertakan `reverse` kembali**, jika tidak balasannya akan dikirim ke penunggu yang salah.
- **Tanpa base64 di data plane**: data besar seperti body pesan langsung ditempatkan di payload frame `Data` (`stream` + byte mentah), dipotong sesuai kebutuhan.

> Jika Anda memakai Go, gunakan langsung `pkg/sidecar`, dan semua detail di atas tidak perlu Anda implementasikan sendiri.

---

## 5. Menjalankannya Bersama: Integrasi, Melayani Klien, Pengemasan ★

Bagian ini menjawab "bagaimana mengintegrasikan ke SwiftMQ dan bagaimana melayani klien setelah selesai dikembangkan".

### 5.1 Mendeklarasikan Plugin di Konfigurasi

Plugin eksternal **sepenuhnya dikelola oleh konfigurasi**, dan kernel tidak perlu mengubah kode apa pun untuk itu. Tambahkan satu entri pada bagian `plugins` di `swiftmqd.json`
(**konfigurasi sebenarnya adalah JSON standar dan tidak boleh berisi komentar**):

```json
{
  "listeners": {
    "myproto": [{ "addr": ":19002" }]
  },
  "plugins": {
    "my-sidecar": {
      "builtin": false,
      "enabled": true,
      "required": false,
      "sidecar": {
        "address": "tcp://127.0.0.1:19001",
        "spawn": ["/usr/local/bin/my-sidecar", "-addr", "tcp://127.0.0.1:19001"],
        "restart": "always",
        "protocols": [
          {
            "name": "myproto",
            "prefix": "MP",
            "listeners": [{ "name": "myproto", "addr": ":19002" }]
          }
        ]
      }
    }
  }
}
```

Penjelasan per item (daftar field lihat tabel di bawah):

- Nama kunci `plugins.<nama plugin>` **harus sesuai dengan `HelloAck.name` yang dilaporkan plugin**, jika tidak handshake akan ditolak.
- `builtin: false`: mendeklarasikan plugin eksternal secara eksplisit (jika tidak ditulis, plane manajemen akan menampilkannya sebagai bawaan).
- `enabled`: mematikannya = tidak menjalankan proses, tidak membuat listener.
- Dengan `required: true`, kegagalan start akan memblokir start kernel —— jangan mengaktifkannya untuk plugin eksternal.
- `address` adalah **alamat yang dihubungi kernel** (kernel sebagai klien); saat `spawn` tidak kosong, kernel yang menjalankan prosesnya.
- `protocols[].prefix` **harus tidak kosong** (aturan sniffing lihat §5.3).
- `listeners` adalah **port eksternal protokol tersebut, dibuka oleh kernel** (klien tersambung ke kernel).

Daftar field:

| Field | Wajib | Keterangan |
| --- | --- | --- |
| `sidecar.address` | ✅ | Alamat proses plugin: `tcp://host:port` atau `unix:///path` |
| `sidecar.spawn` | ✕ | Baris perintah yang dijalankan kernel atas nama Anda (elemen pertama adalah file eksekutabel); **kosong = kernel hanya menyambung dan tidak menjalankan**, proses Anda kelola sendiri |
| `sidecar.restart` | ✕ | `always` (default, pulih otomatis setelah terputus/crash) atau `never` (hanya menandai `down` dan menunggu intervensi operator) |
| `sidecar.protocols[].name` | ✅ | Nama protokol (unik secara global, ikut menentukan prioritas sniffing) |
| `sidecar.protocols[].prefix` | ✕ | Prefiks sniffing (ASCII); **kosong = tidak ikut sniffing** |
| `sidecar.protocols[].listeners[]` | ✕ | Listener eksternal protokol tersebut (`name` + `addr`), dibuat oleh kernel |
| `sidecar.handshake_timeout_seconds` | ✕ | Menimpa timeout handshake (default 5s) |
| `sidecar.heartbeat_seconds` | ✕ | Menimpa interval heartbeat (default 2s) |

> **Nama plugin dan nama protokol**: keduanya **boleh berbeda** (misalnya plugin `my-sidecar` menyediakan protokol `myproto`).
> Penonaktifan runtime pertama-tama mencari semua protokol yang terdaftar oleh nama plugin tersebut, lalu menutup port protokol-protokol itu, sehingga tidak perlu sengaja memakai nama yang sama.

### 5.2 Tiga Mode Integrasi

| Mode | Konfigurasi | Cocok untuk |
| --- | --- | --- |
| **Host sama + dijalankan kernel (spawn)** | `spawn: [...]`, `address` menunjuk ke alamat yang didengarkannya | Deployment satu mesin, satu kontainer; paling praktis, kernel yang menjalankan dan mereklamasi |
| **Host sama + dikelola sendiri (dial)** | `spawn: []`, `address` menunjuk ke proses yang sudah berjalan | Kelola siklus hidup plugin dengan systemd / supervisor |
| **Lintas host / lintas kontainer (dial, wajib tcp)** | `spawn: []`, `address: "tcp://<nama layanan>:19001"` | Plugin dan kernel di-deploy di kontainer / mesin terpisah |

Pemilihan alamat:

- **Pada mesin yang sama, unix socket direkomendasikan** (`unix:///tmp/my-sidecar.sock`): tidak memakai port TCP dan tidak terpengaruh oleh port host yang terpakai.
  Perhatikan bahwa path socket harus dapat ditulis oleh proses kernel (di dalam kontainer, pengguna non-root `swiftmq`).
- **Lintas kontainer wajib TCP**, dan proses plugin harus mendengarkan `0.0.0.0`, sedangkan `address` memakai **nama layanan di jaringan kontainer**.

> Jangan terbalik arahnya: **alamat yang didengarkan plugin** = `address`; **port yang dibuka untuk klien** = `protocols[].listeners`.

### 5.3 Melayani Klien: Dikenali lewat `prefix`

Saat lapisan akses mendistribusikan koneksi, ia **hanya melihat hasil sniffing**: untuk setiap protokol yang aktif, ia memanggil `Sniff(peek)` sesuai urutan pendaftaran (peek maksimal 8 byte),
dan yang cocok akan mengambil alih koneksi tersebut. Karena itu:

1. **`prefix` harus tidak kosong** (ASCII, ≤ 8 byte). Hanya jika beberapa byte pertama yang dikirim klien sama dengannya, koneksi akan diserahkan ke plugin Anda.
   Contoh: `"prefix": "PY"` → byte pertama klien harus `PY` (prefiks dapat Anda anggap sebagai magic header protokol Anda).
2. **`prefix` kosong berarti tidak ikut sniffing**: koneksi semacam ini **tidak** akan diserahkan ke plugin (terbukti: koneksi pada port listener akan langsung diputus).
   Karena itu `prefix` kosong hanya cocok untuk skenario "ada protokol lain yang meneruskan untuk Anda pada port yang sama"; **jangan** memakainya untuk membuat port khusus.
3. `listeners[].addr` menentukan "di port mana layanan dibuka ke luar", dan `prefix` menentukan "apakah koneksi ini milik Anda" ——
   keduanya harus dipakai bersama: **port khusus pun perlu diberi `prefix` yang tidak kosong** (inilah juga alasan pada contoh konfigurasi kernel,
   `echo-sidecar` menuliskan `prefix: "ECHO"` sekaligus `listeners: [":1885"]`).
4. Sniffing dicocokkan menurut urutan pendaftaran protokol, dan **yang cocok lebih dulu yang berlaku**: saat beberapa plugin hidup bersama, prefiks harus dapat dibedakan (misalnya semuanya diawali byte yang sama akan saling menutupi).

### 5.4 Menimpa Alamat Listener dan TLS

- Alamat listener eksternal dapat diberikan di **dua tempat**: `sidecar.protocols[].listeners[].addr` (default) dan
  `listeners.<nama protokol>` (menimpa secara menyeluruh berdasarkan nama protokol). Jika keduanya ada, `listeners.<nama protokol>` yang berlaku.
- Jika memerlukan TLS, berikan sertifikat di `listeners.<nama protokol>[i].tls` (field sama dengan protokol bawaan).
  Berikut cuplikan `listeners` (**JSON standar, tidak boleh berisi komentar**): item 1 plaintext, item 2 memakai TLS.

```json
"listeners": {
  "myproto": [
    { "addr": ":19002" },
    { "addr": ":19003", "tls": { "cert_file": "/etc/swiftmq/tls/cert.pem",
                                 "key_file":  "/etc/swiftmq/tls/key.pem" } }
  ]
}
```

> TLS diakhiri oleh **kernel** di sisi listener; proses plugin menerima aliran plaintext —— plugin tidak perlu menangani TLS.

### 5.5 Pengemasan: Membuat Plugin Berjalan Bersama Kernel

**Cara A —— Bak ke dalam image yang sama** (direkomendasikan untuk plugin yang "dirilis bersama kernel"): tambahkan satu baris ke tahap runtime `swiftmq/Dockerfile`:

```dockerfile
COPY --from=<构建你的插件的 stage> /out/my-sidecar /usr/local/bin/my-sidecar
# 或直接拷预编译产物：
# COPY bin/my-sidecar /usr/local/bin/my-sidecar
```

Lalu di konfigurasi setel `spawn: ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"]`,
dengan `address` bernilai sama. Kernel akan menjalankannya saat start.

**Cara B —— Mount binary** (tanpa mengubah image, cocok untuk debugging bersama):

```yaml
services:
  swiftmq:
    image: houzch/swiftmq:1.1.03
    command: ["-config", "/etc/swiftmq/swiftmqd.json", "-log-level", "info"]
    volumes:
      - ./configs/swiftmqd.json:/etc/swiftmq/swiftmqd.json:ro
      - ./bin/my-sidecar:/usr/local/bin/my-sidecar:ro   # 插件二进制
    ports:
      - "5672:5672"        # AMQP（内置）
      - "15672:15672"      # 管理面
      - "19002:19002"      # ← 你的协议对外端口（由内核监听）
```

Konfigurasi memakai unix socket (agar tidak memakai port tambahan). Berikut cuplikan `sidecar` di dalam `plugins.my-sidecar`
(**JSON standar, tidak boleh berisi komentar**; `prefix` tetap harus tidak kosong, lihat §5.3):

```json
"sidecar": {
  "address": "unix:///tmp/my-sidecar.sock",
  "spawn": ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"],
  "protocols": [{ "name": "myproto", "prefix": "MP", "listeners": [{ "name": "myproto", "addr": ":19002" }] }]
}
```

**Cara C —— Kontainer terpisah** (plugin dirilis terpisah / diskalakan secara mandiri):

```yaml
services:
  swiftmq:
    image: houzch/swiftmq:1.1.03
    volumes: ["./configs/swiftmqd.json:/etc/swiftmq/swiftmqd.json:ro"]
    ports: ["5672:5672", "15672:15672", "19002:19002"]
    depends_on: [my-sidecar]

  my-sidecar:
    image: my-sidecar:0.1.0
    command: ["-addr", "tcp://0.0.0.0:19001"]   # 对外（对内）监听 0.0.0.0
```

Di konfigurasi, `spawn: []` (kernel hanya menyambung, tidak menjalankan), `address: "tcp://my-sidecar:19001"` (nama layanan compose).

### 5.6 Start dan Verifikasi

```bash
# 1) 内核日志里应能看到握手与接入
docker compose logs swiftmq | grep -E "外部插件已接入|外部插件进程"

# 2) 经 CLI 看插件状态（state=enabled 且 RuntimeNote 为空）
./bin/swiftmqctl plugins list
./bin/swiftmqctl plugins show my-sidecar

# 3) 经管理 API 看（等价入口）
curl -u guest:guest http://127.0.0.1:15672/api/plugins/my-sidecar

# 4) 实测对外服务：你的协议端口由内核监听，直接连它
#    若你的协议是文本行协议，可以这样冒烟：
printf 'hello\n' | nc 127.0.0.1 19002
```

Pada `plugins show`, perhatikan terutama `state` dan `RuntimeNote`:
`failed` menyertakan alasan kegagalan (handshake ditolak / port tidak dapat dibuka…); `down` menyertakan alasan terputus (proses crash / koneksi terputus).

### 5.7 Operasi Runtime

| Operasi | Perintah / API | Efek |
| --- | --- | --- |
| Nonaktifkan runtime | `swiftmqctl plugins disable my-sidecar` atau `PUT /api/plugins/my-sidecar/disable` | **Menutup listener eksternal plugin tersebut** (penonaktifan tingkat kemampuan); kernel dan plugin lain tidak terpengaruh |
| Aktifkan runtime | `swiftmqctl plugins enable my-sidecar` | Membuka kembali listener-nya; jika sebelumnya gagal start akan dicoba sekali lagi |
| Lihat status | `swiftmqctl plugins list/show` | Status + alasan gagal/terputus |
| Kernel keluar | — | Memutus koneksi dengan plugin, mereklamasi sesi jembatan, **menghentikan proses anak yang dijalankan kernel via `spawn`** |

> Penonaktifan runtime hanya menutup "kemampuan" (port listener) dan **tidak** membunuh proses plugin yang dijalankan via `spawn`; reklamasi proses terjadi saat kernel keluar.

### 5.8 Menyediakan Entri di Konsol Admin (Opsional)

Jika plugin menyertakan antarmuka operasi sendiri, tambahkan `console_url` (alamat antarmuka admin; field lainnya lihat §5.1) pada bagian `plugins.<nama plugin>`.
Berikut hanya menampilkan entri `plugins.my-sidecar` (**JSON standar, tidak boleh berisi komentar**; isi bagian `sidecar` sama seperti §5.1):

```json
"plugins": {
  "my-sidecar": {
    "builtin": false,
    "enabled": true,
    "console_url": "http://127.0.0.1:19003/",
    "sidecar": { }
  }
}
```

- Halaman **Manajemen Plugin** di konsol admin (datanya berasal dari field `console_url` pada `GET /api/plugins`) akan menampilkan
  tombol "Buka antarmuka admin" untuk plugin semacam ini, yang **terbuka di tab baru**.
- Jika `console_url` tidak dideklarasikan, tombol tidak tersedia dan tooltip menampilkan "Plugin ini tidak menyediakan antarmuka admin".
- Ini hanyalah **metadata yang ditulis pihak deployment ke dalam konfigurasi**: bukan bagian dari API plugin (`pkg/plugin`), tidak ikut dalam start-stop plugin,
  dan antarmukanya sendiri di-host oleh plugin (bisa di dalam proses plugin, atau layanan mandiri mana pun).

---

## 6. Siklus Hidup dan Matriks Toleransi Kesalahan

| Skenario | Perilaku kernel | Dampak di sisi plugin |
| --- | --- | --- |
| Proses plugin belum start / handshake ditolak | Mencoba ulang koneksi dalam 8s; jika masih gagal, menandai `failed` dan mengisolasinya (tidak memblokir start kernel) | Tidak ada |
| Proses yang dijalankan via `spawn` keluar | Mencatat log; menandai `down`; melakukan backoff dan reconnect / menjalankan ulang sesuai kebijakan `restart` | Proses baru melakukan handshake lagi |
| Proses plugin crash (saat runtime) | Kernel tidak terpengaruh; `down` + backoff reconnect | `pkg/sidecar.Server` akan menutup koneksi tersebut |
| Kernel di-`kill -9` | — | Sisi plugin mereklamasi koneksi sendiri melalui idle timeout (default tanpa frame selama 24s), tanpa meninggalkan zombie |
| Kernel keluar normal | Memanggil `Stop`: memutus koneksi, mereklamasi pengiriman yang belum diselesaikan (sebagai requeue), `Kill` proses anak | Menerima SIGKILL |
| Pengiriman selama koneksi plugin terputus | Pengiriman yang belum diselesaikan selalu **di-requeue** dan tidak akan hilang | — |
| Klien terputus / `Open` kembali | Menutup stream yang bersangkutan, melepas sesi dan konsumen stream tersebut | `Stream.Read` mengembalikan EOF |

---

## 7. Garis Merah dan Batas yang Diketahui

**Garis merah**

1. Plugin hanya boleh bergantung pada `pkg/sidecar` (serta opsional `pkg/plugin`); **tidak boleh** bergantung pada `internal/**` kernel.
2. Nama plugin harus sesuai konfigurasi dan `APIVersion` harus sesuai kernel, jika tidak tidak dapat tersambung (ini mencegah "berjalan diam-diam tetapi tidak berlaku").
3. Saat memakai `session.*`: **`core.authenticate` lebih dulu, lalu `session.open`**, dan setiap pengiriman diselesaikan **tepat sekali**.
4. `protocols[].prefix` harus tidak kosong, jika tidak koneksi tidak akan diserahkan ke plugin (lihat §5.3).
5. Penolakan pada `Hello` harus **secara eksplisit mengembalikan error** (jangan diam) —— jika tidak kernel hanya melihat "koneksi ditutup" dan tidak dapat menemukan penyebabnya.

**Batas yang diketahui**

- **Sniffing berada di sisi kernel**: plugin eksternal tidak dapat mendefinisikan fungsi sniffing sendiri dan hanya dapat mencocokkan lewat `prefix` (ASCII, ≤ 8 byte);
  `prefix` kosong berarti "tidak dapat memperoleh koneksi" (lihat §5.3).
- **Data plane melalui proksi lokal**: tidak ada fd passing (Windows tidak memiliki `SCM_RIGHTS`), menambah satu salinan memori dibandingkan dalam-proses;
  setiap reverse call juga menambah satu RPC lokal.
- **Tipe tabel properti mengalami degradasi**: `Properties.Headers` diteruskan melalui JSON, sehingga pembedaan seperti `int32` / `double` hilang (lihat §3.4).
- **Backpressure satu stream memengaruhi seluruh koneksi**: saat buffer penerima satu stream penuh, goroutine dispatch koneksi tersebut diblokir; pembatasan laju per stream adalah optimasi lanjutan.
- **Kegagalan autentikasi hanya meneruskan teks**: kegagalan autentikasi kernel adalah `*plugin.AuthError` (klasifikasi yang berbeda dari `plugin.ErrorKind`),
  dan hanya teksnya yang tiba di plugin melalui jembatan; plugin harus memetakannya ke kode error protokol sesuai konvensinya sendiri.
- **Hanya kemampuan `net.listen` yang benar-benar berlaku**: `store.read/write`, `http.route`, `cluster.metadata.write`,
  `auth.verify` adalah **slot cadangan**; mendeklarasikannya hanya ikut dalam audit (lihat tampilan tata kelola di §5.6), dan saat ini belum ada titik ekstensi yang sesuai.

---

## 8. FAQ Pemecahan Masalah

| Gejala | Penyebab dan penanganan |
| --- | --- |
| Status `failed`, alasan memuat "nama plugin tidak cocok" | Nama plugin di konfigurasi ≠ `HelloAck.name`; samakan keduanya |
| Status `failed`, alasan memuat "versi API tidak cocok" | `HelloAck.api_version` ≠ `APIVersion` kernel; samakan keduanya |
| Status `failed`, alasan memuat "handshake ditolak" | `Hello` plugin mengembalikan error (`deny`); periksa keluaran plugin yang diteruskan di log kernel |
| Status `failed`, alasan memuat "gagal menyambung ke plugin eksternal" | Proses tidak berjalan / `address` salah / path socket tidak dapat ditulis (di dalam kontainer perhatikan izin pengguna `swiftmq`) |
| Status `down` | Proses plugin crash atau koneksi terputus; `restart=always` akan menyambung ulang otomatis, `never` memerlukan penjalanan manual |
| Port tidak terbuka / klien tidak dapat tersambung | `protocols[].listeners` tidak dikonfigurasi atau alamatnya ditimpa oleh `listeners.<nama protokol>`; periksa kedua tempat |
| Klien tersambung ke port lain lalu langsung terputus | Port tersebut tidak cocok dengan protokol Anda (`prefix` kosong atau prefiks tidak sesuai); berikan `prefix` yang tidak kosong untuk protokol tersebut (lihat §5.3) |
| Muncul `ACCESS_REFUSED - ... for user ''` | **Tidak ada autentikasi** sebelum jembatan semantik; panggil `core.authenticate` sebelum `session.open` |
| Reverse call melaporkan "stream N belum membuka sesi" | Lakukan `core.authenticate` lebih dulu, lalu `session.open`, baru kemudian panggil `session.*` lainnya |
| Tidak menerima pengiriman konsumsi | Pengiriman tiba di `Call` Anda sebagai **forward call** `session.deliver`; pastikan metode itu telah ditangani |
| Plugin di luar kontainer, kernel di dalam, tidak dapat tersambung | Gunakan `address: tcp://host.docker.internal:<port>` (atau letakkan plugin di dalam kontainer juga dan pakai nama layanan); plugin harus mendengarkan `0.0.0.0` |

---

## 9. Referensi (Indeks Sumber)

| Yang ingin dilihat | File |
| --- | --- |
| Protokol kabel dan implementasi kedua sisi (**wajib dibaca untuk pengembangan**) | [`pkg/sidecar/`](../../../pkg/sidecar/): `frame.go` (frame), `proto.go` (pesan), `server.go` (sisi plugin), `client.go` (sisi kernel), `bridge.go` (kontrak `session.*`), `stream.go` (stream) |
| Host sidecar sisi kernel (integrasi/reconnect/proksi/status) | [`internal/plugin/sidecar/sidecar.go`](../../../internal/plugin/sidecar/sidecar.go) |
| Jembatan semantik sisi kernel (`session.*` → `plugin.Session`) | [`internal/plugin/sidecar/bridge.go`](../../../internal/plugin/sidecar/bridge.go) |
| Tipe permukaan operasi sesi kernel (`Message` / `Delivery` / `ErrorKind`) | [`pkg/plugin/session.go`](../../../pkg/plugin/session.go) |
| Listener, sniffing, start-stop runtime per plugin | [`internal/transport/server.go`](../../../internal/transport/server.go) |
| Siklus hidup dan tata kelola plugin (isolasi/status/audit) | [`internal/plugin/manager.go`](../../../internal/plugin/manager.go), [`registry.go`](../../../internal/plugin/registry.go) |
| Item konfigurasi dan contoh (termasuk bagian sidecar) | [`internal/config/config.go`](../../../internal/config/config.go), [`configs/swiftmqd.json`](../../../configs/swiftmqd.json) |
| Perakitan proses (bagaimana sidecar dirakit ke dalam kernel) | [`cmd/swiftmqd/main.go`](../../../cmd/swiftmqd/main.go) |
| Implementasi referensi Go (memakai `pkg/sidecar.Server`, termasuk jembatan `session.*` dan `core.authenticate`) | Proyek uji mandiri `swiftmq-test/test/integration/echosidecar/` |
| **Panduan per bahasa + proyek contoh** | Direktori ini `plugin-development-python.md` / `-nodejs.md` / `-php.md` / `-java.md`; contoh ada di **workspace** `swiftmq-plugin/{python,nodejs,php,java}/` |
