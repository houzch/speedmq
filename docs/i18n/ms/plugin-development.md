# Panduan Pembangunan Pemalam Proses Luaran (sidecar) SwiftMQ

> **Sasaran**: pembangun yang tidak mahu fork / mengkompil semula kernel tetapi ingin memperluas keupayaan SwiftMQ dengan **sebarang bahasa**.
> **Skop**: dokumen ini hanya membincangkan satu bentuk pemalam —— **pemalam proses luaran** (istilah kernel `sidecar`). Pemalam protokol terbina dalam kernel (AMQP 0-9-1 / MQTT) tidak termasuk dalam skop dokumen ini.
> **Cara membaca**: Bahagian 1-2 membina model mental, Bahagian 3 menulis kod, **Bahagian 5 ialah "selepas siap dibangunkan, cara menyambungkannya untuk berjalan bersama dan menyediakan perkhidmatan kepada klien"**;
> **untuk bahasa lain (Python / Node.js / PHP / Java), sila lihat panduan per bahasa dalam §4** (setiap satu disertakan projek contoh lengkap yang telah diuji sebenar).
> Kod dalam dokumen ini ialah rangka minimum yang boleh dijalankan, boleh terus disalin sebagai titik permulaan. Bahasa Cina Ringkas ialah bahasa sumber.

---

## 1. Apakah Ia

Satu **proses berasingan**, yang melaksanakan sesuatu "protokol" (menghurai strim bait klien) dalam prosesnya sendiri,
kernel mengurusnya mengikut konfigurasi: **port dibuka oleh kernel, sambungan diproksikan oleh kernel**, pendaftaran / mula henti / audit / pengasingan semuanya menggunakan semula mekanisme sedia ada kernel.

Bina dahulu tiga model mental yang betul (bahagian yang paling mudah tersalah):

1. **Proses pemalam ialah "perkhidmatan setempat"**: ia hanya mendengar satu **alamat setempat** (TCP atau unix socket), menunggu **kernel datang menyambung**.
   Arah sambungan ialah **kernel (klien) -> pemalam (pelayan)**, jabat tangan juga dihantar oleh kernel terlebih dahulu.
2. **Port perniagaan luaran tidak dibuka oleh pemalam**: ia dicipta oleh **kernel** mengikut `protocols[].listeners` dalam konfigurasi, dan dipetakan keluar kepada klien.
   Klien menyambung ke **port kernel**, bait diproksikan oleh kernel ke proses pemalam. Proses pemalam **tidak perlu** membuka port perniagaan sendiri.
3. **Semantik adalah pilihan**: pemalam boleh hanya "memindahkan bait" (protokol dilaksanakan sepenuhnya oleh anda sendiri),
   atau boleh mencapai semantik kernel (baris gilir / penghalaan / kebenaran / pengesahan) melalui **panggilan terbalik** `session.*`,
   berkongsi **set semantik yang sama sepenuhnya** dengan pemalam protokol terbina dalam (justeru vhost, kebenaran, penghalaan, tingkah laku baris gilir tidak akan bercabang).

| Faedah | Kos |
| --- | --- |
| Boleh diperluas tanpa mengubah atau mengkompil semula kernel | Satah data mempunyai satu salinan bait setempat tambahan (proksi kernel, tiada penghantaran fd, konsisten merentas platform) |
| Dilaksanakan dalam sebarang bahasa (hanya perlu melaksanakan protokol wayar) | Setiap panggilan terbalik mempunyai satu RPC setempat tambahan (pengekodan/penyahkodan JSON + salinan) |
| Pemalam boleh diterbitkan / ditingkatkan / dimulakan semula secara berasingan | Pengendusan kekal di pihak kernel: hanya boleh dikenali melalui "awalan" atau "port khusus" |
| Ranap hanya menjejaskan pemalam tersebut: kernel menandanya `down`, tidak keluar tidak ranap | Hanya keupayaan `net.listen` benar-benar berkesan, nilai keupayaan lain ialah simpanan (lihat §7) |

---

## 2. Cara Ia Berfungsi

Pembentukan sambungan (**kernel ialah klien, pemalam ialah pelayan**):

```
内核：读配置 plugins.<名>.sidecar
      ├─（可选）spawn 拉起子进程
      └─ dial(address)
插件：sidecar.NewServer 监听 address（Accept）
```

Jabat tangan:

```
内核 ──Hello(kind=1)─────►  插件 Handler.Hello
内核 ◄─HelloAck(kind=2)──   插件（拒绝接入时回 error，内核据此隔离该插件）
```

Mula menyediakan perkhidmatan:

```
内核：按配置 protocols 注册协议 + 创建对外监听（对外端口由内核打开）

客户端 ─► 内核端口 ─嗅探(prefix)─► 建流 Open ─► 插件 Handler.Open(Stream)
                                                  │
  插件 ─反向调用 session.*（BridgeFromContext）─► 内核（队列 / 路由 / 权限 / 确认）
  内核 ─投递回推 session.deliver ──────────────► 插件 Handler.Call
  插件 ─结算 session.settle ───────────────────► 内核
```

**Kitaran hayat** (hos `internal/plugin/sidecar` di pihak kernel):

```
Load → Init：spawn(可选) → dial(address) → 握手 → 按配置注册 protocols（此时才起对外监听）
     → Start：起监督协程（断线重连 / 退避）
     → [运行期] 断线 → 状态 down → 退避重连（restart=always）或等待运维（restart=never）
     → Stop（内核退出）：断开连接、回收桥上会话与未结算投递、终止由内核拉起的子进程
```

**Semantik status** (boleh dilihat melalui `swiftmqctl plugins show`):

| Status | Maksud | Tindakan operasi |
| --- | --- | --- |
| `enabled` | Telah disambungkan dan sedang berkhidmat | — |
| `failed` | **Gagal semasa permulaan** (konfigurasi salah / jabat tangan ditolak / proses tidak dapat dilancarkan) | Lihat `RuntimeNote` dan log kernel, ubah konfigurasi atau baiki pemalam; mulakan semula kernel baharu akan mencuba lagi |
| `down` | **Pernah hidup, kini tiada** (proses ranap / sambungan terputus) | Pergi lancarkan proses pemalam; kernel akan memulihkan secara automatik mengikut strategi `restart` |
| `disabled` | `enabled=false` dalam konfigurasi, atau dihentikan panas oleh operasi | `plugins enable <nama>` untuk memulihkan |

---

## 3. Pembangunan (Go)

### 3.1 Membina projek

Pemalam ialah **modul Go bebas**, hanya bergantung pada dua pakej kontrak luaran:

- `github.com/houzch/swiftmq/pkg/sidecar` —— protokol wayar dan pelaksanaan di pihak pemalam (**wajib**)
- `github.com/houzch/swiftmq/pkg/plugin` —— hanya apabila anda ingin menggunakan jenis seperti `plugin.Message` / `plugin.Error` (pilihan)

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

> Apabila menggunakan `replace` untuk penyepaduan, pemalam dan kernel mesti menggunakan **sumber kod yang sama**, jika tidak walaupun versi API (`v1`) konsisten, jenis mungkin berbeza.

### 3.2 Melaksanakan `Handler` (tiga kaedah)

Seluruh satah perniagaan proses pemalam ialah `Hello` / `Call` / `Open` bagi `sidecar.Handler`.
Jabat tangan, denyutan jantung, pemultipleksan, pembahagian blok semuanya dikendalikan oleh `pkg/sidecar`, anda tidak perlu menyentuh bingkai.

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

### 3.3 Satah data: membaca/menulis `Stream`

`sidecar.Stream` melaksanakan `io.ReadWriteCloser`, boleh terus digunakan sebagai "satu sambungan":

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

Perkara penting:

- Setiap sambungan klien = satu strim; pemalam boleh mula menghantar/menerima serta-merta dalam `Handler.Open`.
- Mesej besar dibahagikan kepada blok secara automatik oleh pustaka (setiap bingkai <= 64 KiB), **penggunaan memori tidak berkaitan dengan saiz mesej**.
- Tekanan balik: penimbal penerimaan satu strim mempunyai had atas, apabila penimbal penuh ia menyekat **goroutine penyebaran** sambungan tersebut (semua strim menunggu bersama) ——
  ini ialah pertukaran antara "memori boleh diramal" dan "had kadar satu strim", lihat §7 untuk butiran.

### 3.4 Menggunakan jambatan semantik kernel (`session.*`)

Jika ingin pemalam menggunakan semula semantik baris gilir / penghalaan / kebenaran / pengesahan kernel (bukannya membina sendiri), gunakan **panggilan terbalik**.
Gunakannya pada strim mengikut urutan `session.open` -> `session.*` yang lain -> (`session.close`):

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

**Senarai kaedah panggilan terbalik** (pemalar dalam `pkg/sidecar/bridge.go` -> nama wayar):

| Kumpulan | Nama wayar (pemalar) | Penerangan |
| --- | --- | --- |
| Pengesahan | `core.authenticate` (`MethodCoreAuthenticate`) | **Mesti dilakukan dahulu**: serahkan bukti kelayakan dalam protokol kepada kernel untuk pengesahan; parameter `{stream, mechanism, response}`, mengembalikan `{user}` |
| Sesi | `session.open` (`MethodSessionOpen`) | Buka sesi sesuatu vhost pada strim; hanya boleh dilakukan **selepas pengesahan** |
| | `session.close` (`MethodSessionClose`) | Lepaskan sesi pada strim tersebut (batalkan pengguna, padam baris gilir eksklusif) |
| Penukar | `session.declare_exchange` / `session.delete_exchange` | Tambah/padam (pengisytiharan pasif yang tidak wujud -> `KindNotFound`) |
| | `session.bind_exchange` / `session.unbind_exchange` | Pengikatan penukar ke penukar |
| Baris gilir | `session.declare_queue` / `session.delete_queue` | Tambah/padam; apabila `name` kosong, pelayan menjana |
| | `session.bind_queue` / `session.unbind_queue` | Pengikatan baris gilir ke penukar |
| | `session.purge_queue` | Kosongkan mesej sedia (tidak termasuk yang belum diakui) |
| Penerbitan | `session.publish` | Mengembalikan `{routed, rejected}`; kegigihan selesai sebelum respons |
| Penarikan | `session.get` | Tarik satu secara aktif; `found=false` bermaksud baris gilir kosong |
| Penggunaan | `session.consume` / `session.cancel` | Daftar / batalkan pengguna |
| Penyelesaian | `session.settle` | Selesaikan satu penghantaran (`ack` / `requeue` / `reject`) |
| **Hadapan** | `session.deliver` | **Kernel -> pemalam**: penghantaran ditolak balik (dikendalikan dalam `Handler.Call` anda) |

**Empat konvensyen yang mesti dipatuhi**:

1. **`core.authenticate` dahulu**: satah operasi kernel sambungan tiada identiti sebelum pengesahan,
   pada masa ini `session.open` akan ditolak (`ACCESS_REFUSED - access to vhost '/' refused for user ''`).
   Pemalam bertanggungjawab mengambil bukti kelayakan daripada protokolnya sendiri; logik pengesahan dan jadual pengguna masih berada dalam kernel, pemalam tidak menyentuh pangkalan kata laluan.
2. **Kemudian `session.open`**: jika sesi belum dibuka dan memanggil kaedah lain, kernel mengembalikan `KindPreconditionFailed` ("strim N belum membuka sesi").
3. **Setiap penghantaran diselesaikan tepat sekali**: pilih satu daripada tiga `Ack` / `Requeue` / `Reject`.
   `Ack` dan `Reject` kedua-duanya membuang mesej, **hanya `Reject` melalui surat mati**.
4. **Penghantaran yang belum diselesaikan tidak akan hilang**: apabila strim berakhir (klien terputus / `Handler.Open` kembali) atau sambungan pemalam terputus,
   kernel mengendalikan semua penghantaran yang belum diselesaikan **sebagai "kembali ke baris gilir"**, mengelakkan mesej terperap.

**Pemulihan ralat**: `*plugin.Error` kernel tiba melalui jambatan sebagai `*sidecar.RPCError` (medan `Kind` / `Text`),
boleh dipulihkan kepada `plugin.Error` berkategori, bukannya membuang kategori dalam rentetan:

```go
if err := br.Call(ctx, sidecar.MethodSessionPublish, params, nil); err != nil {
	var rpc *sidecar.RPCError
	if errors.As(err, &rpc) {
		return plugin.Errorf(plugin.ErrorKind(rpc.Kind), "%s", rpc.Text)
	}
	return err
}
```

Pemetaan `Kind` biasa dengan protokol terbina dalam (untuk anda tentukan cara mengembalikan ralat kepada klien):

| `plugin.ErrorKind` | Semantik | Pemetaan AMQP 0-9-1 |
| --- | --- | --- |
| `KindNotFound` | Objek tidak wujud | 404 NOT_FOUND (tutup Channel) |
| `KindPreconditionFailed` | Parameter tidak konsisten dengan objek sedia ada / sesi belum dibuka | 406 PRECONDITION_FAILED (tutup Channel) |
| `KindAccessRefused` | Kebenaran tidak mencukupi / nama terpelihara | 403 ACCESS_REFUSED (tutup Channel) |
| `KindResourceLocked` | Sumber eksklusif diduduki | 405 RESOURCE_LOCKED (tutup Channel) |
| `KindInvalidPath` | vhost tidak wujud | 402 INVALID_PATH (tutup sambungan) |
| `KindNotImplemented` | Keupayaan belum dilaksanakan | 540 NOT_IMPLEMENTED (tutup sambungan) |
| `KindInternal` | Ralat dalaman kernel | 541 INTERNAL_ERROR (tutup sambungan) |

**Sempadan kesetiaan jenis mesej**: jadual atribut (`MessageDTO.Properties.Headers`) melalui pengantaraan JSON,
seperti AMQP field-table, **maklumat jenis berangka yang membezakan `int32` / `double` tidak dapat diperoleh**.
Apabila kesetiaan jenis yang ketat diperlukan, letakkan maklumat sedemikian ke dalam badan mesej (bait mentah) untuk dibawa sendiri.

### 3.5 Status dan log di pihak pemalam

- **Status** pemalam luaran **ditentukan oleh "sama ada sambungan masih hidup"**, pemalam tidak perlu melaporkan sendiri (`StateReporter` pemalam terbina dalam tidak terpakai untuk proses luaran).
- Log: `sidecar.ServerOptions.Logger` output ke stdout/stderr proses pemalam;
  **apabila dilancarkan oleh `spawn` kernel, output ini akan diteruskan oleh kernel ke log kernel** (dengan tag `plugin`), memudahkan pengumpulan bersatu.
- Semasa penggunaan bebas (bukan spawn), log pemalam dikumpulkan mengikut cara anda sendiri.

### 3.6 Boleh menguji sendiri tanpa menyambung kernel

`Handler` ialah antara muka Go biasa, dalam ujian unit boleh terus dijadikan contoh, memanggil `Hello` / `Call` / `Open` untuk meliputi logik perniagaan, tanpa perlu memulakan rangkaian:

```go
func TestHelloRejectsWrongName(t *testing.T) {
	h := &handler{}
	if _, err := h.Hello(context.Background(), sidecar.Hello{Plugin: "other", APIVersion: "v1"}); err == nil {
		t.Fatal("插件名不一致时应当拒绝")
	}
}
```

Pengesahan hujung ke hujung lihat §5.

---

## 4. Pembangunan dengan bahasa lain (spesifikasi protokol wayar)

`pkg/sidecar` ialah kontrak luaran tanpa kebergantungan; protokol wayar itu sendiri sangat mudah, mana-mana bahasa boleh melaksanakannya.
Untuk bersambung, anda perlu melaksanakan konvensyen "peringkat bait" berikut (sumber kod lihat `pkg/sidecar/frame.go`, `proto.go`).

> **Panduan per bahasa dengan projek contoh lengkap telah disediakan** (contoh semuanya telah diuji lulus jabat tangan -> pengesahan -> jambatan semantik -> penghantaran/penyelesaian -> strim bait):
>
> | Bahasa | Panduan | Projek contoh (ruang kerja `swiftmq-plugin/`) |
> | --- | --- | --- |
> | Python | [plugin-development-python.md](plugin-development-python.md) | `python/sidecar_plugin.py` (pustaka piawai sahaja) |
> | Node.js | [plugin-development-nodejs.md](plugin-development-nodejs.md) | `nodejs/index.js` (pustaka piawai sahaja) |
> | PHP | [plugin-development-php.md](plugin-development-php.md) | `php/sidecar_plugin.php` (pustaka piawai sahaja) |
> | Java | [plugin-development-java.md](plugin-development-java.md) | `java/SidecarPlugin.java` (fail tunggal, JDK sahaja) |
>
> Pelaksanaan rujukan lengkap Go lihat projek ujian bebas `swiftmq-test/test/integration/echosidecar/` (ia terus menggunakan `pkg/sidecar.Server`,
> tidak perlu mengambil berat tentang butiran lapisan bait di bawah).

**Format bingkai** (semua bingkai seragam):

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)，即长度字段**包含** kind 字节；单帧上限 16 MiB。
```

**Jenis bingkai `kind`**:

| kind | Nama | Arah | Muatan |
| --- | --- | --- | --- |
| 1 | Hello | Kernel -> pemalam | JSON `Hello` |
| 2 | HelloAck | Pemalam -> kernel | JSON `HelloAck` |
| 3 | Ping | Kernel -> pemalam | Kosong |
| 4 | Pong | Pemalam -> kernel | Kosong |
| 5 | Call | Dua hala | JSON `Call` |
| 6 | Reply | Dua hala | JSON `Reply` |
| 7 | Open | Kernel -> pemalam | JSON `Open` |
| 8 | OpenAck | Pemalam -> kernel | JSON `OpenAck` |
| 9 | Data | Dua hala | `u32 BE stream` + bait mentah |
| 10 | Close | Dua hala | JSON `Close` |

**Struktur JSON satah kawalan** (nama medan sama dengan `proto.go`).

> Perenggan ini ialah **contoh mesej protokol wayar** (beberapa mesej diberikan mengikut urutan dalam blok yang sama, jadi `//` digunakan untuk memisahkan penerangan),
> **bukan konfigurasi yang boleh terus ditulis ke dalam `swiftmqd.json`**.

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

**Semantik yang mesti dipatuhi**:

- **Jabat tangan**: kernel menghantar `Hello` dahulu, pemalam mesti mengembalikan satu bingkai `HelloAck`.
  Kernel akan mengesahkan `HelloAck.name == nama pemalam dalam konfigurasi` dan `HelloAck.api_version == APIVersion kernel`;
  `deny` yang tidak kosong dianggap sebagai penolakan akses (pemalam diasingkan).
- **Denyutan jantung**: kernel secara lalai menghantar `Ping` setiap 2s, pemalam mesti mengembalikan `Pong` dalam 8s; di pihak pemalam, jika tiada bingkai diterima dalam 24s ia boleh menutup sambungan sendiri.
- **Dua ruang ID**: `reverse` bagi `Call` membezakan arah, kedua-dua arah masing-masing meningkat dari 1,
  justeru `Reply` **mesti membawa balik `reverse`**, jika tidak respons akan dihantar kepada penunggu yang salah.
- **Satah data bukan base64**: data blok besar seperti badan mesej diletakkan terus dalam muatan bingkai `Data` (`stream` + bait mentah), dibahagikan kepada blok mengikut keperluan.

> Jika menggunakan Go, terus gunakan `pkg/sidecar`, butiran di atas tidak perlu dilaksanakan sendiri.

---

## 5. Menjalankannya bersama: penyambungan, perkhidmatan luaran, pembungkusan ★

Bahagian ini menjawab "selepas siap dibangunkan, cara menyambung ke SwiftMQ, cara menyediakan perkhidmatan kepada klien".

### 5.1 Mengisytiharkan pemalam dalam konfigurasi

Pemalam luaran **dikendalikan sepenuhnya oleh konfigurasi**, kernel tidak perlu mengubah sebarang kod untuknya. Tambah satu item dalam segmen `plugins` `swiftmqd.json`
(**konfigurasi sebenar ialah JSON standard, tidak boleh mengandungi komen**):

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

Penjelasan item demi item (senarai medan lihat jadual di bawah):

- Nama kunci `plugins.<nama pemalam>` **mesti konsisten dengan `HelloAck.name` yang dilaporkan sendiri oleh pemalam**, jika tidak jabat tangan ditolak.
- `builtin: false`: diisytiharkan secara eksplisit sebagai pemalam luaran (jika tidak ditulis ia akan dipaparkan sebagai terbina dalam oleh satah pengurusan).
- `enabled`: mematikannya = tidak melancarkan proses, tidak mencipta pendengar.
- Apabila `required: true`, kegagalan permulaan akan menyekat permulaan kernel —— pemalam luaran jangan hidupkan.
- `address` ialah **alamat yang kernel sambung** (kernel ialah klien); apabila `spawn` tidak kosong, proses dilancarkan oleh kernel bagi pihak anda.
- `protocols[].prefix` **mesti tidak kosong** (peraturan pengendusan lihat §5.3).
- `listeners` ialah **port luaran** protokol tersebut, **dibuka oleh kernel** (klien menyambung ke kernel).

Senarai medan:

| Medan | Wajib | Penerangan |
| --- | --- | --- |
| `sidecar.address` | ✅ | Alamat proses pemalam: `tcp://host:port` atau `unix:///path` |
| `sidecar.spawn` | ✕ | Baris arahan permulaan oleh kernel (elemen pertama ialah fail boleh laksana); **kosong = kernel hanya menyambung tidak melancarkan**, proses diurus oleh anda sendiri |
| `sidecar.restart` | ✕ | `always` (lalai, pulih automatik selepas terputus/ranap) atau `never` (hanya tanda `down`, menunggu campur tangan operasi) |
| `sidecar.protocols[].name` | ✅ | Nama protokol (unik global, menyertai keutamaan pengendusan) |
| `sidecar.protocols[].prefix` | ✕ | Awalan pengendusan (ASCII); **kosong = tidak menyertai pengendusan** |
| `sidecar.protocols[].listeners[]` | ✕ | Pendengar luaran protokol tersebut (`name` + `addr`), dicipta oleh kernel |
| `sidecar.handshake_timeout_seconds` | ✕ | Menggantikan tamat masa jabat tangan (lalai 5s) |
| `sidecar.heartbeat_seconds` | ✕ | Menggantikan selang denyutan jantung (lalai 2s) |

> **Nama pemalam dan nama protokol**: kedua-duanya **boleh berbeza** (contohnya pemalam `my-sidecar` menyediakan protokol `myproto`).
> Henti panas akan mencari semua protokol yang didaftarkan mengikut nama pemalam terlebih dahulu, kemudian menutup port protokol tersebut, justeru tidak perlu sengaja mengambil nama yang sama.

### 5.2 Tiga cara penyambungan

| Cara | Konfigurasi | Sesuai untuk |
| --- | --- | --- |
| **Hos sama + kernel melancarkan (spawn)** | `spawn: [...]`, `address` menunjuk ke alamat yang didengarnya | Penggunaan mesin sama, kontena tunggal; paling mudah, kernel bertanggungjawab melancarkan dan mengutip |
| **Hos sama + urus sendiri (dial)** | `spawn: []`, `address` menunjuk ke proses yang sedang berjalan | Menggunakan systemd / supervisor untuk mengurus kitaran hayat pemalam |
| **Hos silang / kontena silang (dial, mesti tcp)** | `spawn: []`, `address: "tcp://<nama perkhidmatan>:19001"` | Pemalam dan kernel digunakan dalam kontena / mesin berasingan |

Pemilihan alamat:

- **Mesin sama disyorkan unix socket** (`unix:///tmp/my-sidecar.sock`): tidak mengambil port TCP, tidak terjejas oleh pendudukan port hos.
  Perhatikan laluan socket mesti boleh ditulis oleh proses kernel (dalam kontena ialah pengguna `swiftmq` bukan root).
- **Kontena silang mesti TCP**, dan proses pemalam mesti mendengar `0.0.0.0`, `address` menggunakan **nama perkhidmatan dalam rangkaian kontena**.

> Jangan terbalikkan arah: **alamat yang didengar pemalam** = `address`; **port yang dibuka kepada klien** = `protocols[].listeners`.

### 5.3 Menyediakan perkhidmatan luaran: dikenali melalui `prefix`

Semasa lapisan penyambungan mengedarkan sambungan ia **hanya melihat hasil pengendusan**: ia menanyakan `Sniff(peek)` kepada setiap protokol yang dihidupkan mengikut urutan pendaftaran (peek paling banyak 8 bait),
yang sepadan mengambil alih sambungan ini. Justeru:

1. **`prefix` mesti tidak kosong** (ASCII, <= 8 bait). Beberapa bait pertama yang dihantar klien sama dengannya, barulah sambungan diserahkan kepada pemalam anda.
   Contoh: `"prefix": "PY"` -> bait pertama klien mesti `PY` (boleh anggap awalan sebagai penanda ajaib protokol anda).
2. **`prefix` kosong bermaksud tidak menyertai pengendusan**: sambungan jenis ini **tidak akan** diserahkan kepada pemalam (ujian sebenar: sambungan pada port pendengar akan diputuskan serta-merta).
   Justeru `prefix` kosong hanya sesuai untuk senario "protokol lain akan memajukan untuk anda pada port yang sama", **jangan** gunakannya untuk port khusus.
3. `listeners[].addr` menentukan "port mana yang dibuka kepada luar", `prefix` menentukan "sama ada sambungan ini milik anda" ——
   kedua-duanya perlu digunakan bersama: **port khusus juga perlu diberikan `prefix` yang tidak kosong** (inilah sebabnya dalam konfigurasi contoh kernel
   `echo-sidecar` menulis kedua-dua `prefix: "ECHO"` dan `listeners: [":1885"]`).
4. Pengendusan dipadankan mengikut urutan pendaftaran protokol, **yang dipadankan dahulu berkesan**: apabila berbilang pemalam wujud bersama, awalan perlu mempunyai daya pembezaan (contohnya jika semuanya bermula dengan bait yang sama ia akan saling menghalang).

### 5.4 Menggantikan alamat pendengar dan TLS

- Alamat pendengar luaran boleh diberikan di **dua tempat**: `sidecar.protocols[].listeners[].addr` (lalai) dan
  `listeners.<nama protokol>` (menggantikan keseluruhan mengikut nama protokol). Apabila kedua-duanya wujud serentak, `listeners.<nama protokol>` diutamakan.
- Apabila TLS diperlukan, berikan sijil dalam `listeners.<nama protokol>[i].tls` (medan sama dengan protokol terbina dalam).
  Berikut ialah serpihan `listeners` (**JSON standard, tidak boleh mengandungi komen**): item pertama teks biasa, item kedua melalui TLS.

```json
"listeners": {
  "myproto": [
    { "addr": ":19002" },
    { "addr": ":19003", "tls": { "cert_file": "/etc/swiftmq/tls/cert.pem",
                                 "key_file":  "/etc/swiftmq/tls/key.pem" } }
  ]
}
```

> TLS ditamatkan oleh **kernel** di sisi pendengar, proses pemalam menerima strim teks biasa —— pemalam tidak perlu mengendalikan TLS.

### 5.5 Pembungkusan: menjalankan pemalam bersama kernel

**Cara A —— bungkus ke dalam imej yang sama** (disyorkan untuk pemalam "diterbitkan bersama kernel"): tambah satu baris dalam peringkat jalanan `swiftmq/Dockerfile`:

```dockerfile
COPY --from=<构建你的插件的 stage> /out/my-sidecar /usr/local/bin/my-sidecar
# 或直接拷预编译产物：
# COPY bin/my-sidecar /usr/local/bin/my-sidecar
```

Kemudian dalam konfigurasi `spawn: ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"]`,
`address` nilai sama. Kernel akan melancarkannya semasa permulaan.

**Cara B —— memasang binari** (tidak mengubah imej, sesuai untuk penyepaduan):

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

Konfigurasi menggunakan unix socket (elakkan mengambil port tambahan). Berikut ialah serpihan `sidecar` dalam `plugins.my-sidecar`
(**JSON standard, tidak boleh mengandungi komen**; `prefix` masih perlu tidak kosong, lihat §5.3):

```json
"sidecar": {
  "address": "unix:///tmp/my-sidecar.sock",
  "spawn": ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"],
  "protocols": [{ "name": "myproto", "prefix": "MP", "listeners": [{ "name": "myproto", "addr": ":19002" }] }]
}
```

**Cara C —— kontena bebas** (pemalam diterbitkan berasingan / skala bebas):

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

Dalam konfigurasi `spawn: []` (kernel hanya menyambung tidak melancarkan), `address: "tcp://my-sidecar:19001"` (nama perkhidmatan compose).

### 5.6 Permulaan dan pengesahan

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

Dalam `plugins show` fokus pada `state` dan `RuntimeNote`:
`failed` akan membawa sebab kegagalan (jabat tangan ditolak / port tidak dapat dilancarkan...); `down` akan membawa sebab terputus (proses ranap / sambungan terputus).

### 5.7 Operasi semasa berjalan

| Operasi | Arahan / antara muka | Kesan |
| --- | --- | --- |
| Henti panas | `swiftmqctl plugins disable my-sidecar` atau `PUT /api/plugins/my-sidecar/disable` | **Menutup pendengar luaran pemalam tersebut** (henti peringkat keupayaan); kernel dan pemalam lain tidak terjejas |
| Mula panas | `swiftmqctl plugins enable my-sidecar` | Buka semula pendengarnya; jika permulaan sebelumnya gagal ia akan mencuba sekali lagi |
| Lihat status | `swiftmqctl plugins list/show` | Status + sebab gagal/terputus |
| Kernel keluar | — | Putuskan sambungan dengan pemalam, kutip sesi pada jambatan, **matikan proses anak yang dilancarkan oleh `spawn` kernel** |

> Henti panas hanya menutup "keupayaan" (port pendengar), **tidak akan** mematikan proses pemalam yang dilancarkan dengan `spawn`; pengutipan proses berlaku semasa kernel keluar.

### 5.8 Menyediakan pintu masuk dalam backend pengurusan (pilihan)

Apabila pemalam mempunyai antara muka operasi sendiri, tambah `console_url` (alamat antara muka pengurusan, medan lain lihat §5.1) dalam segmen `plugins.<nama pemalam>`.
Berikut hanya melukis item `plugins.my-sidecar` (**JSON standard, tidak boleh mengandungi komen**; kandungan segmen `sidecar` sama seperti §5.1):

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

- Halaman **pengurusan pemalam** backend pengurusan (data daripada medan `console_url` `GET /api/plugins`) akan memaparkan
  butang "Buka antara muka pengurusan" untuk pemalam jenis ini, **dibuka dalam tab baharu**.
- Apabila `console_url` tidak diisytiharkan, butang tidak tersedia, petunjuk hover "pemalam ini tidak menyediakan antara muka pengurusan".
- Ia hanyalah **metadata yang ditulis oleh pihak penggunaan dalam konfigurasi**: bukan sebahagian daripada API pemalam (`pkg/plugin`), tidak menyertai mula/henti pemalam,
  antara muka itu sendiri dihoskan oleh pemalam (boleh berada dalam proses pemalam, atau mana-mana perkhidmatan bebas).

---

## 6. Kitaran hayat dan matriks toleransi kesalahan

| Keadaan | Tingkah laku kernel | Kesan di pihak pemalam |
| --- | --- | --- |
| Proses pemalam tidak dimulakan / jabat tangan ditolak | Cuba sambung semula dalam 8s, jika masih gagal tandakan `failed` dan asingkan (tidak menyekat permulaan kernel) | Tiada |
| Proses yang dilancarkan oleh `spawn` keluar | Rekod log; tanda `down`; mengikut strategi `restart` undur sambung semula / lancar semula | Proses baharu menjabat tangan semula |
| Proses pemalam ranap (semasa berjalan) | Kernel tidak terjejas; `down` + undur sambung semula | `pkg/sidecar.Server` akan menutup sambungan tersebut |
| Kernel `kill -9` | — | Di pihak pemalam bergantung pada tamat masa melahu (lalai 24s tanpa bingkai) untuk mengutip sambungan sendiri, tidak meninggalkan zombi |
| Kernel keluar normal | Memanggil `Stop`: putus sambungan, kutip penghantaran yang belum diselesaikan (sebagai kembali ke baris gilir), `Kill` proses anak | Menerima SIGKILL |
| Penghantaran semasa sambungan pemalam terputus | Penghantaran yang belum diselesaikan semuanya **kembali ke baris gilir**, tidak akan hilang | — |
| Klien terputus / `Open` kembali | Tutup strim yang sepadan, lepaskan sesi dan pengguna strim tersebut | `Stream.Read` mengembalikan EOF |

---

## 7. Garis merah dan sempadan yang diketahui

**Garis merah**

1. Pemalam hanya dibenarkan bergantung pada `pkg/sidecar` (serta `pkg/plugin` pilihan); **tidak dibenarkan** bergantung pada `internal/**` kernel.
2. Nama pemalam mesti konsisten dengan konfigurasi, `APIVersion` mesti konsisten dengan kernel, jika tidak tidak dapat disambungkan (ini untuk mencegah "berjalan senyap tetapi tidak berkesan").
3. Semasa menggunakan `session.*`: **`core.authenticate` dahulu, kemudian `session.open`**, setiap penghantaran **diselesaikan tepat sekali**.
4. `protocols[].prefix` mesti tidak kosong, jika tidak sambungan tidak akan diserahkan kepada pemalam (lihat §5.3).
5. Penolakan dalam `Hello` mesti **mengembalikan ralat secara eksplisit** (jangan senyap) —— jika tidak kernel hanya dapat melihat "sambungan ditutup", tidak dapat mengenal pasti sebab.

**Sempadan yang diketahui**

- **Pengendusan di pihak kernel**: pemalam luaran tidak boleh menyesuaikan fungsi pengendusan, hanya boleh bergantung pada padanan `prefix` (ASCII, <= 8 bait);
  `prefix` kosong bermakna "tidak mendapat sambungan" (lihat §5.3).
- **Satah data melalui proksi setempat**: tiada penghantaran fd (Windows tiada `SCM_RIGHTS`), satu salinan memori lebih daripada dalam proses;
  setiap panggilan terbalik juga mempunyai satu RPC setempat tambahan.
- **Jenis jadual atribut akan merosot**: `Properties.Headers` melalui pengantaraan JSON, pembezaan seperti `int32` / `double` hilang (lihat §3.4).
- **Tekanan balik satu strim menjejaskan keseluruhan sambungan**: apabila penimbal penerimaan satu strim penuh ia akan menyekat goroutine penyebaran sambungan tersebut; had kadar mengikut strim ialah pengoptimuman susulan.
- **Kegagalan pengesahan hanya menyampaikan teks**: kegagalan pengesahan kernel ialah `*plugin.AuthError` (tidak sama dengan klasifikasi `plugin.ErrorKind`),
  apabila tiba di pemalam melalui jambatan hanya ada teks, pemalam perlu memetakan kepada kod ralat protokol mengikut konvensyennya sendiri.
- **Hanya keupayaan `net.listen` benar-benar berkesan**: `store.read/write`, `http.route`, `cluster.metadata.write`,
  `auth.verify` ialah **slot simpanan**, selepas diisytiharkan hanya menyertai audit (lihat paparan tadbir urus §5.6), pada masa ini tiada titik sambungan yang sepadan.

---

## 8. FAQ penyelesaian masalah

| Fenomena | Sebab dan pengendalian |
| --- | --- |
| Status `failed`, sebab mengandungi "nama pemalam tidak konsisten" | Nama pemalam konfigurasi != `HelloAck.name`; selaraskan |
| Status `failed`, sebab mengandungi "versi API tidak padan" | `HelloAck.api_version` != `APIVersion` kernel; selaraskan |
| Status `failed`, sebab mengandungi "menolak jabat tangan" | `Hello` pemalam mengembalikan ralat (`deny`); lihat output pemalam yang diteruskan dalam log kernel |
| Status `failed`, sebab mengandungi "gagal menyambung pemalam luaran" | Proses tidak dilancarkan / `address` salah tulis / laluan socket tidak boleh ditulis (dalam kontena perhatikan kebenaran pengguna `swiftmq`) |
| Status `down` | Proses pemalam ranap atau sambungan terputus; `restart=always` akan menyambung semula automatik, `never` memerlukan pelancaran manual |
| Port tidak dibuka / klien tidak dapat menyambung | `protocols[].listeners` tidak dikonfigurasi atau alamat digantikan oleh `listeners.<nama protokol>`; semak kedua-dua tempat |
| Klien menyambung port lain kemudian terus terputus | Port tersebut tidak sepadan dengan protokol anda (`prefix` kosong atau awalan tidak padan); berikan protokol `prefix` yang tidak kosong (lihat §5.3) |
| Melaporkan `ACCESS_REFUSED - ... for user ''` | Sebelum jambatan semantik **tiada pengesahan**; panggil `core.authenticate` dahulu kemudian `session.open` |
| Panggilan terbalik melaporkan "strim N belum membuka sesi" | `core.authenticate` dahulu, kemudian `session.open`, barulah boleh memanggil `session.*` yang lain |
| Tidak menerima penghantaran penggunaan | Penghantaran tiba di `Call` anda melalui **panggilan hadapan** `session.deliver`; sahkan kaedah tersebut telah dikendalikan |
| Pemalam di luar kontena, kernel dalam kontena, tidak dapat menyambung | `address` gunakan `tcp://host.docker.internal:<port>` (atau letakkan pemalam dalam kontena juga, gunakan nama perkhidmatan); pemalam perlu mendengar `0.0.0.0` |

---

## 9. Rujukan (indeks kod sumber)

| Ingin melihat apa | Fail |
| --- | --- |
| Protokol wayar dan pelaksanaan dua hujung (**wajib baca untuk pembangunan**) | [`pkg/sidecar/`](../../../pkg/sidecar/): `frame.go` (bingkai), `proto.go` (mesej), `server.go` (pihak pemalam), `client.go` (pihak kernel), `bridge.go` (kontrak `session.*`), `stream.go` (strim) |
| Hos sidecar di pihak kernel (penyambungan/sambung semula/proksi/status) | [`internal/plugin/sidecar/sidecar.go`](../../../internal/plugin/sidecar/sidecar.go) |
| Jambatan semantik di pihak kernel (`session.*` -> `plugin.Session`) | [`internal/plugin/sidecar/bridge.go`](../../../internal/plugin/sidecar/bridge.go) |
| Jenis satah operasi sesi kernel (`Message` / `Delivery` / `ErrorKind`) | [`pkg/plugin/session.go`](../../../pkg/plugin/session.go) |
| Pendengar, pengendusan, mula/henti panas mengikut pemalam | [`internal/transport/server.go`](../../../internal/transport/server.go) |
| Kitaran hayat dan tadbir urus pemalam (pengasingan/status/audit) | [`internal/plugin/manager.go`](../../../internal/plugin/manager.go), [`registry.go`](../../../internal/plugin/registry.go) |
| Item konfigurasi dan contoh (termasuk segmen sidecar) | [`internal/config/config.go`](../../../internal/config/config.go), [`configs/swiftmqd.json`](../../../configs/swiftmqd.json) |
| Pemasangan proses (bagaimana sidecar dipasang ke dalam kernel) | [`cmd/swiftmqd/main.go`](../../../cmd/swiftmqd/main.go) |
| Pelaksanaan rujukan Go (menggunakan `pkg/sidecar.Server`, termasuk jambatan `session.*` dan `core.authenticate`) | Projek ujian bebas `swiftmq-test/test/integration/echosidecar/` |
| **Panduan per bahasa + projek contoh** | Direktori ini `plugin-development-python.md` / `-nodejs.md` / `-php.md` / `-java.md`; contoh dalam **ruang kerja** `swiftmq-plugin/{python,nodejs,php,java}/` |
