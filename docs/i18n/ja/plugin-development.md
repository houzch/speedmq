# SpeedMQ 外部プロセスプラグイン（sidecar）開発ガイド

> **対象読者**：カーネルを fork / 再コンパイルせず、**任意の言語**で SpeedMQ に拡張機能を追加したい開発者。
> **範囲**：本ドキュメントは一種類のプラグイン形態だけを扱います —— **外部プロセスプラグイン**（カーネル用語 `sidecar`）。カーネル内蔵のプロトコルプラグイン（AMQP 0-9-1 / MQTT）は対象外です。
> **読み方**：第 1〜2 節でメンタルモデルを構築し、第 3 節でコードを書き、**第 5 節が「開発後にどう組み込んで一緒に動かし、外部にサービスを提供するか」**です；
> **他の言語（Python / Node.js / PHP / Java）を使う場合は §4 の言語別ガイド**を参照してください（それぞれ実際に動作確認済みの完全なサンプルプロジェクト付き）。
> 本文中のコードは最小限の動作する骨組みで、そのままコピーして出発点にできます。原文は簡体字中国語です。

---

## 1. それは何か

**独立したプロセス**であり、自身のプロセス内である「プロトコル」（クライアントのバイトストリームを解析する）を実装します。
カーネルが設定に従ってそれをホストします：**ポートはカーネルが開き、接続はカーネルがプロキシする**ため、登録 / 起動停止 / 監査 / 隔離はすべてカーネルの既存機構を再利用します。

まず 3 つの正しいメンタルモデルを確立しましょう（最も間違えやすい点）：

1. **プラグインプロセスは「ローカルサービス」である**：それは**ローカルアドレス**（TCP または unix socket）だけをリッスンし、**カーネルが接続してくる**のを待ちます。
   接続の方向は **カーネル（クライアント）→ プラグイン（サーバー）** で、ハンドシェイクもカーネルが先に送ります。
2. **外部ビジネスポートはプラグインが開かない**：**カーネル**が設定内の `protocols[].listeners` に従って作成し、クライアント向けにマッピングします。
   クライアントが接続するのは**カーネルのポート**で、バイトはカーネルがプラグインプロセスへプロキシします。プラグインプロセスは自分でビジネスポートを開く**必要はありません**。
3. **セマンティクスは任意**：プラグインは単に「バイトを運ぶ」だけでもよく（プロトコルは完全に自分で実装）、
   **逆方向呼び出し** `session.*` を通じてカーネルのセマンティクス（キュー / ルーティング / 権限 / 確認応答）に到達することもでき、
   内蔵プロトコルプラグインと**完全に同じセマンティクス**を共有します（したがって vhost、権限、ルーティング、キュー動作は分岐しません）。

| 利点 | コスト |
| --- | --- |
| カーネルを変更せず、再コンパイルせずに拡張できる | データプレーンでローカルのバイトコピーが 1 回増える（カーネルプロキシ、fd 受け渡しなし、クロスプラットフォームで一貫） |
| 任意の言語で実装できる（ワイヤプロトコルを実装できればよい） | 逆方向呼び出しごとにローカル RPC が 1 回増える（JSON エンコード/デコード + コピー） |
| プラグインを独立してリリース / アップグレード / 再起動できる | スニッフはカーネル側に残る：「プレフィックス」か「専用ポート」でのみ識別できる |
| クラッシュはそのプラグインだけに影響：カーネルは `down` とマークし、終了もクラッシュもしない | 実際に有効なのは `net.listen` ケイパビリティのみで、他のケイパビリティ値は予約（§7 参照） |

---

## 2. 動作原理

接続確立（**カーネルがクライアント、プラグインがサーバー**）：

```
内核：读配置 plugins.<名>.sidecar
      ├─（可选）spawn 拉起子进程
      └─ dial(address)
插件：sidecar.NewServer 监听 address（Accept）
```

ハンドシェイク：

```
内核 ──Hello(kind=1)─────►  插件 Handler.Hello
内核 ◄─HelloAck(kind=2)──   插件（拒绝接入时回 error，内核据此隔离该插件）
```

サービスの開始：

```
内核：按配置 protocols 注册协议 + 创建对外监听（对外端口由内核打开）

客户端 ─► 内核端口 ─嗅探(prefix)─► 建流 Open ─► 插件 Handler.Open(Stream)
                                                  │
  插件 ─反向调用 session.*（BridgeFromContext）─► 内核（队列 / 路由 / 权限 / 确认）
  内核 ─投递回推 session.deliver ──────────────► 插件 Handler.Call
  插件 ─结算 session.settle ───────────────────► 内核
```

**ライフサイクル**（カーネル側 `internal/plugin/sidecar` ホスト）：

```
Load → Init：spawn(可选) → dial(address) → 握手 → 按配置注册 protocols（此时才起对外监听）
     → Start：起监督协程（断线重连 / 退避）
     → [运行期] 断线 → 状态 down → 退避重连（restart=always）或等待运维（restart=never）
     → Stop（内核退出）：断开连接、回收桥上会话与未结算投递、终止由内核拉起的子进程
```

**状態のセマンティクス**（`speedmqctl plugins show` で確認可能）：

| 状態 | 意味 | 運用アクション |
| --- | --- | --- |
| `enabled` | 接続済みでサービス中 | — |
| `failed` | **起動時に立ち上がらなかった**（設定エラー / ハンドシェイク拒否 / プロセスを起動できない） | `RuntimeNote` とカーネルログを確認し、設定を直すかプラグインを修正する；カーネルを再起動して初めてリトライする |
| `down` | **以前は起動していたが、今は存在しない**（プロセスがクラッシュ / 接続が切れた） | プラグインプロセスを起動する；カーネルは `restart` ポリシーに従って自動復旧する |
| `disabled` | 設定で `enabled=false`、または運用者によるホット無効化 | `plugins enable <名前>` で復旧 |

---

## 3. 開発（Go）

### 3.1 プロジェクトの作成

プラグインは**独立した Go module** で、対外契約パッケージ 2 つだけに依存します：

- `github.com/houzch/speedmq/pkg/sidecar` —— ワイヤプロトコルとプラグイン側実装（**必須**）
- `github.com/houzch/speedmq/pkg/plugin` —— `plugin.Message` / `plugin.Error` などの型を使う場合のみ（任意）

```
my-sidecar/
├── go.mod          # module my-sidecar；require github.com/houzch/speedmq（或 replace 指到本地源码）
├── main.go         # 启动 sidecar.Server
└── handler.go      # 实现 sidecar.Handler
```

```bash
go mod init my-sidecar
go get github.com/houzch/speedmq@v1.1.03
# 本地联调时可改用 replace 指向源码：
#   go mod edit -replace github.com/houzch/speedmq=../speedmq
```

> `replace` で共同デバッグする場合、プラグインとカーネルは**同一のソースツリー**でなければなりません。そうでないと API バージョン（`v1`）は一致しても型が異なる可能性があります。

### 3.2 `Handler` の実装（3 つのメソッド）

プラグインプロセスの業務面はすべて `sidecar.Handler` の `Hello` / `Call` / `Open` です。
ハンドシェイク、ハートビート、多重化、分割はすべて `pkg/sidecar` が処理するため、フレームに触れる必要はありません。

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

	"github.com/houzch/speedmq/pkg/sidecar"
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

### 3.3 データプレーン：`Stream` の読み書き

`sidecar.Stream` は `io.ReadWriteCloser` を実装しているので、そのまま「1 本の接続」として使えます：

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

ポイント：

- クライアント接続 1 本 = ストリーム 1 本；プラグインは `Handler.Open` 内ですぐに送受信を開始できます。
- 大きなメッセージはライブラリが自動的に分割します（各フレーム ≤ 64 KiB）。**メモリ使用量はメッセージサイズに依存しません**。
- バックプレッシャ：単一ストリームの受信バッファには上限があり、バッファが満杯になるとその接続の**ディスパッチゴルーチン**がブロックされます（すべてのストリームが一緒に待つ）——
  これは「メモリの予測可能性」と「単一ストリームのレート制限」のトレードオフです。詳細は §7。

### 3.4 カーネルセマンティクスブリッジを使う（`session.*`）

プラグインにカーネルのキュー / ルーティング / 権限 / 確認応答セマンティクスを再利用させたい（独自に作るのではなく）場合は、**逆方向呼び出し**を使います。
ストリーム上で `session.open` → その他の `session.*` → （`session.close`）の順に使います：

```go
import (
	"errors"

	"github.com/houzch/speedmq/pkg/plugin"
	"github.com/houzch/speedmq/pkg/sidecar"
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

**逆方向呼び出しメソッド一覧**（`pkg/sidecar/bridge.go` の定数 → ワイヤ名）：

| グループ | ワイヤ名（定数） | 説明 |
| --- | --- | --- |
| 認証 | `core.authenticate`（`MethodCoreAuthenticate`） | **最初に必ず行う**：プロトコル内の資格情報をカーネルに渡して検証させる；パラメータ `{stream, mechanism, response}`、戻り値 `{user}` |
| セッション | `session.open`（`MethodSessionOpen`） | ストリーム上である vhost のセッションを開く；**認証後**でなければできない |
| | `session.close`（`MethodSessionClose`） | そのストリーム上のセッションを解放する（コンシューマーのキャンセル、排他キューの削除） |
| エクスチェンジ | `session.declare_exchange` / `session.delete_exchange` | 追加/削除（パッシブ宣言で存在しない → `KindNotFound`） |
| | `session.bind_exchange` / `session.unbind_exchange` | エクスチェンジからエクスチェンジへのバインド |
| キュー | `session.declare_queue` / `session.delete_queue` | 追加/削除；`name` が空のときはサーバーが生成 |
| | `session.bind_queue` / `session.unbind_queue` | キューからエクスチェンジへのバインド |
| | `session.purge_queue` | 準備完了メッセージをパージする（未確認は含まない） |
| パブリッシュ | `session.publish` | `{routed, rejected}` を返す；永続化は応答前に完了 |
| 取得 | `session.get` | 能動的に 1 件取得；`found=false` はキューが空を意味する |
| コンシューム | `session.consume` / `session.cancel` | コンシューマーの登録 / キャンセル |
| 確定 | `session.settle` | 1 件の配信を確定する（`ack` / `requeue` / `reject`） |
| **順方向** | `session.deliver` | **カーネル → プラグイン**：配信の回推（あなたの `Handler.Call` で処理） |

**必ず守るべき 4 つの約束**：

1. **まず `core.authenticate`**：接続のカーネル操作面は認証前に身元を持たないため、
   このとき `session.open` は拒否されます（`ACCESS_REFUSED - access to vhost '/' refused for user ''`）。
   プラグインは自分のプロトコルから資格情報を取り出します；認証ロジックとユーザーテーブルは引き続きカーネルにあり、プラグインはパスワードストアに触れません。
2. **次に `session.open`**：セッションを開かずに他のメソッドを呼ぶと、カーネルは `KindPreconditionFailed` を返します（「ストリーム N はまだセッションを開いていません」）。
3. **各配信はちょうど 1 回確定する**：`Ack` / `Requeue` / `Reject` のいずれか 1 つ。
   `Ack` と `Reject` はどちらもメッセージを破棄しますが、**`Reject` だけがデッドレターへ進みます**。
4. **未確定の配信は失われない**：ストリームが終了したとき（クライアント切断 / `Handler.Open` が返った）やプラグイン接続が切れたとき、
   カーネルはすべての未確定配信を**「再キュー」として処理**し、メッセージの滞留を防ぎます。

**エラーの復元**：カーネルの `*plugin.Error` はブリッジを通じて `*sidecar.RPCError`（フィールド `Kind` / `Text`）として届き、
分類を文字列に埋め込むことなく、分類付きの `plugin.Error` に復元できます：

```go
if err := br.Call(ctx, sidecar.MethodSessionPublish, params, nil); err != nil {
	var rpc *sidecar.RPCError
	if errors.As(err, &rpc) {
		return plugin.Errorf(plugin.ErrorKind(rpc.Kind), "%s", rpc.Text)
	}
	return err
}
```

一般的な `Kind` と内蔵プロトコルのマッピング（クライアントへどうエラーを返すか判断するため）：

| `plugin.ErrorKind` | セマンティクス | AMQP 0-9-1 マッピング |
| --- | --- | --- |
| `KindNotFound` | オブジェクトが存在しない | 404 NOT_FOUND（Channel を閉じる） |
| `KindPreconditionFailed` | パラメータが既存オブジェクトと不一致 / セッションが未オープン | 406 PRECONDITION_FAILED（Channel を閉じる） |
| `KindAccessRefused` | 権限不足 / 予約名 | 403 ACCESS_REFUSED（Channel を閉じる） |
| `KindResourceLocked` | 排他リソースが使用中 | 405 RESOURCE_LOCKED（Channel を閉じる） |
| `KindInvalidPath` | vhost が存在しない | 402 INVALID_PATH（接続を閉じる） |
| `KindNotImplemented` | ケイパビリティ未実装 | 540 NOT_IMPLEMENTED（接続を閉じる） |
| `KindInternal` | カーネル内部エラー | 541 INTERNAL_ERROR（接続を閉じる） |

**メッセージ型の忠実性の境界**：プロパティテーブル（`MessageDTO.Properties.Headers`）は JSON を経由するため、
AMQP field-table のように **`int32` / `double` を区別する数値型情報は得られません**。
厳密な型の忠実性が必要な場合は、そのような情報をメッセージ本文（生のバイト）に入れて自分で運んでください。

### 3.5 プラグイン側の状態とログ

- 外部プラグインの**状態は「接続が生きているかどうか」で決まり**、プラグインが自分で報告する必要はありません（内蔵プラグインの `StateReporter` は外部プロセスには適用されません）。
- ログ：`sidecar.ServerOptions.Logger` はプラグインプロセスの stdout/stderr に出力します；
  **カーネルが `spawn` で起動した場合、これらの出力はカーネルによってカーネルログに転送されます**（`plugin` タグ付き）。一元収集が容易になります。
- 独立デプロイ（spawn でない）の場合、プラグインログは自分の方法で収集してください。

### 3.6 カーネルなしでも自己テスト可能

`Handler` は普通の Go インターフェースなので、単体テストで直接インスタンス化し `Hello` / `Call` / `Open` を呼べばネットワークを起動せずに業務ロジックをカバーできます：

```go
func TestHelloRejectsWrongName(t *testing.T) {
	h := &handler{}
	if _, err := h.Hello(context.Background(), sidecar.Hello{Plugin: "other", APIVersion: "v1"}); err == nil {
		t.Fatal("插件名不一致时应当拒绝")
	}
}
```

エンドツーエンドの検証は §5 を参照。

---

## 4. 他の言語での開発（ワイヤプロトコル仕様）

`pkg/sidecar` はゼロ依存の対外契約です；ワイヤプロトコル自体は非常に単純で、どの言語でも実装できます。
接続するには、以下の「バイトレベル」の取り決めを実装する必要があります（ソースは `pkg/sidecar/frame.go`、`proto.go` を参照）。

> **完全なサンプルプロジェクト付きの言語別ガイドを提供しています**（サンプルはすべて実測で通過：ハンドシェイク → 認証 → セマンティクスブリッジ → 配信/確定 → バイトストリーム）：
>
> | 言語 | ガイド | サンプルプロジェクト（ワークスペース `speedmq-plugin/`） |
> | --- | --- | --- |
> | Python | [plugin-development-python.md](plugin-development-python.md) | `python/sidecar_plugin.py`（標準ライブラリのみ） |
> | Node.js | [plugin-development-nodejs.md](plugin-development-nodejs.md) | `nodejs/index.js`（標準ライブラリのみ） |
> | PHP | [plugin-development-php.md](plugin-development-php.md) | `php/sidecar_plugin.php`（標準ライブラリのみ） |
> | Java | [plugin-development-java.md](plugin-development-java.md) | `java/SidecarPlugin.java`（単一ファイル、JDK のみ） |
>
> Go の完全なリファレンス実装は独立したテストプロジェクト `speedmq-test/test/integration/echosidecar/` を参照してください（これは `pkg/sidecar.Server` を直接使うので、
> 以下のバイトレベルの詳細を気にする必要はありません）。

**フレーム形式**（すべてのフレームで共通）：

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)，即长度字段**包含** kind 字节；单帧上限 16 MiB。
```

**フレーム種別 `kind`**：

| kind | 名称 | 方向 | ペイロード |
| --- | --- | --- | --- |
| 1 | Hello | カーネル → プラグイン | JSON `Hello` |
| 2 | HelloAck | プラグイン → カーネル | JSON `HelloAck` |
| 3 | Ping | カーネル → プラグイン | 空 |
| 4 | Pong | プラグイン → カーネル | 空 |
| 5 | Call | 双方向 | JSON `Call` |
| 6 | Reply | 双方向 | JSON `Reply` |
| 7 | Open | カーネル → プラグイン | JSON `Open` |
| 8 | OpenAck | プラグイン → カーネル | JSON `OpenAck` |
| 9 | Data | 双方向 | `u32 BE stream` + 生のバイト |
| 10 | Close | 双方向 | JSON `Close` |

**制御プレーン JSON 構造**（フィールド名は `proto.go` と一致）。

> この部分は**ワイヤプロトコルのメッセージ例**です（同じブロック内に複数のメッセージを順に示すため、`//` で区切って説明しています）、
> **`speedmqd.json` に直接書ける設定ではありません**。

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

**必ず守るべきセマンティクス**：

- **ハンドシェイク**：カーネルが先に `Hello` を送り、プラグインは `HelloAck` フレームを返さなければなりません。
  カーネルは `HelloAck.name == 設定内のプラグイン名` かつ `HelloAck.api_version == カーネルの APIVersion` を検証します；
  `deny` が非空なら接続拒否と見なされます（プラグインは隔離されます）。
- **ハートビート**：カーネルは既定で 2 秒ごとに `Ping` を送り、プラグインは 8 秒以内に `Pong` を返す必要があります；プラグイン側は 24 秒以内にどのフレームも受信しなければ自ら接続を閉じてよい。
- **2 つの ID 空間**：`Call` の `reverse` が方向を区別し、2 つの方向はそれぞれ 1 から増加します、
  したがって `Reply` は**必ず `reverse` を返さなければならず**、そうでないと応答は誤った待ち手に届きます。
- **データプレーンは base64 にしない**：メッセージ本文などの大きなデータは直接 `Data` フレームのペイロードに置きます（`stream` + 生のバイト）。必要に応じて分割します。

> Go を使う場合は `pkg/sidecar` を直接使えば、上記の詳細を自分で実装する必要はありません。

---

## 5. 組み込んで一緒に動かす：接続、外部サービス、パッケージング ★

この節は「開発後にどう SpeedMQ に組み込み、どう外部にサービスを提供するか」に答えます。

### 5.1 設定でプラグインを宣言する

外部プラグインは**完全に設定で管理**され、カーネルはそのためにコードを変更する必要がありません。`speedmqd.json` の `plugins` セクションに 1 項目を追加します
（**実際の設定は標準 JSON で、コメントを付けられません**）：

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

項目ごとの説明（フィールド一覧は下表）：

- `plugins.<プラグイン名>` のキー名は**プラグインが自己申告する `HelloAck.name` と一致しなければならず**、そうでないとハンドシェイクが拒否されます。
- `builtin: false`：外部プラグインを明示的に宣言します（書かないと管理面が内蔵として表示します）。
- `enabled`：これをオフにすると = プロセスを起動せず、リスナーも作成しません。
- `required: true` の場合、起動失敗がカーネルの起動をブロックします —— 外部プラグインでは有効にしないでください。
- `address` は**カーネルが接続する先のアドレス**です（カーネルがクライアント）；`spawn` が非空のときはカーネルが代わりにプロセスを起動します。
- `protocols[].prefix` は**非空でなければなりません**（スニッフ規則は §5.3）。
- `listeners` はそのプロトコルの**対外ポートで、カーネルが開きます**（クライアントが接続するのはカーネル）。

フィールド一覧：

| フィールド | 必須 | 説明 |
| --- | --- | --- |
| `sidecar.address` | ✅ | プラグインプロセスのアドレス：`tcp://host:port` または `unix:///path` |
| `sidecar.spawn` | ✕ | カーネルが代わりに起動するコマンドライン（先頭要素が実行ファイル）；**空 = カーネルは接続するだけで起動しない**、プロセスは自分で管理 |
| `sidecar.restart` | ✕ | `always`（既定、切断/クラッシュ後に自動復旧）または `never`（`down` とマークするだけで運用者の介入を待つ） |
| `sidecar.protocols[].name` | ✅ | プロトコル名（グローバルに一意、スニッフ優先度に参加） |
| `sidecar.protocols[].prefix` | ✕ | スニッフプレフィックス（ASCII）；**空 = スニッフに参加しない** |
| `sidecar.protocols[].listeners[]` | ✕ | そのプロトコルの対外リスナー（`name` + `addr`）、カーネルが作成 |
| `sidecar.handshake_timeout_seconds` | ✕ | ハンドシェイクタイムアウトを上書き（既定 5 秒） |
| `sidecar.heartbeat_seconds` | ✕ | ハートビート間隔を上書き（既定 2 秒） |

> **プラグイン名とプロトコル名**：両者は**異なってよい**（例：プラグイン `my-sidecar` がプロトコル `myproto` を提供）。
> ホット無効化はまずプラグイン名で登録済みの全プロトコルを見つけ、次にそれらのプロトコルのポートを閉じるため、わざわざ同名にする必要はありません。

### 5.2 3 つの接続方式

| 方式 | 設定 | 適する場面 |
| --- | --- | --- |
| **同一ホスト + カーネル起動（spawn）** | `spawn: [...]`、`address` はそれがリッスンするアドレスを指す | 同一マシンデプロイ、単一コンテナ；最も手間が少なく、カーネルが起動と回収を担当 |
| **同一ホスト + 自己管理（dial）** | `spawn: []`、`address` はすでに起動しているプロセスを指す | systemd / supervisor でプラグインのライフサイクルを管理 |
| **クロスホスト / クロスコンテナ（dial、必ず tcp）** | `spawn: []`、`address: "tcp://<サービス名>:19001"` | プラグインとカーネルを別コンテナ / 別マシンにデプロイ |

アドレスの選択：

- **同一マシンでは unix socket を推奨**（`unix:///tmp/my-sidecar.sock`）：TCP ポートを占有せず、ホストのポート占有の影響を受けません。
  socket パスはカーネルプロセス（コンテナ内では非 root の `speedmq` ユーザー）から書き込み可能である必要があります。
- **クロスコンテナでは必ず TCP**、かつプラグインプロセスは `0.0.0.0` をリッスンし、`address` には**コンテナネットワーク内のサービス名**を使います。

> 方向を逆にしないでください：**プラグインがリッスンするアドレス** = `address`；**クライアントに公開するポート** = `protocols[].listeners`。

### 5.3 外部へのサービス提供：`prefix` で識別される

アクセス層が接続をディスパッチするときは**スニッフ結果だけを見ます**：有効な各プロトコルに対して登録順に `Sniff(peek)` を問い合わせ（peek は最大 8 バイト）、
一致したものがこの接続を引き継ぎます。したがって：

1. **`prefix` は非空でなければなりません**（ASCII、≤ 8 バイト）。クライアントが送ってきた先頭数バイトがそれと一致したときだけ、接続があなたのプラグインに渡されます。
   例：`"prefix": "PY"` → クライアントの先頭バイトは `PY` でなければならない（プレフィックスを自分のプロトコルのマジックヘッダとして扱えます）。
2. **`prefix` が空はスニッフに参加しないことを意味します**：このような接続はプラグインに渡され**ません**（実測：リスンポート上の接続は即座に切断されます）。
   したがって空の `prefix` は「別のプロトコルが同じポートであなたの代わりに転送する」シナリオにのみ適しており、専用ポートを作るのには**使わないでください**。
3. `listeners[].addr` は「どのポートで外部に公開するか」を決め、`prefix` は「この接続が自分のかどうか」を決めます ——
   両者はセットで使う必要があります：**専用ポートでも非空の `prefix` を与えてください**（これがカーネルのサンプル設定で
   `echo-sidecar` が `prefix: "ECHO"` と `listeners: [":1885"]` を両方書いている理由でもあります）。
4. スニッフはプロトコルの登録順に一致し、**先に一致したものが有効**です：複数のプラグインが共存するとき、プレフィックスには区別が必要です（例えば同じバイトで始まると互いに遮蔽し合います）。

### 5.4 リスンアドレスと TLS の上書き

- 対外リスンアドレスは**2 か所**で指定できます：`sidecar.protocols[].listeners[].addr`（既定）と
  `listeners.<プロトコル名>`（プロトコル名で一括上書き）。両方が存在する場合は `listeners.<プロトコル名>` が優先されます。
- TLS が必要な場合は、`listeners.<プロトコル名>[i].tls` に証明書を指定します（フィールドは内蔵プロトコルと同一）。
  以下は `listeners` の抜粋です（**標準 JSON、コメント不可**）：1 番目は平文、2 番目は TLS。

```json
"listeners": {
  "myproto": [
    { "addr": ":19002" },
    { "addr": ":19003", "tls": { "cert_file": "/etc/speedmq/tls/cert.pem",
                                 "key_file":  "/etc/speedmq/tls/key.pem" } }
  ]
}
```

> TLS は**カーネル**がリスナー側で終端するため、プラグインプロセスが受け取るのは平文ストリームです —— プラグインが TLS を処理する必要はありません。

### 5.5 パッケージング：プラグインをカーネルと一緒に動かす

**方法 A —— 同じイメージに組み込む**（「カーネルと一緒にリリースする」プラグインに推奨）：`speedmq/Dockerfile` のランタイムステージに 1 行追加します：

```dockerfile
COPY --from=<构建你的插件的 stage> /out/my-sidecar /usr/local/bin/my-sidecar
# 或直接拷预编译产物：
# COPY bin/my-sidecar /usr/local/bin/my-sidecar
```

次に設定で `spawn: ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"]`、
`address` も同じ値にします。カーネル起動時にこれが起動されます。

**方法 B —— バイナリをマウント**（イメージを変更しない、共同デバッグに最適）：

```yaml
services:
  speedmq:
    image: houzch/speedmq:1.1.03
    command: ["-config", "/etc/speedmq/speedmqd.json", "-log-level", "info"]
    volumes:
      - ./configs/speedmqd.json:/etc/speedmq/speedmqd.json:ro
      - ./bin/my-sidecar:/usr/local/bin/my-sidecar:ro   # 插件二进制
    ports:
      - "5672:5672"        # AMQP（内置）
      - "15672:15672"      # 管理面
      - "19002:19002"      # ← 你的协议对外端口（由内核监听）
```

設定は unix socket を使います（余分なポート占有を避ける）。以下は `plugins.my-sidecar` 内の `sidecar` 抜粋です
（**標準 JSON、コメント不可**；`prefix` は依然として非空でなければならない、§5.3 参照）：

```json
"sidecar": {
  "address": "unix:///tmp/my-sidecar.sock",
  "spawn": ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"],
  "protocols": [{ "name": "myproto", "prefix": "MP", "listeners": [{ "name": "myproto", "addr": ":19002" }] }]
}
```

**方法 C —— 独立コンテナ**（プラグインを個別リリース / 独立スケール）：

```yaml
services:
  speedmq:
    image: houzch/speedmq:1.1.03
    volumes: ["./configs/speedmqd.json:/etc/speedmq/speedmqd.json:ro"]
    ports: ["5672:5672", "15672:15672", "19002:19002"]
    depends_on: [my-sidecar]

  my-sidecar:
    image: my-sidecar:0.1.0
    command: ["-addr", "tcp://0.0.0.0:19001"]   # 对外（对内）监听 0.0.0.0
```

設定では `spawn: []`（カーネルは接続するだけで起動しない）、`address: "tcp://my-sidecar:19001"`（compose のサービス名）。

### 5.6 起動と検証

```bash
# 1) 内核日志里应能看到握手与接入
docker compose logs speedmq | grep -E "外部插件已接入|外部插件进程"

# 2) 经 CLI 看插件状态（state=enabled 且 RuntimeNote 为空）
./bin/speedmqctl plugins list
./bin/speedmqctl plugins show my-sidecar

# 3) 经管理 API 看（等价入口）
curl -u guest:guest http://127.0.0.1:15672/api/plugins/my-sidecar

# 4) 实测对外服务：你的协议端口由内核监听，直接连它
#    若你的协议是文本行协议，可以这样冒烟：
printf 'hello\n' | nc 127.0.0.1 19002
```

`plugins show` では `state` と `RuntimeNote` に特に注目してください：
`failed` は失敗理由（ハンドシェイク拒否 / ポートを開けない…）を伴います；`down` は切断理由（プロセスがクラッシュ / 接続が切れた）を伴います。

### 5.7 運用時の操作

| 操作 | コマンド / インターフェース | 効果 |
| --- | --- | --- |
| ホット無効化 | `speedmqctl plugins disable my-sidecar` または `PUT /api/plugins/my-sidecar/disable` | **そのプラグインの対外リスナーを閉じる**（ケイパビリティレベルの無効化）；カーネルと他のプラグインは影響を受けない |
| ホット有効化 | `speedmqctl plugins enable my-sidecar` | そのリスナーを再度開く；以前に起動失敗した場合は 1 回リトライする |
| 状態確認 | `speedmqctl plugins list/show` | 状態 + 失敗/切断理由 |
| カーネル終了 | — | プラグインとの接続を切断し、ブリッジ上のセッションを回収し、**カーネルが `spawn` した子プロセスを終了する** |

> ホット無効化は「ケイパビリティ」（リスンポート）だけを閉じ、`spawn` で起動したプラグインプロセスを**殺しません**；プロセスの回収はカーネル終了時に行われます。

### 5.8 管理 UI に入口を提供する（任意）

プラグインが独自の操作画面を持つ場合、`plugins.<プラグイン名>` セクションに `console_url`（管理画面のアドレス、その他のフィールドは §5.1）を追加します。
以下は `plugins.my-sidecar` の項目のみを示します（**標準 JSON、コメント不可**；`sidecar` セクションの内容は §5.1 と同じ）：

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

- 管理 UI の**プラグイン管理**ページ（データは `GET /api/plugins` の `console_url` フィールドから）は、この種のプラグインに
  「管理画面を開く」ボタンを表示し、**新しいタブで開きます**。
- `console_url` が宣言されていない場合、ボタンは使用不可で、ツールチップに「このプラグインは管理画面を提供していません」と表示されます。
- これは単に**デプロイ担当者が設定に書いたメタデータ**です：プラグイン API（`pkg/plugin`）には属さず、プラグインの起動停止にも関与せず、
  画面自体はプラグインが自らホストします（プラグインプロセス内でも、任意の独立サービスでもかまいません）。

---

## 6. ライフサイクルとフォールトトレランスマトリクス

| 状況 | カーネルの動作 | プラグイン側への影響 |
| --- | --- | --- |
| プラグインプロセスが未起動 / ハンドシェイク拒否 | 8 秒以内に接続をリトライし、なお失敗すれば `failed` とマークして隔離（カーネル起動はブロックしない） | なし |
| `spawn` で起動したプロセスが終了 | ログに記録；`down` とマーク；`restart` ポリシーに従ってバックオフ再接続 / 再起動 | 新プロセスがハンドシェイクをやり直す |
| プラグインプロセスがクラッシュ（運用時） | カーネルは影響を受けない；`down` + バックオフ再接続 | `pkg/sidecar.Server` がその接続を閉じる |
| カーネルが `kill -9` される | — | プラグイン側はアイドルタイムアウト（既定 24 秒フレームなし）で自ら接続を回収し、ゾンビを残さない |
| カーネルが正常終了 | `Stop` を呼ぶ：接続を切断、未確定配信を回収（再キューとして）、子プロセスを `Kill` | SIGKILL を受信 |
| プラグイン接続が切れている間の配信 | 未確定配信は一律**再キュー**され、失われない | — |
| クライアント切断 / `Open` が返る | 対応するストリームを閉じ、そのストリームのセッションとコンシューマーを解放 | `Stream.Read` が EOF を返す |

---

## 7. レッドラインと既知の境界

**レッドライン**

1. プラグインは `pkg/sidecar`（および任意の `pkg/plugin`）にのみ依存できます；カーネルの `internal/**` に依存しては**なりません**。
2. プラグイン名は設定と一致し、`APIVersion` はカーネルと一致しなければならず、そうでないと接続できません（これは「静かに動いていて効いていない」のを防ぐものです）。
3. `session.*` を使うとき：**まず `core.authenticate`、次に `session.open`**、各配信は**ちょうど 1 回確定**します。
4. `protocols[].prefix` は非空でなければならず、そうでないと接続はプラグインに渡されません（§5.3 参照）。
5. `Hello` での拒否は**明示的に error を返さなければなりません**（黙ってはいけません）—— さもないとカーネルは「接続が閉じられた」としか見えず、原因を特定できません。

**既知の境界**

- **スニッフはカーネル側**：外部プラグインはスニッフ関数をカスタマイズできず、`prefix`（ASCII、≤ 8 バイト）での一致のみです；
  `prefix` が空は「接続を取得できない」ことを意味します（§5.3 参照）。
- **データプレーンはローカルプロキシ経由**：fd 受け渡しがなく（Windows には `SCM_RIGHTS` がない）、プロセス内よりメモリコピーが 1 回多くなります；
  逆方向呼び出しも毎回ローカル RPC が 1 回多くなります。
- **プロパティテーブルの型は退化する**：`Properties.Headers` は JSON を経由するため、`int32` / `double` などの区別が失われます（§3.4 参照）。
- **単一ストリームのバックプレッシャは接続全体に影響**：1 本のストリームの受信バッファが満杯になるとその接続のディスパッチゴルーチンがブロックされます；ストリーム単位のレート制限は今後の最適化です。
- **認証失敗はテキストのみ透過**：カーネルの認証失敗は `*plugin.AuthError`（`plugin.ErrorKind` とは別の分類体系）で、
  ブリッジを通じてプラグインに届くのはテキストのみです；プラグインは自分の取り決めに従ってプロトコルエラーコードにマッピングする必要があります。
- **実際に有効なのは `net.listen` ケイパビリティのみ**：`store.read/write`、`http.route`、`cluster.metadata.write`、
  `auth.verify` は**予約枠**で、宣言しても監査に参加するだけです（§5.6 のガバナンス表示を参照）。現在対応する拡張点はありません。

---

## 8. トラブルシューティング FAQ

| 症状 | 原因と対処 |
| --- | --- |
| 状態 `failed`、理由に「プラグイン名不一致」 | 設定のプラグイン名 ≠ `HelloAck.name`；揃える |
| 状態 `failed`、理由に「API バージョン不一致」 | `HelloAck.api_version` ≠ カーネルの `APIVersion`；揃える |
| 状態 `failed`、理由に「ハンドシェイク拒否」 | プラグインの `Hello` が error（`deny`）を返した；カーネルログに転送されたプラグイン出力を確認 |
| 状態 `failed`、理由に「外部プラグインへの接続失敗」 | プロセスが起動していない / `address` が誤り / socket パスが書き込み不可（コンテナ内では `speedmq` ユーザー権限に注意） |
| 状態 `down` | プラグインプロセスがクラッシュしたか接続が切れた；`restart=always` は自動再接続、`never` は手動起動が必要 |
| ポートが開かない / クライアントが接続できない | `protocols[].listeners` が未設定か、アドレスが `listeners.<プロトコル名>` に上書きされている；2 か所を照合 |
| クライアントが別のポートに接続後すぐ切断される | そのポートがあなたのプロトコルに一致しない（`prefix` が空かプレフィックス不一致）；プロトコルに非空の `prefix` を設定（§5.3 参照） |
| `ACCESS_REFUSED - ... for user ''` が報告される | セマンティクスブリッジの前に**認証がない**；先に `core.authenticate` を呼び、次に `session.open` |
| 逆方向呼び出しで「ストリーム N はまだセッションを開いていません」と報告される | 先に `core.authenticate`、次に `session.open`、その後で他の `session.*` を呼ぶ |
| コンシューム配信を受信できない | 配信は**順方向呼び出し** `session.deliver` としてあなたの `Call` に届く；そのメソッドを処理しているか確認 |
| プラグインがコンテナ外、カーネルがコンテナ内で接続できない | `address` に `tcp://host.docker.internal:<port>` を使う（またはプラグインもコンテナに入れてサービス名を使う）；プラグインは `0.0.0.0` をリッスンする必要がある |

---

## 9. 参考（ソース索引）

| 見たいもの | ファイル |
| --- | --- |
| ワイヤプロトコルと両端の実装（**開発必読**） | [`pkg/sidecar/`](../../../pkg/sidecar/)：`frame.go`（フレーム）、`proto.go`（メッセージ）、`server.go`（プラグイン側）、`client.go`（カーネル側）、`bridge.go`（`session.*` 契約）、`stream.go`（ストリーム） |
| カーネル側 sidecar ホスト（接続/再接続/プロキシ/状態） | [`internal/plugin/sidecar/sidecar.go`](../../../internal/plugin/sidecar/sidecar.go) |
| カーネル側セマンティクスブリッジ（`session.*` → `plugin.Session`） | [`internal/plugin/sidecar/bridge.go`](../../../internal/plugin/sidecar/bridge.go) |
| カーネルセッション操作面の型（`Message` / `Delivery` / `ErrorKind`） | [`pkg/plugin/session.go`](../../../pkg/plugin/session.go) |
| リスン、スニッフ、プラグイン単位のホット起動停止 | [`internal/transport/server.go`](../../../internal/transport/server.go) |
| プラグインのライフサイクルとガバナンス（隔離/状態/監査） | [`internal/plugin/manager.go`](../../../internal/plugin/manager.go)、[`registry.go`](../../../internal/plugin/registry.go) |
| 設定項目とサンプル（sidecar セクション含む） | [`internal/config/config.go`](../../../internal/config/config.go)、[`configs/speedmqd.json`](../../../configs/speedmqd.json) |
| プロセス組み立て（sidecar がどうカーネルに組み込まれるか） | [`cmd/speedmqd/main.go`](../../../cmd/speedmqd/main.go) |
| Go リファレンス実装（`pkg/sidecar.Server` を使用、`session.*` ブリッジと `core.authenticate` を含む） | 独立テストプロジェクト `speedmq-test/test/integration/echosidecar/` |
| **言語別ガイド + サンプルプロジェクト** | 本ディレクトリの `plugin-development-python.md` / `-nodejs.md` / `-php.md` / `-java.md`；サンプルは**ワークスペース** `speedmq-plugin/{python,nodejs,php,java}/` |
