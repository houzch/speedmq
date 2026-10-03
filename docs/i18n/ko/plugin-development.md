# SwiftMQ 외부 프로세스 플러그인(sidecar) 개발 가이드

> **대상**: 커널을 fork / 재컴파일하지 않고 **임의의 언어**로 SwiftMQ를 확장하려는 개발자.
> **범위**: 이 문서는 한 가지 플러그인 형태인 **외부 프로세스 플러그인**(커널 용어 `sidecar`)만 다룹니다. 커널 내장 프로토콜 플러그인(AMQP 0-9-1 / MQTT)은 이 문서의 범위가 아닙니다.
> **읽는 법**: 1–2절에서 멘탈 모델을 세우고, 3절에서 코드를 작성하며, **5절은 "개발을 마친 뒤 어떻게 연결해 함께 실행하고 대외 서비스를 제공하는가"** 입니다;
> **다른 언어(Python / Node.js / PHP / Java)는 §4의 언어별 가이드를 보세요** (각각 실측으로 검증된 완전한 예제 프로젝트를 포함합니다).
> 문서의 코드는 최소 실행 가능 골격으로 그대로 복사해 시작점으로 삼을 수 있습니다. 원본 언어는 중국어 간체입니다.

---

## 1. 무엇인가

**독립 프로세스**가 자신의 프로세스 안에서 어떤 "프로토콜"(클라이언트 바이트 스트림 파싱)을 구현합니다.
커널이 설정에 따라 이를 호스팅합니다: **포트는 커널이 열고 연결은 커널이 프록시**하며, 등록 / 기동·정지 / 감사 / 격리는 모두 커널의 기존 메커니즘을 재사용합니다.

먼저 세 가지 올바른 멘탈 모델을 세웁니다(가장 헷갈리기 쉬운 지점):

1. **플러그인 프로세스는 "로컬 서비스"입니다**: 하나의 **로컬 주소**(TCP 또는 unix socket)만 리슨하고 **커널이 연결해 오기를** 기다립니다.
   연결 방향은 **커널(클라이언트) → 플러그인(서버)** 이며, 핸드셰이크도 커널이 먼저 보냅니다.
2. **대외 비즈니스 포트는 플러그인이 열지 않습니다**: **커널**이 설정의 `protocols[].listeners`에 따라 생성하여 클라이언트에 매핑해 노출합니다.
   클라이언트가 연결하는 것은 **커널의 포트**이고, 바이트는 커널이 플러그인 프로세스로 프록시합니다. 플러그인 프로세스는 비즈니스 포트를 직접 열 **필요가 없습니다**.
3. **시맨틱은 선택 사항입니다**: 플러그인은 단지 "바이트를 나르기"만 할 수도 있고(프로토콜을 전적으로 직접 구현),
   **역방향 호출** `session.*`를 통해 커널 시맨틱(큐 / 라우팅 / 권한 / 확인)에 도달할 수도 있습니다.
   내장 프로토콜 플러그인과 **완전히 동일한 시맨틱**을 가집니다(vhost, 권한, 라우팅, 큐 동작이 갈라지지 않음).

| 장점 | 대가 |
| --- | --- |
| 커널을 수정·재컴파일하지 않고 확장 | 데이터 플레인에 로컬 바이트 복사가 한 번 더 발생(커널 프록시, fd 전달 없음, 크로스 플랫폼 일관) |
| 임의 언어로 구현(와이어 프로토콜만 구현하면 됨) | 역방향 호출마다 로컬 RPC가 한 번 더 발생(JSON 인코딩/디코딩 + 복사) |
| 플러그인을 독립적으로 릴리스 / 업그레이드 / 재시작 | 스니핑은 커널 측에 남음: "프리픽스" 또는 "전용 포트"로만 식별됨 |
| 크래시가 해당 플러그인에만 영향: 커널은 `down`으로 표시하고 종료·크래시하지 않음 | `net.listen` 능력만 실제로 유효하고 나머지 능력 값은 예약됨(§7 참조) |

---

## 2. 동작 원리

연결 수립(**커널이 클라이언트, 플러그인이 서버**):

```
内核：读配置 plugins.<名>.sidecar
      ├─（可选）spawn 拉起子进程
      └─ dial(address)
插件：sidecar.NewServer 监听 address（Accept）
```

핸드셰이크:

```
内核 ──Hello(kind=1)─────►  插件 Handler.Hello
内核 ◄─HelloAck(kind=2)──   插件（拒绝接入时回 error，内核据此隔离该插件）
```

서비스 시작:

```
内核：按配置 protocols 注册协议 + 创建对外监听（对外端口由内核打开）

客户端 ─► 内核端口 ─嗅探(prefix)─► 建流 Open ─► 插件 Handler.Open(Stream)
                                                  │
  插件 ─反向调用 session.*（BridgeFromContext）─► 内核（队列 / 路由 / 权限 / 确认）
  内核 ─投递回推 session.deliver ──────────────► 插件 Handler.Call
  插件 ─结算 session.settle ───────────────────► 内核
```

**수명 주기**(커널 측 `internal/plugin/sidecar` 호스트):

```
Load → Init：spawn(可选) → dial(address) → 握手 → 按配置注册 protocols（此时才起对外监听）
     → Start：起监督协程（断线重连 / 退避）
     → [运行期] 断线 → 状态 down → 退避重连（restart=always）或等待运维（restart=never）
     → Stop（内核退出）：断开连接、回收桥上会话与未结算投递、终止由内核拉起的子进程
```

**상태 시맨틱**(`swiftmqctl plugins show`로 확인 가능):

| 상태 | 의미 | 운영 조치 |
| --- | --- | --- |
| `enabled` | 접속되어 서비스 중 | — |
| `failed` | **기동 자체가 안 됨**(설정 오류 / 핸드셰이크 거부 / 프로세스 기동 불가) | `RuntimeNote`와 커널 로그를 보고 설정을 고치거나 플러그인을 수정; 커널을 재시작해야 재시도 |
| `down` | **기동한 적이 있고 지금은 없음**(프로세스 크래시 / 연결 끊김) | 플러그인 프로세스를 기동; 커널이 `restart` 정책에 따라 자동 복구 |
| `disabled` | 설정에서 `enabled=false`이거나 운영자가 핫 비활성화 | `plugins enable <이름>`으로 복구 |

---

## 3. 개발(Go)

### 3.1 프로젝트 생성

플러그인은 **독립 Go module**이며, 두 개의 대외 계약 패키지에만 의존합니다:

- `github.com/houzch/swiftmq/pkg/sidecar` —— 와이어 프로토콜과 플러그인 측 구현(**필수**)
- `github.com/houzch/swiftmq/pkg/plugin` —— `plugin.Message` / `plugin.Error` 같은 타입이 필요할 때만(선택)

```
my-sidecar/
├── go.mod          # module my-sidecar；require github.com/houzch/swiftmq（或 replace 指到本地源码）
├── main.go         # 启动 sidecar.Server
└── handler.go      # 实现 sidecar.Handler
```

```bash
go mod init my-sidecar
go get github.com/houzch/swiftmq@v1.1.01
# 本地联调时可改用 replace 指向源码：
#   go mod edit -replace github.com/houzch/swiftmq=../swiftmq
```

> `replace`로 연동 디버깅할 때는 플러그인과 커널이 **동일한 소스**여야 합니다. 그렇지 않으면 API 버전(`v1`)은 일치해도 타입은 다를 수 있습니다.

### 3.2 `Handler` 구현(세 개의 메서드)

플러그인 프로세스의 전체 비즈니스 표면은 `sidecar.Handler`의 `Hello` / `Call` / `Open`입니다.
핸드셰이크, 하트비트, 멀티플렉싱, 청킹은 모두 `pkg/sidecar`가 처리하므로 프레임을 건드릴 필요가 없습니다.

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

### 3.3 데이터 플레인: `Stream` 읽기/쓰기

`sidecar.Stream`은 `io.ReadWriteCloser`를 구현하므로 "하나의 연결"로 그대로 쓰면 됩니다:

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

요점:

- 클라이언트 연결 하나 = 스트림 하나; 플러그인은 `Handler.Open`에서 즉시 송수신을 시작할 수 있습니다.
- 큰 메시지는 라이브러리가 자동 청킹합니다(프레임당 ≤ 64 KiB). **메모리 사용량은 메시지 크기와 무관합니다**.
- 백프레셔: 단일 스트림의 수신 버퍼에는 상한이 있고, 버퍼가 가득 차면 해당 연결의 **디스패치 고루틴**을 차단합니다(모든 스트림이 함께 대기) ——
  이는 "메모리 예측 가능성"과 "단일 스트림 속도 제한"의 트레이드오프입니다. 자세한 내용은 §7을 참조하세요.

### 3.4 커널 시맨틱 브리지 사용(`session.*`)

플러그인이 커널의 큐 / 라우팅 / 권한 / 확인 시맨틱을 재사용하도록(직접 만드는 대신) 하려면 **역방향 호출**을 사용합니다.
스트림에서 `session.open` → 다른 `session.*` → (`session.close`) 순서로 사용하세요:

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

**역방향 호출 메서드 일람**(`pkg/sidecar/bridge.go`의 상수 → 와이어 이름):

| 그룹 | 와이어 이름(상수) | 설명 |
| --- | --- | --- |
| 인증 | `core.authenticate`(`MethodCoreAuthenticate`) | **가장 먼저 해야 함**: 프로토콜의 자격 증명을 커널에 넘겨 검증받음; 파라미터 `{stream, mechanism, response}`, 반환 `{user}` |
| 세션 | `session.open`(`MethodSessionOpen`) | 스트림에서 특정 vhost의 세션을 염; **인증 이후**에만 가능 |
|  | `session.close`(`MethodSessionClose`) | 해당 스트림의 세션 해제(소비자 취소, 독점 큐 삭제) |
| 익스체인지 | `session.declare_exchange` / `session.delete_exchange` | 추가/삭제(수동 선언 중 없으면 → `KindNotFound`) |
|  | `session.bind_exchange` / `session.unbind_exchange` | 익스체인지 ↔ 익스체인지 바인딩 |
| 큐 | `session.declare_queue` / `session.delete_queue` | 추가/삭제; `name`이 비어 있으면 서버가 생성 |
|  | `session.bind_queue` / `session.unbind_queue` | 큐 ↔ 익스체인지 바인딩 |
|  | `session.purge_queue` | 준비된 메시지 비우기(미확인 메시지 제외) |
| 발행 | `session.publish` | `{routed, rejected}` 반환; 영속화는 응답 전에 완료 |
| 가져오기 | `session.get` | 능동적으로 한 건 가져옴; `found=false`는 큐가 비었음을 의미 |
| 소비 | `session.consume` / `session.cancel` | 소비자 등록 / 취소 |
| 정산 | `session.settle` | 전달 한 건 정산(`ack` / `requeue` / `reject`) |
| **정방향** | `session.deliver` | **커널 → 플러그인**: 전달 푸시백(당신의 `Handler.Call`에서 처리) |

**반드시 지켜야 할 네 가지 규약**:

1. **먼저 `core.authenticate`**: 연결의 커널 조작면은 인증 전에는 신원이 없으므로
   이때 `session.open`은 거부됩니다(`ACCESS_REFUSED - access to vhost '/' refused for user ''`).
   플러그인이 자신의 프로토콜에서 자격 증명을 추출할 책임이 있습니다. 인증 로직과 사용자 테이블은 여전히 커널에 있으며, 플러그인은 비밀번호 저장소에 접근하지 않습니다.
2. **그다음 `session.open`**: 세션을 열지 않고 다른 메서드를 호출하면 커널이 `KindPreconditionFailed`("스트림 N은 아직 세션을 열지 않음")를 반환합니다.
3. **각 전달은 정확히 한 번 정산**: `Ack` / `Requeue` / `Reject` 중 하나를 선택합니다.
   `Ack`과 `Reject` 모두 메시지를 버리지만, **`Reject`만 데드레터로 갑니다**.
4. **미정산 전달은 유실되지 않음**: 스트림이 끝나거나(클라이언트 연결 끊김 / `Handler.Open` 반환) 플러그인 연결이 끊기면,
   커널이 모든 미정산 전달을 **"큐 복귀"로 처리**하여 메시지가 정체되지 않게 합니다.

**오류 복원**: 커널의 `*plugin.Error`는 브리지를 거쳐 `*sidecar.RPCError`(필드 `Kind` / `Text`)로 도착하며,
분류를 문자열 속에 잃지 않고 분류가 있는 `plugin.Error`로 복원할 수 있습니다:

```go
if err := br.Call(ctx, sidecar.MethodSessionPublish, params, nil); err != nil {
	var rpc *sidecar.RPCError
	if errors.As(err, &rpc) {
		return plugin.Errorf(plugin.ErrorKind(rpc.Kind), "%s", rpc.Text)
	}
	return err
}
```

흔한 `Kind`와 내장 프로토콜의 매핑(클라이언트에 오류를 어떻게 돌려줄지 판단하는 데 사용):

| `plugin.ErrorKind` | 시맨틱 | AMQP 0-9-1 매핑 |
| --- | --- | --- |
| `KindNotFound` | 객체가 존재하지 않음 | 404 NOT_FOUND(Channel 닫기) |
| `KindPreconditionFailed` | 파라미터가 기존 객체와 불일치 / 세션 미개설 | 406 PRECONDITION_FAILED(Channel 닫기) |
| `KindAccessRefused` | 권한 부족 / 예약 이름 | 403 ACCESS_REFUSED(Channel 닫기) |
| `KindResourceLocked` | 독점 리소스가 점유됨 | 405 RESOURCE_LOCKED(Channel 닫기) |
| `KindInvalidPath` | vhost가 존재하지 않음 | 402 INVALID_PATH(연결 닫기) |
| `KindNotImplemented` | 능력 미구현 | 540 NOT_IMPLEMENTED(연결 닫기) |
| `KindInternal` | 커널 내부 오류 | 541 INTERNAL_ERROR(연결 닫기) |

**메시지 타입 보존의 경계**: 속성 테이블(`MessageDTO.Properties.Headers`)은 JSON을 거치므로,
AMQP field-table처럼 **`int32` / `double`을 구분하는 수치 타입 정보를 얻을 수 없습니다**.
엄격한 타입 보존이 필요하면 이런 정보를 메시지 본문(원시 바이트)에 담아 직접 전달하세요.

### 3.5 플러그인 측 상태와 로그

- 외부 플러그인의 **상태는 "연결이 살아 있는지"로 결정**되며, 플러그인이 스스로 보고할 필요가 없습니다(내장 플러그인의 `StateReporter`는 외부 프로세스에 적용되지 않습니다).
- 로그: `sidecar.ServerOptions.Logger`는 플러그인 프로세스의 stdout/stderr로 출력됩니다;
  **커널이 `spawn`으로 기동한 경우 이 출력은 커널이 커널 로그로 포워딩**합니다(`plugin` 태그 포함). 통합 수집에 편리합니다.
- 독립 배포(spawn 아님) 시에는 플러그인 로그를 자신의 방식으로 수집하세요.

### 3.6 커널 없이도 자체 테스트

`Handler`는 평범한 Go 인터페이스이므로, 단위 테스트에서 직접 인스턴스화하고 `Hello` / `Call` / `Open`을 호출해 비즈니스 로직을 커버할 수 있습니다. 네트워크를 띄울 필요가 없습니다:

```go
func TestHelloRejectsWrongName(t *testing.T) {
	h := &handler{}
	if _, err := h.Hello(context.Background(), sidecar.Hello{Plugin: "other", APIVersion: "v1"}); err == nil {
		t.Fatal("插件名不一致时应当拒绝")
	}
}
```

엔드투엔드 검증은 §5를 참조하세요.

---

## 4. 다른 언어로 개발(와이어 프로토콜 규격)

`pkg/sidecar`는 의존성 없는 대외 계약입니다. 와이어 프로토콜 자체는 매우 단순하여 어떤 언어로든 구현할 수 있습니다.
연동하려면 다음의 "바이트 수준" 규약을 구현해야 합니다(소스는 `pkg/sidecar/frame.go`, `proto.go` 참조).

> **완전한 예제 프로젝트가 포함된 언어별 가이드를 제공합니다**(예제는 모두 핸드셰이크 → 인증 → 시맨틱 브리지 → 전달/정산 → 바이트 스트림까지 실측 통과):
>
> | 언어 | 가이드 | 예제 프로젝트(작업 공간 `swiftmq-plugin/`) |
> | --- | --- | --- |
> | Python | [plugin-development-python.md](plugin-development-python.md) | `python/sidecar_plugin.py`(표준 라이브러리만) |
> | Node.js | [plugin-development-nodejs.md](plugin-development-nodejs.md) | `nodejs/index.js`(표준 라이브러리만) |
> | PHP | [plugin-development-php.md](plugin-development-php.md) | `php/sidecar_plugin.php`(표준 라이브러리만) |
> | Java | [plugin-development-java.md](plugin-development-java.md) | `java/SidecarPlugin.java`(단일 파일, JDK만) |
>
> Go의 완전한 참조 구현은 독립 테스트 프로젝트 `swiftmq-test/test/integration/echosidecar/`를 참조하세요(이는 `pkg/sidecar.Server`를 직접 사용하므로
> 아래 바이트 계층 세부 사항을 신경 쓸 필요가 없습니다).

**프레임 형식**(모든 프레임 공통):

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)，即长度字段**包含** kind 字节；单帧上限 16 MiB。
```

**프레임 타입 `kind`**:

| kind | 이름 | 방향 | 페이로드 |
| --- | --- | --- | --- |
| 1 | Hello | 커널 → 플러그인 | JSON `Hello` |
| 2 | HelloAck | 플러그인 → 커널 | JSON `HelloAck` |
| 3 | Ping | 커널 → 플러그인 | 비어 있음 |
| 4 | Pong | 플러그인 → 커널 | 비어 있음 |
| 5 | Call | 양방향 | JSON `Call` |
| 6 | Reply | 양방향 | JSON `Reply` |
| 7 | Open | 커널 → 플러그인 | JSON `Open` |
| 8 | OpenAck | 플러그인 → 커널 | JSON `OpenAck` |
| 9 | Data | 양방향 | `u32 BE stream` + 원시 바이트 |
| 10 | Close | 양방향 | JSON `Close` |

**컨트롤 플레인 JSON 구조**(필드 이름은 `proto.go`와 일치).

> 이 단락은 **와이어 프로토콜 메시지 예시**입니다(한 블록에 여러 메시지를 순서대로 제시하므로 `//`로 설명을 구분),
> **`swiftmqd.json`에 그대로 쓸 수 있는 설정이 아닙니다**.

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

**반드시 지켜야 할 시맨틱**:

- **핸드셰이크**: 커널이 먼저 `Hello`를 보내고, 플러그인은 반드시 `HelloAck` 프레임 하나를 회신해야 합니다.
  커널은 `HelloAck.name == 설정의 플러그인 이름`이고 `HelloAck.api_version == 커널의 APIVersion`인지 검증합니다;
  `deny`가 비어 있지 않으면 접속 거부로 간주합니다(플러그인 격리).
- **하트비트**: 커널은 기본적으로 2초마다 `Ping`을 보내고, 플러그인은 8초 안에 `Pong`을 회신해야 합니다; 플러그인 측은 24초 동안 어떤 프레임도 받지 못하면 스스로 연결을 닫을 수 있습니다.
- **두 개의 ID 공간**: `Call`의 `reverse`가 방향을 구분하며, 두 방향이 각각 1부터 증가하므로
  `Reply`는 **반드시 `reverse`를 함께 실어야** 합니다. 그렇지 않으면 응답이 잘못된 대기자에게 전달됩니다.
- **데이터 플레인은 base64 아님**: 메시지 본문 같은 큰 데이터는 `Data` 프레임 페이로드(`stream` + 원시 바이트)에 직접 담고, 필요에 따라 청킹합니다.

> Go를 쓴다면 `pkg/sidecar`를 직접 사용하세요. 위 세부 사항을 직접 구현할 필요가 없습니다.

---

## 5. 함께 실행하기: 연동, 대외 서비스, 패키징 ★

이 절은 "개발을 마친 뒤 어떻게 SwiftMQ에 연결하고, 어떻게 대외 서비스를 제공하는가"를 다룹니다.

### 5.1 설정에 플러그인 선언

외부 플러그인은 **전적으로 설정으로 관리**되며, 커널은 이를 위해 어떤 코드도 바꿀 필요가 없습니다. `swiftmqd.json`의 `plugins` 섹션에 항목을 추가하세요
(**실제 설정은 표준 JSON이며 주석을 넣을 수 없습니다**):

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

항목별 설명(필드 목록은 아래 표 참조):

- `plugins.<플러그인 이름>`의 키 이름은 **플러그인이 스스로 보고한 `HelloAck.name`과 일치해야** 하며, 그렇지 않으면 핸드셰이크가 거부됩니다.
- `builtin: false`: 외부 플러그인을 명시적으로 선언(쓰지 않으면 관리면이 내장으로 표시).
- `enabled`: 끄면 = 프로세스를 기동하지 않고 리스너를 만들지 않음.
- `required: true`이면 기동 실패 시 커널 기동이 막힙니다 —— 외부 플러그인에는 켜지 마세요.
- `address`는 **커널이 연결하러 가는 주소**입니다(커널이 클라이언트); `spawn`이 비어 있지 않으면 커널이 대신 프로세스를 기동합니다.
- `protocols[].prefix`는 **반드시 비어 있지 않아야** 합니다(스니핑 규칙은 §5.3 참조).
- `listeners`는 해당 프로토콜의 **대외 포트이며 커널이 엽니다**(클라이언트는 커널에 연결).

필드 일람:

| 필드 | 필수 | 설명 |
| --- | --- | --- |
| `sidecar.address` | ✅ | 플러그인 프로세스 주소: `tcp://host:port` 또는 `unix:///path` |
| `sidecar.spawn` | ✕ | 커널이 대신 기동하는 명령줄(첫 요소가 실행 파일); **비워 두면 = 커널은 연결만 하고 기동하지 않음**, 프로세스는 직접 관리 |
| `sidecar.restart` | ✕ | `always`(기본, 연결 끊김/크래시 후 자동 복구) 또는 `never`(`down`만 표시하고 운영자 개입 대기) |
| `sidecar.protocols[].name` | ✅ | 프로토콜 이름(전역 유일, 스니핑 우선순위에 참여) |
| `sidecar.protocols[].prefix` | ✕ | 스니핑 프리픽스(ASCII); **비어 있으면 = 스니핑에 참여하지 않음** |
| `sidecar.protocols[].listeners[]` | ✕ | 해당 프로토콜의 대외 리스너(`name` + `addr`), 커널이 생성 |
| `sidecar.handshake_timeout_seconds` | ✕ | 핸드셰이크 타임아웃 재정의(기본 5초) |
| `sidecar.heartbeat_seconds` | ✕ | 하트비트 간격 재정의(기본 2초) |

> **플러그인 이름과 프로토콜 이름**: 둘은 **달라도 됩니다**(예: 플러그인 `my-sidecar`가 프로토콜 `myproto` 제공).
> 핫 비활성화는 먼저 플러그인 이름으로 등록된 모든 프로토콜을 찾은 다음 그 프로토콜의 포트를 닫으므로, 굳이 같은 이름을 쓸 필요는 없습니다.

### 5.2 세 가지 연동 방식

| 방식 | 설정 | 적용 대상 |
| --- | --- | --- |
| **동일 호스트 + 커널 기동(spawn)** | `spawn: [...]`, `address`가 그것이 리슨하는 주소를 가리킴 | 동일 머신 배포, 단일 컨테이너; 가장 간편하고 커널이 기동과 회수를 담당 |
| **동일 호스트 + 직접 관리(dial)** | `spawn: []`, `address`가 이미 실행 중인 프로세스를 가리킴 | systemd / supervisor로 플러그인 수명 주기 관리 |
| **크로스 호스트 / 크로스 컨테이너(dial, 반드시 tcp)** | `spawn: []`, `address: "tcp://<서비스 이름>:19001"` | 플러그인과 커널을 컨테이너 / 머신 단위로 분리 배포 |

주소 선택:

- **동일 머신에서는 unix socket 권장**(`unix:///tmp/my-sidecar.sock`): TCP 포트를 차지하지 않고, 호스트 포트 점유 영향도 받지 않습니다.
  단, socket 경로는 커널 프로세스(컨테이너에서는 비 root `swiftmq` 사용자)가 쓸 수 있어야 합니다.
- **크로스 컨테이너는 반드시 TCP**여야 하며, 플러그인 프로세스는 `0.0.0.0`을 리슨하고 `address`에는 **컨테이너 네트워크의 서비스 이름**을 사용합니다.

> 방향을 헷갈리지 마세요: **플러그인이 리슨하는 주소** = `address`; **클라이언트에 개방하는 포트** = `protocols[].listeners`.

### 5.3 대외 서비스: `prefix`로 식별됨

접속 계층이 연결을 분배할 때는 **스니핑 결과만 봅니다**: 활성화된 각 프로토콜에 등록 순서대로 `Sniff(peek)`를 물어보고(peek는 최대 8바이트),
매칭된 쪽이 이 연결을 인수합니다. 따라서:

1. **`prefix`는 반드시 비어 있지 않아야 합니다**(ASCII, ≤ 8바이트). 클라이언트가 보낸 처음 몇 바이트가 이와 같아야 연결이 플러그인에 넘겨집니다.
   예: `"prefix": "PY"` → 클라이언트 첫 바이트가 `PY`여야 함(프리픽스를 프로토콜의 매직 헤더로 삼을 수 있습니다).
2. **`prefix`가 비어 있으면 스니핑에 참여하지 않습니다**: 이런 연결은 플러그인에 **넘겨지지 않습니다**(실측: 리슨 포트의 연결이 즉시 끊김).
   따라서 빈 `prefix`는 "다른 프로토콜이 같은 포트에서 대신 전달해 주는" 시나리오에만 적합하며, **전용 포트용으로 쓰지 마세요**.
3. `listeners[].addr`는 "어느 포트에서 대외 개방할지"를, `prefix`는 "이 연결이 당신 것인지"를 결정합니다 ——
   둘은 함께 써야 합니다: **전용 포트에도 비어 있지 않은 `prefix`를 주어야 합니다**(이것이 커널 예시 설정에서
   `echo-sidecar`가 `prefix: "ECHO"`와 `listeners: [":1885"]`를 함께 쓰는 이유이기도 합니다).
4. 스니핑은 프로토콜 등록 순서로 매칭되며 **먼저 매칭된 쪽이 유효**합니다: 여러 플러그인이 공존할 때 프리픽스는 구분력이 있어야 합니다(예: 모두 같은 바이트로 시작하면 서로 가립니다).

### 5.4 리슨 주소와 TLS 재정의

- 대외 리슨 주소는 **두 곳**에서 줄 수 있습니다: `sidecar.protocols[].listeners[].addr`(기본)와
  `listeners.<프로토콜 이름>`(프로토콜 이름으로 전체 재정의). 둘 다 존재하면 `listeners.<프로토콜 이름>`이 우선합니다.
- TLS가 필요하면 `listeners.<프로토콜 이름>[i].tls`에 인증서를 지정합니다(필드는 내장 프로토콜과 동일).
  아래는 `listeners` 조각입니다(**표준 JSON, 주석 불가**): 1번은 평문, 2번은 TLS.

```json
"listeners": {
  "myproto": [
    { "addr": ":19002" },
    { "addr": ":19003", "tls": { "cert_file": "/etc/swiftmq/tls/cert.pem",
                                 "key_file":  "/etc/swiftmq/tls/key.pem" } }
  ]
}
```

> TLS는 **커널**이 리스너 측에서 종료하며, 플러그인 프로세스가 받는 것은 평문 스트림입니다 —— 플러그인은 TLS를 처리할 필요가 없습니다.

### 5.5 패키징: 플러그인을 커널과 함께 실행

**방법 A —— 같은 이미지에 넣기**("커널과 함께 릴리스"하는 플러그인에 권장): `swiftmq/Dockerfile`의 런타임 스테이지에 한 줄을 추가:

```dockerfile
COPY --from=<构建你的插件的 stage> /out/my-sidecar /usr/local/bin/my-sidecar
# 或直接拷预编译产物：
# COPY bin/my-sidecar /usr/local/bin/my-sidecar
```

그다음 설정에 `spawn: ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"]`,
`address`도 같은 값으로. 커널이 기동할 때 이를 띄웁니다.

**방법 B —— 바이너리 마운트**(이미지 변경 없음, 연동 디버깅에 적합):

```yaml
services:
  swiftmq:
    image: houzch/swiftmq:1.1.01
    command: ["-config", "/etc/swiftmq/swiftmqd.json", "-log-level", "info"]
    volumes:
      - ./configs/swiftmqd.json:/etc/swiftmq/swiftmqd.json:ro
      - ./bin/my-sidecar:/usr/local/bin/my-sidecar:ro   # 插件二进制
    ports:
      - "5672:5672"        # AMQP（内置）
      - "15672:15672"      # 管理面
      - "19002:19002"      # ← 你的协议对外端口（由内核监听）
```

설정은 unix socket 사용(추가 포트 점유 회피). 아래는 `plugins.my-sidecar` 안의 `sidecar` 조각
(**표준 JSON, 주석 불가**; `prefix`는 여전히 비어 있지 않아야 함, §5.3 참조):

```json
"sidecar": {
  "address": "unix:///tmp/my-sidecar.sock",
  "spawn": ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"],
  "protocols": [{ "name": "myproto", "prefix": "MP", "listeners": [{ "name": "myproto", "addr": ":19002" }] }]
}
```

**방법 C —— 독립 컨테이너**(플러그인 개별 릴리스 / 독립 확장):

```yaml
services:
  swiftmq:
    image: houzch/swiftmq:1.1.01
    volumes: ["./configs/swiftmqd.json:/etc/swiftmq/swiftmqd.json:ro"]
    ports: ["5672:5672", "15672:15672", "19002:19002"]
    depends_on: [my-sidecar]

  my-sidecar:
    image: my-sidecar:0.1.0
    command: ["-addr", "tcp://0.0.0.0:19001"]   # 对外（对内）监听 0.0.0.0
```

설정에서 `spawn: []`(커널은 연결만, 기동 안 함), `address: "tcp://my-sidecar:19001"`(compose 서비스 이름).

### 5.6 기동과 검증

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

`plugins show`에서 `state`와 `RuntimeNote`를 중점적으로 보세요:
`failed`는 실패 사유를 동반합니다(핸드셰이크 거부 / 포트 기동 불가…); `down`은 연결 끊김 사유를 동반합니다(프로세스 크래시 / 연결 끊김).

### 5.7 런타임 운영

| 작업 | 명령 / 인터페이스 | 효과 |
| --- | --- | --- |
| 핫 비활성화 | `swiftmqctl plugins disable my-sidecar` 또는 `PUT /api/plugins/my-sidecar/disable` | **해당 플러그인의 대외 리스너 닫기**(능력 수준 비활성화); 커널과 다른 플러그인은 영향 없음 |
| 핫 활성화 | `swiftmqctl plugins enable my-sidecar` | 리스너 다시 열기; 이전 기동이 실패했다면 한 번 재시도 |
| 상태 보기 | `swiftmqctl plugins list/show` | 상태 + 실패/연결 끊김 사유 |
| 커널 종료 | — | 플러그인 연결 끊기, 브리지 세션 회수, **커널이 `spawn`한 자식 프로세스 종료** |

> 핫 비활성화는 "능력"(리스닝 포트)만 닫고, `spawn`으로 띄운 플러그인 프로세스를 **죽이지 않습니다**; 프로세스 회수는 커널 종료 시 발생합니다.

### 5.8 관리 콘솔에 진입점 제공(선택)

플러그인에 자체 조작 UI가 있으면 `plugins.<플러그인 이름>` 섹션에 `console_url`(관리 UI 주소, 나머지 필드는 §5.1 참조)을 추가하세요.
아래는 `plugins.my-sidecar` 항목만 그린 것입니다(**표준 JSON, 주석 불가**; `sidecar` 섹션 내용은 §5.1과 동일):

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

- 관리 콘솔의 **플러그인 관리** 페이지(`GET /api/plugins`의 `console_url` 필드에서 데이터를 가져옴)는 이런 플러그인에
  "관리 UI 열기" 버튼을 표시하며, **새 탭에서 엽니다**.
- `console_url`을 선언하지 않으면 버튼이 비활성화되고, 툴팁 "이 플러그인은 관리 UI를 제공하지 않습니다"가 표시됩니다.
- 이것은 단지 **배포자가 설정에 적어 넣은 메타데이터**입니다: 플러그인 API(`pkg/plugin`)에 속하지 않고, 플러그인 기동/정지에 참여하지 않으며,
  UI 자체는 플러그인이 직접 호스팅합니다(플러그인 프로세스 안일 수도 있고, 임의의 독립 서비스일 수도 있음).

---

## 6. 수명 주기와 장애 허용 매트릭스

| 상황 | 커널 동작 | 플러그인 측 영향 |
| --- | --- | --- |
| 플러그인 프로세스 미기동 / 핸드셰이크 거부 | 8초 내 연결 재시도, 계속 실패하면 `failed`로 표시하고 격리(커널 기동을 막지 않음) | 없음 |
| `spawn`으로 띄운 프로세스 종료 | 로그 기록; `down` 표시; `restart` 정책에 따라 백오프 재연결 / 재기동 | 새 프로세스가 핸드셰이크 다시 수행 |
| 플러그인 프로세스 크래시(런타임) | 커널은 영향 없음; `down` + 백오프 재연결 | `pkg/sidecar.Server`가 해당 연결을 닫음 |
| 커널이 `kill -9`됨 | — | 플러그인 측은 유휴 타임아웃(기본 24초 무프레임)으로 스스로 연결을 회수, 좀비를 남기지 않음 |
| 커널 정상 종료 | `Stop` 호출: 연결 끊기, 미정산 전달 회수(큐 복귀), 자식 프로세스 `Kill` | SIGKILL 수신 |
| 플러그인 연결이 끊긴 동안의 전달 | 미정산 전달은 모두 **큐 복귀**되며 유실되지 않음 | — |
| 클라이언트 연결 끊김 / `Open` 반환 | 해당 스트림을 닫고 그 스트림의 세션과 소비자 해제 | `Stream.Read`가 EOF 반환 |

---

## 7. 레드라인과 알려진 경계

**레드라인**

1. 플러그인은 `pkg/sidecar`(및 선택적 `pkg/plugin`)에만 의존할 수 있습니다; 커널 `internal/**`에 **의존해서는 안 됩니다**.
2. 플러그인 이름은 설정과 일치해야 하고 `APIVersion`은 커널과 일치해야 합니다. 그렇지 않으면 접속할 수 없습니다(이는 "조용히 실행되지만 적용되지 않음"을 방지합니다).
3. `session.*`를 사용할 때: **먼저 `core.authenticate`, 그다음 `session.open`**, 각 전달은 **정확히 한 번 정산**합니다.
4. `protocols[].prefix`는 반드시 비어 있지 않아야 합니다. 그렇지 않으면 연결이 플러그인에 넘겨지지 않습니다(§5.3 참조).
5. `Hello`에서의 거부는 **반드시 명시적으로 error를 반환**해야 합니다(조용히 넘어가지 마세요) —— 그렇지 않으면 커널은 "연결이 닫힘"만 볼 수 있어 원인을 찾을 수 없습니다.

**알려진 경계**

- **스니핑은 커널 측에 있음**: 외부 플러그인은 스니핑 함수를 커스터마이즈할 수 없고 `prefix`(ASCII, ≤ 8바이트)로만 매칭할 수 있습니다;
  `prefix`가 비어 있으면 "연결을 받지 못함"입니다(§5.3 참조).
- **데이터 플레인은 로컬 프록시를 거침**: fd 전달이 없고(Windows에는 `SCM_RIGHTS` 없음), 인프로세스보다 메모리 복사가 한 번 더 발생합니다;
  역방향 호출도 매번 로컬 RPC가 한 번 더 발생합니다.
- **속성 테이블 타입이 퇴화**: `Properties.Headers`는 JSON을 거치므로 `int32` / `double` 같은 구분이 사라집니다(§3.4 참조).
- **단일 스트림 백프레셔가 연결 전체에 영향**: 한 스트림의 수신 버퍼가 가득 차면 그 연결의 디스패치 고루틴을 차단합니다; 스트림별 속도 제한은 후속 최적화입니다.
- **인증 실패는 텍스트만 전달**: 커널 인증 실패는 `*plugin.AuthError`(`plugin.ErrorKind`와 다른 분류 체계)이며,
  브리지를 거쳐 플러그인에 도달할 때는 텍스트만 있습니다; 플러그인은 자신의 규약에 따라 프로토콜 오류 코드로 매핑해야 합니다.
- **`net.listen` 능력만 실제로 유효**: `store.read/write`, `http.route`, `cluster.metadata.write`,
  `auth.verify`는 **예약 슬롯**이며, 선언해도 감사에만 참여합니다(§5.6의 거버넌스 표시 참조). 현재 대응하는 확장 지점이 없습니다.

---

## 8. 문제 해결 FAQ

| 증상 | 원인과 처리 |
| --- | --- |
| 상태 `failed`, 사유에 "플러그인 이름 불일치" | 설정의 플러그인 이름 ≠ `HelloAck.name`; 일치시키세요 |
| 상태 `failed`, 사유에 "API 버전 불일치" | `HelloAck.api_version` ≠ 커널 `APIVersion`; 일치시키세요 |
| 상태 `failed`, 사유에 "핸드셰이크 거부" | 플러그인 `Hello`가 error를 반환(`deny`); 커널 로그에 포워딩된 플러그인 출력을 확인하세요 |
| 상태 `failed`, 사유에 "외부 플러그인 연결 실패" | 프로세스가 안 뜸 / `address` 오타 / socket 경로 쓰기 불가(컨테이너에서는 `swiftmq` 사용자 권한 주의) |
| 상태 `down` | 플러그인 프로세스 크래시 또는 연결 끊김; `restart=always`는 자동 재연결, `never`는 수동 기동 필요 |
| 포트가 안 열림 / 클라이언트 연결 불가 | `protocols[].listeners` 미설정 또는 주소가 `listeners.<프로토콜 이름>`에 덮임; 두 곳을 대조하세요 |
| 클라이언트가 다른 포트에 연결 후 즉시 끊김 | 그 포트가 프로토콜과 매칭되지 않음(`prefix`가 비었거나 프리픽스 불일치); 프로토콜에 비어 있지 않은 `prefix` 설정(§5.3 참조) |
| `ACCESS_REFUSED - ... for user ''` 발생 | 시맨틱 브리지 전에 **인증이 없음**; `core.authenticate`를 먼저 호출하고 `session.open` |
| 역방향 호출에서 "스트림 N이 아직 세션을 열지 않음" | 먼저 `core.authenticate`, 그다음 `session.open`, 그 후에야 다른 `session.*` 호출 |
| 소비 전달을 받지 못함 | 전달은 **정방향 호출** `session.deliver`로 당신의 `Call`에 도착합니다; 해당 메서드를 처리했는지 확인하세요 |
| 플러그인은 컨테이너 밖, 커널은 컨테이너 안, 연결 불가 | `address`에 `tcp://host.docker.internal:<port>` 사용(또는 플러그인도 컨테이너에 넣고 서비스 이름 사용); 플러그인은 `0.0.0.0`을 리슨해야 함 |

---

## 9. 참고(소스 인덱스)

| 보고 싶은 것 | 파일 |
| --- | --- |
| 와이어 프로토콜과 양측 구현(**개발 필독**) | [`pkg/sidecar/`](../../../pkg/sidecar/): `frame.go`(프레임), `proto.go`(메시지), `server.go`(플러그인 측), `client.go`(커널 측), `bridge.go`(`session.*` 계약), `stream.go`(스트림) |
| 커널 측 sidecar 호스트(접속/재연결/프록시/상태) | [`internal/plugin/sidecar/sidecar.go`](../../../internal/plugin/sidecar/sidecar.go) |
| 커널 측 시맨틱 브리지(`session.*` → `plugin.Session`) | [`internal/plugin/sidecar/bridge.go`](../../../internal/plugin/sidecar/bridge.go) |
| 커널 세션 조작면 타입(`Message` / `Delivery` / `ErrorKind`) | [`pkg/plugin/session.go`](../../../pkg/plugin/session.go) |
| 리스닝, 스니핑, 플러그인별 핫 기동·정지 | [`internal/transport/server.go`](../../../internal/transport/server.go) |
| 플러그인 수명 주기와 거버넌스(격리/상태/감사) | [`internal/plugin/manager.go`](../../../internal/plugin/manager.go), [`registry.go`](../../../internal/plugin/registry.go) |
| 설정 항목과 예시(sidecar 섹션 포함) | [`internal/config/config.go`](../../../internal/config/config.go), [`configs/swiftmqd.json`](../../../configs/swiftmqd.json) |
| 프로세스 조립(sidecar가 커널에 어떻게 조립되는지) | [`cmd/swiftmqd/main.go`](../../../cmd/swiftmqd/main.go) |
| Go 참조 구현(`pkg/sidecar.Server` 사용, `session.*` 브리지와 `core.authenticate` 포함) | 독립 테스트 프로젝트 `swiftmq-test/test/integration/echosidecar/` |
| **언어별 가이드 + 예제 프로젝트** | 이 디렉터리의 `plugin-development-python.md` / `-nodejs.md` / `-php.md` / `-java.md`; 예제는 **작업 공간** `swiftmq-plugin/{python,nodejs,php,java}/` |
