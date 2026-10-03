# SwiftMQ 외부 프로세스 플러그인 개발 가이드 —— Python

> **대상**: Python으로 SwiftMQ 외부 프로세스 플러그인(sidecar)을 작성하는 개발자.
> **먼저 읽기**: [외부 프로세스 플러그인(sidecar) 개발 가이드](plugin-development.md) —— 멘탈 모델, 설정 필드, 와이어 프로토콜 총표를 다룹니다;
> 이 문서는 **Python에서 어떻게 구현하는지**와 로컬에서 실측한 단계 및 결과만 다룹니다.
> **예제 프로젝트**: 작업 공간 `swiftmq-plugin/python/sidecar_plugin.py`(표준 라이브러리만, 서드파티 의존성 없음).

---

## 1. 실행하면 어떤 모습인가

```
内核进程 ──dial(tcp 本机地址)──► 你的 Python 进程（sidecar.NewServer 的等价物：一个 TCP 服务端）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

세 가지 요점(헷갈리기 쉬우니 먼저 기억하세요):

1. **당신의 프로세스는 서버**: 로컬 주소 하나를 리슨하고 커널이 연결해 오기를 기다립니다(`plugins.<플러그인 이름>.sidecar.address`).
2. **대외 비즈니스 포트는 커널이 엽니다**: 클라이언트가 연결하는 것은 커널의 포트이고, 바이트는 커널이 당신에게 프록시합니다(`protocols[].listeners`).
3. **`prefix`는 반드시 비어 있지 않아야 합니다**: 커널이 프리픽스 스니핑으로 "이 연결을 누구에게 줄지" 결정합니다. `prefix`가 비어 있으면 **스니핑에 참여하지 않으며**,
   자체 리스너에서도 연결이 당신에게 넘겨지지 않습니다(실측: 연결이 즉시 끊김). 프리픽스 길이는 ≤ 8바이트, ASCII.

---

## 2. 세 단계로 실행

### 1단계: 설정에 플러그인 선언

`swiftmqd.json`(**실제 설정은 표준 JSON이며 주석을 넣을 수 없습니다**):

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

- `address`는 **커널이 당신에게 연결하는 주소**입니다(커널이 클라이언트, 플러그인이 서버).
- `spawn`을 비워 두면 = 커널은 연결만 하고 기동하지 않으며, 프로세스는 직접 관리합니다(systemd / supervisor / compose).
- `prefix`는 **반드시 비어 있지 않아야 합니다**: 클라이언트가 보낸 첫 바이트가 이것으로 시작해야 합니다(커널이 프리픽스 스니핑으로 연결을 누구에게 줄지 결정).
- `listeners`는 대외 포트이며 커널이 엽니다(클라이언트는 커널에 연결하지 당신에게 연결하지 않습니다).
- 크로스 컨테이너 배포 시 `address`에 **서비스 이름**을 사용하고(예: `tcp://py-sidecar:19001`), 플러그인은 `0.0.0.0`을 리슨해야 합니다.

### 2단계: 기동

```bash
# 由内核 spawn 时不用手起；手动调试：
python sidecar_plugin.py -addr 0.0.0.0:19001 -name py-sidecar -session-demo
```

### 3단계: 검증

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins | python -m json.tool   # state 应为 enabled
printf 'PYhello\n' | nc 127.0.0.1 19002                                       # 应回显 PYhello
```

---

## 3. 구현 요점

### 3.1 프레이밍(유일하게 직접 정확히 구현해야 하는 바이트 계층)

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（长度字段**包含** kind 字节）；单帧上限 16 MiB。
```

데이터 프레임 페이로드 = `4바이트 빅엔디언 스트림 번호 + 원시 바이트`; 컨트롤 플레인 페이로드는 JSON입니다.

```python
def read_frame(sock):
    head = read_exact(sock, 5)
    if len(head) < 5:
        return None
    (length,) = struct.unpack(">I", head[:4])
    payload = read_exact(sock, length - 1)
    return head[4], payload          # (kind, payload)
```

### 3.2 핸드셰이크와 하트비트

커널이 연결하면 **먼저 Hello를 보내고**, 당신은 반드시 `HelloAck` 프레임을 회신해야 합니다; 커널이
`name == 설정의 플러그인 이름`이고 `api_version == 커널의 APIVersion`(현재 `v1`)인지 검증합니다.
이후 커널이 2초마다 `Ping`을 보내면 `Pong`만 회신하면 됩니다(회신하지 않으면 죽은 것으로 판정).

### 3.3 논리 스트림

`kindOpen` 도착 → **먼저 `OpenAck` 회신**, 그다음 서비스 시작; `kindData` 도착 → 그대로(또는 당신의 프로토콜로 파싱 후) `kindData`로 기록;
처리 종료 → `kindClose` 전송. 스트림 하나 = 클라이언트 연결 하나.

### 3.4 역방향 호출과 커널 시맨틱 브리지

플러그인 → 커널 호출은 `kindCall`에 `"reverse": true`로 가며, 커널은 같은 연결에서 `kindReply`로 회신합니다.
**순서가 중요합니다**:

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate`는 **생략할 수 없습니다**: 연결의 커널 조작면은 인증 전에는 신원이 없고, 바로 `session.open`하면 거부됩니다
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`). 파라미터는 당신의 프로토콜에서 파싱한 SASL 응답입니다:

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

소비 전달은 커널이 **정방향으로 푸시백**하며(`kindCall`, `method = "session.deliver"`), 처리를 마치면
`session.settle`로 정산합니다(`ack` / `requeue` / `reject`; 전달 번호는 전역 유일하며 스트림 번호가 필요 없음).

### 3.5 동시성 모델(Python 버전)

| 역할 | 스레드 |
| --- | --- |
| 프레임 읽기 루프 | 커널 연결마다 하나 |
| 스트림 처리 | 스트림마다 하나(따라서 여러 클라이언트 연결을 동시 처리 가능) |
| 정방향 호출 처리 | 호출마다 하나 |

**반드시 주의**: 읽기 루프에서 **역방향 호출 응답을 동기 대기하면 안 됩니다**(데드락 발생) —— 정방향 호출(`session.deliver`)은
처리 중 다시 `session.settle`을 일으킬 수 있으므로 독립 스레드로 넘겨야 합니다. 프레임 쓰기는 반드시 잠금으로 직렬화해야 합니다.

---

## 4. 코드 훑어보기(예제 프로젝트)

`swiftmq-plugin/python/sidecar_plugin.py`는 약 320줄이며, 주요 함수:

| 위치 | 역할 |
| --- | --- |
| `read_frame` / `Conn.send` | 프레임 읽기/쓰기(길이 프리픽스 + kind) |
| `Conn.call` | 역방향 호출: 번호 → 전송 → 응답 대기(`reverse=true`와 `id`로 매칭) |
| `Conn.serve` | 프레임 읽기 루프와 디스패치 |
| `Conn._handle_hello` | 플러그인 이름/API 버전 검증 후 HelloAck 회신 |
| `Conn._handle_open` / `_serve_stream` / `_echo` | 스트림 수명 주기와 에코 |
| `Conn._dispatch` | 정방향 호출 처리: `session.deliver`(정산 포함), `stats` |
| `Conn._session_demo` | 인증 + 큐 선언 + 발행 + 소비 |

---

## 5. 실측(로컬 재현)

환경: Windows + Python 3.12; 커널은 Docker(`swiftmq:1.1.01`)에서 실행, 플러그인은 호스트에서 실행,
커널이 `tcp://host.docker.internal:19001`로 연결.

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

커버한 경로: **핸드셰이크 → 인증 → 시맨틱 브리지(선언/발행/소비) → 전달 푸시백 → 정산 → 바이트 스트림 에코**.

---

## 6. Python 특유의 주의점

- **하트비트를 `time.sleep`으로 기다리지 마세요**: 읽기 루프가 블로킹이므로 커널의 Ping으로 연결을 유지하면 됩니다;
  socket에 읽기 타임아웃을 설정했다면 타임아웃을 "연결 종료"로 처리해야 합니다(커널 `kill -9` 시 socket이 즉시 닫히지 않을 수 있음).
- **`json.dumps`는 기본적으로 공백을 넣습니다**: 예제에서 `separators=(",", ":")`를 쓰는 것은 로그를 보기 좋게 하려는 것일 뿐이며 프로토콜 자체가 요구하지 않습니다.
- **바이트는 곧 base64**: `message.body`, `core.authenticate.response`는 JSON에서 모두 base64 문자열이므로
  `base64.b64encode/decode`를 잊지 마세요.
- **프레임 쓰기는 잠금 필요**: 하트비트, 응답, 데이터 청크가 서로 다른 스레드에서 오므로 교차 기록하면 연결 전체가 오염됩니다(예제는 `threading.Lock` 사용).
- `asyncio`도 가능하지만 "쓰기 직렬화 + 읽기 루프 비블로킹"을 보장해야 하며, 사고방식은 스레드 버전과 같습니다.

---

## 7. 심화

- 플러그인에 자체 관리 UI를 붙이려면: 설정에 `console_url`을 추가하면 관리 콘솔의 "플러그인 관리" 페이지에 바로가기 진입점이 나타납니다
  (메인 문서 §5.8 참조).
- 플러그인을 독립 서비스로 만들어 systemd / K8s로 관리: `spawn: []` + `restart: "never"`, 외부에서 기동.
- 다중 프로토콜 공존이 필요하면: 같은 리스너에서 여러 플러그인이 다른 `prefix`를 쓰게 하거나, 각각 전용 포트를 엽니다.
