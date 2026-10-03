# SwiftMQ 외부 프로세스 플러그인 개발 가이드 —— Node.js

> **대상**: Node.js로 SwiftMQ 외부 프로세스 플러그인(sidecar)을 작성하는 개발자.
> **먼저 읽기**: [외부 프로세스 플러그인(sidecar) 개발 가이드](plugin-development.md)(멘탈 모델 / 설정 필드 / 와이어 프로토콜 총표).
> **예제 프로젝트**: 작업 공간 `swiftmq-plugin/nodejs/index.js`(Node 표준 라이브러리만, **npm 의존성 없음**).

---

## 1. 실행하면 어떤 모습인가

```
内核进程 ──dial(tcp 本机地址)──► 你的 Node 进程（net.createServer 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

세 가지 요점: **당신의 프로세스는 서버**(커널이 연결해 오기를 기다림); **대외 포트는 커널이 엽니다**(설정 `protocols[].listeners`);
**`prefix`는 반드시 비어 있지 않아야 함**(빈 프리픽스 = 스니핑 미참여, 연결이 당신에게 넘겨지지 않으며, 실측상 즉시 끊김, ≤8바이트 ASCII).

---

## 2. 세 단계로 실행

### 1단계: 설정

`swiftmqd.json`(**실제 설정은 표준 JSON이며 주석을 넣을 수 없습니다**):

```json
{
  "plugins": {
    "node-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19011",
        "spawn": ["node", "/opt/swiftmq/index.js", "--addr", "0.0.0.0:19011", "--name", "node-sidecar"],
        "protocols": [
          { "name": "nodeecho", "prefix": "ND",
            "listeners": [{ "name": "nodeecho", "addr": ":19012" }] }
        ]
      }
    }
  }
}
```

### 2단계: 기동

```bash
node index.js --addr 0.0.0.0:19011 --name node-sidecar --session-demo
```

### 3단계: 검증

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'NDhello\n' | nc 127.0.0.1 19012                    # 应回显 NDhello
```

---

## 3. 구현 요점

### 3.1 프레이밍

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（包含 kind 字节）；单帧上限 16 MiB。
```

Node에서는 `Buffer`를 사용합니다: 수신한 바이트를 누적하고, 한 프레임이 다 차면 잘라내 처리합니다.

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

스트림 하나 = 클라이언트 연결 하나; 데이터 프레임 페이로드는 `4바이트 빅엔디언 스트림 번호 + 원시 바이트`입니다.

### 3.2 핸드셰이크와 하트비트

커널이 **먼저 Hello를 보내면** 당신은 `HelloAck`를 회신합니다; 커널이 `name`과 `api_version`(현재 `v1`)을 검증한 뒤 접속합니다.
이후 2초마다 `Ping`이 오면 `Pong`만 회신하면 됩니다(읽기 루프가 겸사 처리하므로 타이머가 필요 없음).

### 3.3 비동기 모델(Node 버전)

단일 스레드 이벤트 루프라 "쓰기 교차" 문제는 자연히 피할 수 있습니다 —— 다만 **읽기 루프에서 역방향 호출을 await하지 않도록** 주의하세요:

```js
case KIND_CALL:                 // 内核 → 插件（如 session.deliver）
  this.handleForwardCall(payload)   // 异步处理，不阻塞读循环
  break
```

`handleForwardCall`은 `async`입니다: 다시 `await call('session.settle', …)`할 수 있으므로 절대 동기 대기로 작성하면 안 됩니다.

### 3.4 시맨틱 브리지(반드시 먼저 인증)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate`는 생략할 수 없습니다: 연결의 커널 조작면은 인증 전에는 신원이 없고, 바로 `session.open`하면 거부됩니다
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

전달은 커널이 **정방향으로 푸시백**하며(`method = "session.deliver"`), 처리 후 `session.settle`합니다
(`ack` / `requeue` / `reject`; 전달 번호는 전역 유일하며 스트림 번호를 동반하지 않음).

---

## 4. 코드 훑어보기(예제 프로젝트)

`swiftmq-plugin/nodejs/index.js`는 약 330줄:

| 위치 | 역할 |
| --- | --- |
| `u32()` / `Conn.send()` | 프레임 읽기/쓰기 |
| `Conn.drain()` / `dispatch()` | 프레임 단위 파싱과 디스패치 |
| `Conn.call()` | 역방향 호출(`Promise` + `pending` 테이블, `reverse=true`와 `id`로 매칭) |
| `Stream` | 스트림 읽기 측: `push/end/read`가 비동기 큐를 구성 |
| `handleHello` | 검증 후 HelloAck 회신 |
| `handleForwardCall` / `handleMethod` | 정방향 호출(`session.deliver` + 정산, `stats`) |
| `sessionDemo` | 인증 + 선언 + 발행 + 소비 |

---

## 5. 실측(로컬 재현)

Windows + Node v24; 커널은 Docker(`swiftmq:1.1.01`), 플러그인은 호스트(`tcp://host.docker.internal:19011`).

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

커버: **핸드셰이크 → 인증 → 시맨틱 브리지 → 전달 푸시백 → 정산 → 바이트 스트림 에코**.

---

## 6. Node.js 특유의 주의점

- **`socket.write`는 한 번에 한 프레임**: 예제는 정수 프레임을 하나의 `Buffer`로 합쳐 쓰므로 추가 잠금이 필요 없습니다;
  한 프레임을 여러 번 `write`로 쪼개면 순서를 직접 보장해야 합니다.
- **`stream.on('data')`의 chunk 경계는 프레임과 무관**: 버퍼를 직접 누적해야 합니다(`drain()` 참조).
- **base64**: `message.body`, `core.authenticate.response`는 JSON에서 base64 문자열입니다
  (`Buffer.from(x).toString('base64')` / `Buffer.from(x, 'base64')`).
- **`drain()` 안에서 `await`하지 마세요**: 이는 동기 프레임 분할 함수입니다; 비동기 처리는 `handleForwardCall`에 넘기세요.
- **ESM vs CJS**: 예제는 `node index.js`로 바로 실행되도록 CommonJS(`require`)를 사용합니다; ESM으로 바꾸려면 `import`만 바꾸면 됩니다.

---

## 7. 심화

- 플러그인 자체 관리 UI: 설정에 `console_url` 추가(메인 문서 §5.8), 관리 콘솔 "플러그인 관리" 페이지에 바로가기 진입점이 나타납니다.
- 독립 배포(K8s / systemd): `spawn: []` + `address: "tcp://<서비스 이름>:19011"`, 컨테이너 안에서 `0.0.0.0` 리슨.
- 포트 재사용: 여러 프로토콜에 각기 다른 `prefix`를 주면 커널이 프리픽스로 연결을 각 플러그인에 디스패치합니다.
