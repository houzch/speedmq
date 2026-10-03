# SwiftMQ 외부 프로세스 플러그인 개발 가이드 —— Java

> **대상**: Java로 SwiftMQ 외부 프로세스 플러그인(sidecar)을 작성하는 개발자.
> **먼저 읽기**: [외부 프로세스 플러그인(sidecar) 개발 가이드](plugin-development.md)(멘탈 모델 / 설정 필드 / 와이어 프로토콜 총표).
> **예제 프로젝트**: 작업 공간 `swiftmq-plugin/java/SidecarPlugin.java`(단일 파일, JDK 표준 라이브러리만, Maven/Gradle 불필요).

---

## 1. 실행하면 어떤 모습인가

```
内核进程 ──dial(tcp 本机地址)──► 你的 Java 进程（ServerSocket 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

세 가지 요점: **당신의 프로세스는 서버**(커널이 연결해 오기를 기다림); **대외 포트는 커널이 엽니다**(`protocols[].listeners`);
**`prefix`는 반드시 비어 있지 않아야 함**(빈 프리픽스 = 스니핑 미참여, 연결이 당신에게 넘겨지지 않으며, 실측상 즉시 끊김, ≤8바이트 ASCII).

---

## 2. 세 단계로 실행

### 1단계: 설정

`swiftmqd.json`(**실제 설정은 표준 JSON이며 주석을 넣을 수 없습니다**):

```json
{
  "plugins": {
    "java-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19031",
        "spawn": ["java", "-cp", "/opt/swiftmq/classes", "SidecarPlugin",
                  "--addr", "0.0.0.0:19031", "--name", "java-sidecar"],
        "protocols": [
          { "name": "javaecho", "prefix": "JV",
            "listeners": [{ "name": "javaecho", "addr": ":19032" }] }
        ]
      }
    }
  }
}
```

### 2단계: 컴파일 및 기동

```bash
# JDK 11+ 可直接跑单文件源码（生产上建议先编译成 jar）
javac -encoding UTF-8 -d classes SidecarPlugin.java
java -Dfile.encoding=UTF-8 -cp classes SidecarPlugin --addr 0.0.0.0:19031 --name java-sidecar --session-demo
```

### 3단계: 검증

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'JVhello\n' | nc 127.0.0.1 19032                    # 应回显 JVhello
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

Java는 `DataInputStream`/`DataOutputStream`이 가장 간편합니다 —— 이들의 `readInt`/`writeInt`가 바로 **빅엔디언**입니다:

```java
len = in.readInt();              // 大端 u32
int kind = in.readUnsignedByte();
byte[] payload = new byte[len - 1];
in.readFully(payload);           // 读满，否则抛 EOFException
```

쓰기 측은 반드시 **직렬화**해야 합니다(하트비트, 응답, 데이터 청크가 서로 다른 스레드에서 옴):

```java
void send(int kind, byte[] payload) throws IOException {
    synchronized (writeLock) {
        out.writeInt(1 + payload.length);
        out.write(kind);
        out.write(payload);
        out.flush();
    }
}
```

데이터 프레임 페이로드 = `4바이트 빅엔디언 스트림 번호 + 원시 바이트`.

### 3.2 핸드셰이크와 하트비트

커널이 **먼저 Hello를 보내면** 당신은 `HelloAck`를 회신합니다; 커널이 `name`과 `api_version`(현재 `v1`)을 검증한 뒤 접속합니다.
이후 2초마다 `Ping`이 오면 `Pong`을 회신합니다.

### 3.3 동시성 모델(Java 버전)

| 역할 | 스레드 |
| --- | --- |
| 프레임 읽기 루프 | 커널 연결마다 하나 |
| 스트림 처리 | 스트림마다 하나(여러 클라이언트 연결 동시 처리 가능) |
| 정방향 호출 처리 | 호출마다 하나 |

**읽기 루프에서 역방향 호출 응답을 동기 대기하면 안 됩니다**(데드락): `session.deliver` 처리는 독립 스레드로 넘겨야 합니다.
그 내부에서 다시 `session.settle`(또 다른 역방향 호출)을 하기 때문입니다. 예제가 그렇게 합니다.

### 3.4 시맨틱 브리지(반드시 먼저 인증)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate`는 생략할 수 없습니다: 연결의 커널 조작면은 인증 전에는 신원이 없고, 바로 `session.open`하면 거부됩니다
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`).

```java
Map<String, Object> auth = new LinkedHashMap<>();
auth.put("stream", streamId);
auth.put("mechanism", "PLAIN");
// SASL PLAIN 响应：NUL + user + NUL + password，字节在 JSON 里走 base64
auth.put("response", Base64.getEncoder().encodeToString(plainResponse(user, password)));
Object ident = call("core.authenticate", auth, 10_000);

call("session.open", Map.of("stream", streamId, "vhost", "/"), 10_000);
Map<String, Object> q = asMap(call("session.declare_queue",
        Map.of("stream", streamId, "exclusive", true, "auto_delete", true), 10_000));
call("session.consume", Map.of("stream", streamId, "queue", q.get("name"), "prefetch", 32), 10_000);
```

전달은 커널이 **정방향으로 푸시백**하며(`method = "session.deliver"`), 처리 후 `session.settle`합니다
(`ack` / `requeue` / `reject`; 전달 번호는 전역 유일하며 스트림 번호를 동반하지 않음).

---

## 4. 코드 훑어보기(예제 프로젝트)

`swiftmq-plugin/java/SidecarPlugin.java`는 약 470줄(초경량 JSON 포함):

| 위치 | 역할 |
| --- | --- |
| `Conn.readFrame()` / `Conn.send()` | 프레임 읽기/쓰기(`DataInputStream` + 쓰기 잠금) |
| `Conn.serve()` | 프레임 읽기 루프와 디스패치 |
| `Conn.call()` | 역방향 호출(`pending` 테이블 + 블로킹 큐, 타임아웃 보호) |
| `Conn.handleHello()` | 검증 후 HelloAck 회신 |
| `StreamState` | 스트림 읽기 측(`BlockingQueue`, `STREAM_END`가 종료 표시) |
| `Conn.handleForwardCall()` / `handleMethod()` | 정방향 호출(`session.deliver` + 정산, `stats`) |
| `Conn.sessionDemo()` | 인증 + 선언 + 발행 + 소비 |
| `Json`(파일 끝) | 초경량 JSON 읽기/쓰기, 예제를 무의존으로 만들기 위한 것 |

> **프로덕션 권장**: `Json`을 익숙한 라이브러리(Jackson / Gson)로 바꾸거나, `java.net.http` 외의 기존 스택을 사용하세요 ——
> 이 예제가 설명하려는 것(와이어 프로토콜)과는 무관합니다.

---

## 5. 실측(로컬 재현)

Windows + JDK 25; 커널은 Docker(`swiftmq:1.1.01`), 플러그인은 호스트(`tcp://host.docker.internal:19031`).

```
javac -encoding UTF-8 -d classes SidecarPlugin.java   → 退出码 0
plugin=java-sidecar state=enabled          # /api/plugins
echo=[JVhello]                             # 客户端连内核端口 19032 发 "JVhello\n"

--- 插件 stdout ---
sidecar 已启动 plugin=java-sidecar addr=0.0.0.0:19031 version=0.1.0
内核已接入 plugin=java-sidecar peer=/127.0.0.1:51039
握手完成 plugin=java-sidecar kernel=1.1.01
认证通过 user=guest
session 演示完成 queue=amq.gen-4ce088d93638eafe830191
流已打开 plugin=java-sidecar stream=1 remote=172.17.0.1:34112 local=172.17.0.2:19032
收到投递（session.deliver） queue=amq.gen-4ce088d93638eafe830191 delivery_id=1 body=hello from java sidecar
```

커버: **핸드셰이크 → 인증 → 시맨틱 브리지 → 전달 푸시백 → 정산 → 바이트 스트림 에코**.

---

## 6. Java 특유의 주의점

- **소스의 `\uXXXX`는 컴파일러가 어디서든 처리합니다**(주석 포함!). 예제 주석에서는 의도적으로
  "NUL + 사용자 이름 + NUL + 비밀번호"로 쓰고 `\u0000`을 직접 쓰지 않았습니다. 그렇지 않으면 javac가 잘못된 문자로 보고합니다.
- **중국어 소스는 반드시 `javac -encoding UTF-8`**: 그렇지 않으면 Windows 기본(GBK)에서 "GBK 인코딩의 매핑 불가 문자" 오류가 납니다.
  런타임에 중국어를 올바르게 출력하려면 `-Dfile.encoding=UTF-8`을 추가하세요.
- **lambda가 캡처하는 지역 변수는 effectively final이어야 함**: 예제에서 `name`은 인자 파싱 중 재할당되므로
  lambda 안에서는 `opts.name`(한 번만 할당되는 필드)을 사용합니다.
- **`DataInputStream`은 블로킹**: 연결이 끊기면 `EOFException`/`IOException`을 던지며, 이를 근거로 마무리합니다.
- **base64**: `message.body`, `core.authenticate.response`는 JSON에서 base64 문자열입니다
  (`Base64.getEncoder()/getDecoder()`).
- **JDK 표준 라이브러리에는 JSON이 없음**: 예제는 초경량 구현을 포함합니다; `Json.parse`는 정수를 `Long`, 부동소수를 `Double`로 디코딩하며,
  `id`를 읽을 때는 `((Number) m.get("id")).longValue()`를 사용합니다.

---

## 7. 심화

- 실행 가능 jar(`Main-Class: SidecarPlugin`) 또는 `jlink`로 경량 런타임으로 패키징한 뒤,
  `spawn`을 `["java", "-jar", "/opt/swiftmq/sidecar.jar", …]`로 바꿉니다.
- 플러그인 자체 관리 UI: 설정에 `console_url` 추가(메인 문서 §5.8), 관리 콘솔 "플러그인 관리" 페이지에 바로가기 진입점이 나타납니다.
- 독립 배포: `spawn: []` + `address: "tcp://<서비스 이름>:19031"`, 컨테이너 안에서 `0.0.0.0` 리슨.
