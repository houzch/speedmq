# SwiftMQ 외부 프로세스 플러그인 개발 가이드 —— PHP

> **대상**: PHP로 SwiftMQ 외부 프로세스 플러그인(sidecar)을 작성하는 개발자.
> **먼저 읽기**: [외부 프로세스 플러그인(sidecar) 개발 가이드](plugin-development.md)(멘탈 모델 / 설정 필드 / 와이어 프로토콜 총표).
> **예제 프로젝트**: 작업 공간 `swiftmq-plugin/php/sidecar_plugin.php`(표준 라이브러리만, **composer 의존성 없음**).

---

## 1. 실행하면 어떤 모습인가

```
内核进程 ──dial(tcp 本机地址)──► 你的 PHP 进程（stream_socket_server 监听本机地址）
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

### 2단계: 기동

```bash
php sidecar_plugin.php --addr 0.0.0.0:19021 --name php-sidecar --session-demo
```

### 3단계: 검증

```bash
php -l sidecar_plugin.php                                  # 先过语法检查
curl -u guest:guest http://127.0.0.1:15672/api/plugins      # state 应为 enabled
printf 'PHhello\n' | nc 127.0.0.1 19022                     # 应回显 PHhello
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

PHP는 `pack`/`unpack` 사용:

```php
// 写：pack('N', …) 就是大端 u32
fwrite($sock, pack('N', 1 + strlen($payload)) . chr($kind) . $payload);

// 读：先读 5 字节头，再读 payload
$head = read_exact($sock, 5);
$len  = unpack('N', substr($head, 0, 4))[1];
$kind = ord($head[4]);
$payload = read_exact($sock, $len - 1);
```

데이터 프레임 페이로드 = `pack('N', $streamId) . 원시 바이트`.

### 3.2 핸드셰이크와 하트비트

커널이 **먼저 Hello를 보내면** 당신은 `HelloAck`를 회신합니다; 커널이 `name`과 `api_version`(현재 `v1`)을 검증합니다.
이후 2초마다 `Ping`이 오면 `Pong`을 회신합니다.

### 3.3 동시성 모델: 재진입 가능한 프레임 펌프(PHP에는 스레드가 없음)

PHP CLI는 단일 스레드 블로킹 방식이므로, 여기서는 "스트림마다 스레드 하나" 대신:

- **읽기 루프**(`serve()`)가 핸드셰이크, 하트비트, 스트림 열기, 데이터 에코, 정방향 호출 처리를 담당합니다;
- **에코**에는 추가 상태 머신이 필요 없습니다: `kindData`를 받으면 즉시 그대로 `kindData`로 기록합니다;
- **역방향 호출**은 `callAndWait()`를 사용합니다: `kindCall`을 보낸 뒤 프레임을 읽으면서 디스패치하고,
  **자신의** 응답(`reverse=true`이고 `id` 일치)을 읽을 때까지 기다렸다가 반환합니다.

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

이는 `dispatchOther()`가 **반드시 재진입 가능**해야 함을 의미합니다: 하나의 `callAndWait` 내부에서 다시 호출될 수 있습니다
(예: `session.deliver`를 처리하다가 다시 `session.settle`이 필요). 예제가 바로 그렇게 합니다.

### 3.4 시맨틱 브리지(반드시 먼저 인증)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate`는 생략할 수 없습니다: 연결의 커널 조작면은 인증 전에는 신원이 없고, 바로 `session.open`하면 거부됩니다
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

전달은 커널이 **정방향으로 푸시백**하며(`method = "session.deliver"`), 처리 후 `session.settle`합니다
(`ack` / `requeue` / `reject`; 전달 번호는 전역 유일하며 스트림 번호를 동반하지 않음).

---

## 4. 코드 훑어보기(예제 프로젝트)

`swiftmq-plugin/php/sidecar_plugin.php`는 약 320줄:

| 위치 | 역할 |
| --- | --- |
| `read_exact()` / `Conn::readFrame()` / `sendFrame()` | 프레임 읽기/쓰기 |
| `Conn::serve()` | 메인 읽기 루프 |
| `Conn::dispatchOther()` | 핸드셰이크 외 프레임 디스패치(재진입 가능) |
| `Conn::callAndWait()` | 역방향 호출(재진입 가능한 프레임 펌프) |
| `Conn::handleHello()` | 검증 후 HelloAck 회신 |
| `Conn::handleForwardCall()` / `handleMethod()` | 정방향 호출(`session.deliver` + 정산, `stats`) |
| `Conn::sessionDemo()` | 인증 + 선언 + 발행 + 소비 |

---

## 5. 실측(로컬 재현)

Windows + PHP 7.4; 커널은 Docker(`swiftmq:1.1.01`), 플러그인은 호스트(`tcp://host.docker.internal:19021`).

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

커버: **핸드셰이크 → 인증 → 시맨틱 브리지 → 전달 푸시백 → 정산 → 바이트 스트림 에코**.

---

## 6. PHP 특유의 주의점

- **PHP 7.4에는 `mixed` 반환 타입이 없습니다**(PHP 8.0부터): 예제에서 역방향 호출은 "임의 타입"을 반환하므로
  **반환 타입 선언을 쓰지 않습니다**(`@return mixed` 주석 사용). 7.4에서 `: mixed`를 쓰면 바로 문법 오류입니다.
- **JSON 숫자 타입**: `json_decode($s, true)`는 기본적으로 정수를 `int`로 디코딩하며, 큰 정수는 `float`이 될 수 있습니다;
  전달 번호는 이 예제 규모에서는 문제없지만, 번호가 매우 크면 `JSON_BIGINT_AS_STRING`을 고려하세요.
- **base64는 필수**: `message.body`와 `core.authenticate.response`는 JSON에서 base64 문자열입니다
  (`base64_encode` / `base64_decode($s, true)`).
- **`pcntl_fork`로 동시성을 만들지 마세요**: Windows에는 pcntl이 없고, fork하면 "연결당 단일 작성자" 가정이 깨집니다;
  단일 스레드 + 재진입 가능 프레임 펌프로 충분합니다(스트림에서 매우 무거운 계산을 해야 한다면 외부 서비스가 더 적합합니다).
- **`stream_socket_accept`는 블로킹**: 프로세스 수명 주기는 커널(`spawn`)이나 supervisor가 관리합니다;
  `fread`가 `''`(EOF)를 반환하면 해당 연결을 끝내고 accept로 돌아가도록 처리하세요.
- **출력 버퍼링**: 로그는 `fwrite(STDOUT, …)`에 개행을 붙여, 커널이 줄 단위로 커널 로그에 포워딩할 수 있게 하세요.

---

## 7. 심화

- 플러그인 자체 관리 UI: 설정에 `console_url` 추가(메인 문서 §5.8), 관리 콘솔 "플러그인 관리" 페이지에 바로가기 진입점이 나타납니다.
- 독립 배포: `spawn: []` + `address: "tcp://<서비스 이름>:19021"`, 컨테이너 안에서 `0.0.0.0` 리슨.
- 더 높은 동시성이 필요하면 플러그인을 상주형 Swoole / RoadRunner 등으로 바꿀 수 있지만 **와이어 프로토콜은 변하지 않습니다**. 다음만 보장하면 됩니다:
  프레임 쓰기 직렬화, 읽기 루프 비블로킹, 역방향 호출을 `id`+`reverse`로 매칭.
