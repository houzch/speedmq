# SpeedMQ 外部プロセスプラグイン開発ガイド —— PHP

> **対象読者**：PHP で SpeedMQ の外部プロセスプラグイン（sidecar）を書く開発者。
> **先に読む**：[外部プロセスプラグイン（sidecar）開発ガイド](plugin-development.md)（メンタルモデル / 設定フィールド / ワイヤプロトコル総表）。
> **サンプルプロジェクト**：ワークスペース `speedmq-plugin/php/sidecar_plugin.php`（標準ライブラリのみ、**composer 依存不要**）。

---

## 1. 動かすとどうなるか

```
内核进程 ──dial(tcp 本机地址)──► 你的 PHP 进程（stream_socket_server 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

3 つの要点：**あなたのプロセスがサーバー**（カーネルの接続を待つ）；**対外ポートはカーネルが開く**（`protocols[].listeners`）；
**`prefix` は非空でなければならない**（空プレフィックス = スニッフに参加せず、接続はあなたに渡されない；実測では即座に切断、≤8 バイト ASCII）。

---

## 2. 3 ステップで動かす

### 第 1 ステップ：設定

`speedmqd.json`（**実際の設定は標準 JSON、コメント不可**）：

```json
{
  "plugins": {
    "php-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19021",
        "spawn": ["php", "/opt/speedmq/sidecar_plugin.php", "--addr", "0.0.0.0:19021", "--name", "php-sidecar"],
        "protocols": [
          { "name": "phpecho", "prefix": "PH",
            "listeners": [{ "name": "phpecho", "addr": ":19022" }] }
        ]
      }
    }
  }
}
```

### 第 2 ステップ：起動

```bash
php sidecar_plugin.php --addr 0.0.0.0:19021 --name php-sidecar --session-demo
```

### 第 3 ステップ：検証

```bash
php -l sidecar_plugin.php                                  # 先过语法检查
curl -u guest:guest http://127.0.0.1:15672/api/plugins      # state 应为 enabled
printf 'PHhello\n' | nc 127.0.0.1 19022                     # 应回显 PHhello
```

---

## 3. 実装の要点

### 3.1 フレーミング

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（包含 kind 字节）；单帧上限 16 MiB。
```

PHP では `pack`/`unpack` を使う：

```php
// 写：pack('N', …) 就是大端 u32
fwrite($sock, pack('N', 1 + strlen($payload)) . chr($kind) . $payload);

// 读：先读 5 字节头，再读 payload
$head = read_exact($sock, 5);
$len  = unpack('N', substr($head, 0, 4))[1];
$kind = ord($head[4]);
$payload = read_exact($sock, $len - 1);
```

データフレームのペイロード = `pack('N', $streamId) . 生のバイト`。

### 3.2 ハンドシェイクとハートビート

カーネルが**先に Hello を送り**、あなたは `HelloAck` を返す；カーネルは `name` と `api_version`（現在 `v1`）を検証する。
その後 2 秒ごとに 1 つの `Ping`、`Pong` を返す。

### 3.3 並行モデル：再入可能なフレームポンプ（PHP にはスレッドがない）

PHP CLI はシングルスレッドのブロッキング式なので、ここでは「ストリームごとに 1 スレッド」ではなく：

- **読み取りループ**（`serve()`）がハンドシェイク、ハートビート、ストリーム開始、データのエコー、順方向呼び出しの処理を担当；
- **エコー**には追加の状態機械は不要：`kindData` を受け取ったら即座にそのまま `kindData` に書き戻す；
- **逆方向呼び出し**は `callAndWait()` を使う：`kindCall` を送信した後、フレームを読みながらディスパッチし、
  **自分自身の**応答（`reverse=true` かつ `id` 一致）を読むまで戻らない。

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

これは `dispatchOther()` が**再入可能でなければならない**ことを意味する：1 回の `callAndWait` の内部で再度呼ばれる可能性がある
（例えば `session.deliver` の処理中にさらに `session.settle` する）。サンプルはまさにそうしている。

### 3.4 セマンティクスブリッジ（先に認証が必要）

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` は省けません：接続のカーネル操作面は認証前に身元を持たず、いきなり `session.open` すると拒否されます
（`ACCESS_REFUSED - access to vhost '/' refused for user ''`）。

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

配信はカーネルが**順方向に回推**し（`method = "session.deliver"`）、処理後に `session.settle`
（`ack` / `requeue` / `reject`；配信番号はグローバルに一意で、ストリーム番号は伴わない）。

---

## 4. コードウォークスルー（サンプルプロジェクト）

`speedmq-plugin/php/sidecar_plugin.php` 約 320 行：

| 位置 | 役割 |
| --- | --- |
| `read_exact()` / `Conn::readFrame()` / `sendFrame()` | フレーミングの読み書き |
| `Conn::serve()` | メイン読み取りループ |
| `Conn::dispatchOther()` | ハンドシェイク以外のフレームをディスパッチ（再入可能） |
| `Conn::callAndWait()` | 逆方向呼び出し（再入可能なフレームポンプ） |
| `Conn::handleHello()` | 検証して HelloAck を返す |
| `Conn::handleForwardCall()` / `handleMethod()` | 順方向呼び出し（`session.deliver` + settle、`stats`） |
| `Conn::sessionDemo()` | 認証 + 宣言 + パブリッシュ + コンシューム |

---

## 5. 実測（本機で再現）

Windows + PHP 7.4；カーネルは Docker（`speedmq:1.1.01`）、プラグインはホストマシン（`tcp://host.docker.internal:19021`）。

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

カバー：**ハンドシェイク → 認証 → セマンティクスブリッジ → 配信の回推 → 確定 → バイトストリームのエコー**。

---

## 6. PHP 特有の注意点

- **PHP 7.4 には `mixed` 戻り型がない**（PHP 8.0 以降）：サンプルでは逆方向呼び出しが「任意の型」を返すため、
  **戻り型宣言を書かない**（`@return mixed` コメントを使う）。7.4 で `: mixed` と書くとそのまま構文エラーになる。
- **JSON の数値型**：`json_decode($s, true)` は既定で整数を `int` に解釈するが、大きな整数は `float` になる可能性がある；
  配信番号は本サンプルの規模では問題ないが、番号が非常に大きい場合は `JSON_BIGINT_AS_STRING` を検討する。
- **base64 は必須**：`message.body` と `core.authenticate.response` は JSON 内では base64 文字列
  （`base64_encode` / `base64_decode($s, true)`）。
- **`pcntl_fork` で並行処理をしない**：Windows には pcntl がなく、fork すると「単一接続単一ライター」の仮定が壊れる；
  シングルスレッド + 再入可能なフレームポンプで十分（ストリーム上で非常に重い計算をする場合は、むしろ外部サービスに置く方が適している）。
- **`stream_socket_accept` はブロッキング**：プロセスのライフサイクルはカーネル（`spawn`）または supervisor が管理する；
  `fread` が `''`（EOF）を返す場合の処理を忘れない → その接続を終了して accept に戻る。
- **出力バッファ**：ログは `fwrite(STDOUT, …)` に改行を付けて書き、カーネルが行単位でカーネルログに転送しやすくする。

---

## 7. 応用

- プラグイン独自の管理画面：設定に `console_url`（メインドキュメント §5.8）を追加すると、管理 UI の「プラグイン管理」ページに直通入口が表示される。
- 独立デプロイ：`spawn: []` + `address: "tcp://<サービス名>:19021"`、コンテナ内で `0.0.0.0` をリッスン。
- より高い並行性が必要な場合、プラグインを常駐の Swoole / RoadRunner などに変えてもよいが、**ワイヤプロトコルは不変**で、保証すべきは：
  フレーム書き込みの直列化、読み取りループをブロックしない、逆方向呼び出しを `id`+`reverse` でマッチング。
