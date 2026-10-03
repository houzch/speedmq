# SwiftMQ 外部プロセスプラグイン開発ガイド —— Java

> **対象読者**：Java で SwiftMQ の外部プロセスプラグイン（sidecar）を書く開発者。
> **先に読む**：[外部プロセスプラグイン（sidecar）開発ガイド](plugin-development.md)（メンタルモデル / 設定フィールド / ワイヤプロトコル総表）。
> **サンプルプロジェクト**：ワークスペース `swiftmq-plugin/java/SidecarPlugin.java`（単一ファイル、JDK 標準ライブラリのみ、Maven/Gradle 不要）。

---

## 1. 動かすとどうなるか

```
内核进程 ──dial(tcp 本机地址)──► 你的 Java 进程（ServerSocket 监听本机地址）
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

`swiftmqd.json`（**実際の設定は標準 JSON、コメント不可**）：

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

### 第 2 ステップ：コンパイルして起動

```bash
# JDK 11+ 可直接跑单文件源码（生产上建议先编译成 jar）
javac -encoding UTF-8 -d classes SidecarPlugin.java
java -Dfile.encoding=UTF-8 -cp classes SidecarPlugin --addr 0.0.0.0:19031 --name java-sidecar --session-demo
```

### 第 3 ステップ：検証

```bash
curl -u guest:guest http://127.0.0.1:15672/api/plugins     # state 应为 enabled
printf 'JVhello\n' | nc 127.0.0.1 19032                    # 应回显 JVhello
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

Java では `DataInputStream`/`DataOutputStream` が最も簡単—— それらの `readInt`/`writeInt` はまさに**ビッグエンディアン**：

```java
len = in.readInt();              // 大端 u32
int kind = in.readUnsignedByte();
byte[] payload = new byte[len - 1];
in.readFully(payload);           // 读满，否则抛 EOFException
```

書き込み側は**直列**でなければならない（ハートビート、応答、データブロックは異なるスレッドから来る）：

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

データフレームのペイロード = `4 バイトビッグエンディアンのストリーム番号 + 生のバイト`。

### 3.2 ハンドシェイクとハートビート

カーネルが**先に Hello を送り**、あなたは `HelloAck` を返す；カーネルは `name` と `api_version`（現在 `v1`）を検証してから接続する。
その後 2 秒ごとに 1 つの `Ping`、`Pong` を返す。

### 3.3 並行モデル（Java 版）

| 役割 | スレッド |
| --- | --- |
| フレーム読み取りループ | カーネル接続ごとに 1 つ |
| ストリーム処理 | ストリームごとに 1 つ（複数のクライアント接続を並行処理可能） |
| 順方向呼び出し処理 | 呼び出しごとに 1 つ |

**読み取りループ内で逆方向呼び出しの応答を同期的に待ってはいけない**（デッドロックする）：`session.deliver` の処理は独立したスレッドに投げる必要があり、
その内部でさらに `session.settle`（また逆方向呼び出し）するからである。サンプルはそうしている。

### 3.4 セマンティクスブリッジ（先に認証が必要）

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` は省けません：接続のカーネル操作面は認証前に身元を持たず、いきなり `session.open` すると拒否されます
（`ACCESS_REFUSED - access to vhost '/' refused for user ''`）。

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

配信はカーネルが**順方向に回推**し（`method = "session.deliver"`）、処理後に `session.settle`
（`ack` / `requeue` / `reject`；配信番号はグローバルに一意で、ストリーム番号は伴わない）。

---

## 4. コードウォークスルー（サンプルプロジェクト）

`swiftmq-plugin/java/SidecarPlugin.java` 約 470 行（超簡易 JSON 含む）：

| 位置 | 役割 |
| --- | --- |
| `Conn.readFrame()` / `Conn.send()` | フレーミングの読み書き（`DataInputStream` + 書き込みロック） |
| `Conn.serve()` | フレーム読み取りループとディスパッチ |
| `Conn.call()` | 逆方向呼び出し（`pending` テーブル + ブロッキングキュー、タイムアウト保護） |
| `Conn.handleHello()` | 検証して HelloAck を返す |
| `StreamState` | ストリーム読み取り側（`BlockingQueue`、`STREAM_END` は終了を表す） |
| `Conn.handleForwardCall()` / `handleMethod()` | 順方向呼び出し（`session.deliver` + settle、`stats`） |
| `Conn.sessionDemo()` | 認証 + 宣言 + パブリッシュ + コンシューム |
| `Json`（ファイル末尾） | 超簡易 JSON の読み書き、サンプルをゼロ依存にするためだけ |

> **本番の推奨**：`Json` を慣れたライブラリ（Jackson / Gson）に置き換える、または `java.net.http` 以外の既存スタックを使う——
> 本サンプルが扱うもの（ワイヤプロトコル）とは無関係です。

---

## 5. 実測（本機で再現）

Windows + JDK 25；カーネルは Docker（`swiftmq:1.1.01`）、プラグインはホストマシン（`tcp://host.docker.internal:19031`）。

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

カバー：**ハンドシェイク → 認証 → セマンティクスブリッジ → 配信の回推 → 確定 → バイトストリームのエコー**。

---

## 6. Java 特有の注意点

- **ソース内の `\uXXXX` はコンパイラによりどの位置でも処理される**（コメントも含む！）。サンプルのコメントでは意図的に
  「NUL + ユーザー名 + NUL + パスワード」と書き、直接 `\u0000` を書いていない。そうでないと javac が不正文字を報告する。
- **中国語のソースには `javac -encoding UTF-8` が必須**、そうでないと Windows の既定（GBK）で「エンコーディング GBK のマップ不可文字」エラーになる。
  実行時に中国語を正しく出力したい場合は `-Dfile.encoding=UTF-8` を付ける。
- **lambda が捕捉するローカル変数は effectively final でなければならない**：サンプルでは `name` が引数解析で再代入されるため、
  lambda 内では `opts.name`（1 回だけ代入されるフィールド）を使う。
- **`DataInputStream` はブロッキング**：接続が切れると `EOFException`/`IOException` を投げるので、それで後始末する。
- **base64**：`message.body`、`core.authenticate.response` は JSON 内では base64 文字列
  （`Base64.getEncoder()/getDecoder()`）。
- **JDK 標準ライブラリに JSON はない**：サンプルは超簡易実装を同梱；`Json.parse` は整数を `Long`、浮動小数点を `Double` に解釈し、
  `id` を取るときは `((Number) m.get("id")).longValue()` を使う。

---

## 7. 応用

- 実行可能 jar（`Main-Class: SidecarPlugin`）や `jlink` でランタイムを削減してパッケージし、
  `spawn` を `["java", "-jar", "/opt/swiftmq/sidecar.jar", …]` に変える。
- プラグイン独自の管理画面：設定に `console_url`（メインドキュメント §5.8）を追加すると、管理 UI の「プラグイン管理」ページに直通入口が表示される。
- 独立デプロイ：`spawn: []` + `address: "tcp://<サービス名>:19031"`、コンテナ内で `0.0.0.0` をリッスン。
