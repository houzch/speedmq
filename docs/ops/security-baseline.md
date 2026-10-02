# SwiftMQ 安全加固基线（可勾选清单）

> 原则：**只写本仓库真实具备的能力**。每条给出"为什么要做 + 怎么验证做了"，验证命令均可跑。
> 标注 **【已验证】** 的表示在本机（Windows + PowerShell 5.1，`0.13.0`）**真的执行过**；
> **【未验证】** 表示未执行或当前做不到，绝不假装。
> 所有命令用 `/bin/sh` 风格给出 curl 版本，并附 PowerShell 版本（PowerShell 5.1 请用
> `Invoke-WebRequest ... -UseBasicParsing`）。

---

## A. 认证与访问控制

### A-1. 修改默认账号 `guest/guest` 【已验证】

- **为什么**：默认内置 `guest/guest`（标签 `administrator`），对外暴露即等于敞开大门。
- **怎么做**：运行期改口令 / 删账号，**不要**去改配置文件（`users` 只在首次引导生效）。

```bash
# 改口令
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/users/guest \
     -H 'Content-Type: application/json' -d '{"password":"<新口令>","tags":["administrator"]}'
# 或直接删掉默认账号（先确保已建好新的管理员）
curl -u guest:guest -X DELETE http://127.0.0.1:15672/api/users/guest
```

- **怎么验证**：改后旧口令必须 401、新口令 200。
  **【已验证】** 本机实测输出：

  ```
  --- 改密前 guest/guest ---          HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  PUT /api/users/guest -> 204
  --- 改密后 guest/guest（应 401）---  HTTP 401
  --- 改密后 guest/s3cret（应 200）--- HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  ```
- [ ] 默认账号已改/已删

### A-2. 最小权限：按 vhost 的 `configure` / `write` / `read` 正则 【已验证】

- **为什么**：三分类对齐 RabbitMQ —— `configure` 管拓扑声明/删除、`write` 管发布与绑定、`read` 管消费与拉取；
  越权返回 403。给业务账号只开它需要的。
- **怎么做**：`PUT /api/permissions/{vhost}/{user}`，例如只读消费：`{"configure":"^$","write":"^$","read":".*"}`。

```bash
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/permissions/%2F/appuser \
     -H 'Content-Type: application/json' -d '{"configure":"^$","write":"^$","read":".*"}'
```

- **怎么验证**：用受限账号尝试越权操作，应得 403 `ACCESS_REFUSED`。
  **【已验证】** 本机用真实 AMQP 客户端以 `configure="^$"` 的用户声明交换机，实测：

  ```
  FAIL 声明 durable 交换机失败: Exception (403) Reason: "ACCESS_REFUSED - access to configure 'r.ex' refused for user 'restricted'"
  ```
- [ ] 每个业务账号只授予必需的正则，且未给 `administrator`/`management` 标签

### A-3. `administrator` 标签的口径（隐式完全权限）—— 谨慎授予 【已验证】

- **为什么**：**`administrator` 标签的用户对其可见的所有 vhost 拥有完全权限、无需权限记录**
  （对齐 RabbitMQ 实测口径，见 README / 设计 M8-7）。也就是说，只要给了这个标签，
  权限正则就不起作用了——它是最高权限。
- **怎么做**：只有管理面/运维账号给 `administrator`；业务账号一律不给标签、只走权限正则。

- **怎么验证（演示隐式权限）**：对一个**没有任何权限记录**的新 vhost，`administrator` 用户应直接可用。
  **【已验证】** 本机实测：新建 vhost `drillvh`（未建任何权限记录）后，`guest`（administrator）在其上声明拓扑成功：

  ```
  --- administrator 隐式权限：guest 在 drillvh 上声明拓扑 ---
  OK  已声明 durable 交换机 a.ex / 队列 a.q，并绑定 key=k   (exit=0)
  ```
- [ ] `administrator` 标签仅授予极少数运维账号

### A-4. `remote_access`：限制账号仅本机登录 【未验证（同机无法模拟远端来源）】

- **为什么**：对齐 RabbitMQ，内置 `guest` 默认仅允许本机登录；对外部署时应确保特权账号的来源受限。
- **怎么做 / 口径（重要限制）**：
  - `remote_access` 只能写在**配置文件**的 `users.<name>.remote_access` 里，**仅首次引导生效**；
  - **通过管理 API / `swiftmqctl` 创建的账号一律 `remote_access=true`**（允许任意来源登录）——
    依据 `internal/broker/observe.go` 的 `UpsertUser` 注释与实测 `meta/state.json` 中
    `"remote_access":true`。也就是说 **API 目前无法把某个账号限制为仅本机**。
- **怎么验证**：从**另一台主机**（非 `127.0.0.1`）用该账号连接，应被 403；本机连接应成功。
  **【未验证】**：本机环境无法构造真实远端来源，未实测。
- [ ] 特权账号的来源限制已按上述口径评估（注意 API 建号默认放开远端）

---

## B. 传输安全（TLS）

TLS 配置项（接入层与管理面**共用**同一套字段）：`cert_file` / `key_file` / `ca_file` / `client_auth` / `min_version`。

### B-1. 开启 TLS 并配错即拒绝启动 【已验证】

- **为什么**：证书在**启动时**读取并校验——配错立刻拒绝启动，而不是等第一个客户端连上来才暴露。
- **怎么做**：在 `listeners.<plugin>[].tls` 或 `management.tls` 里提供 `cert_file` + `key_file`（**同给**才开启）。

- **怎么验证**：用错误配置启动，应立刻失败。
  **【已验证】** 本机三种错误配置实测，全部 `exit=1`、拒绝启动：

  ```
  badtls1: swiftmqd 启动失败: listeners.amqp091[0].tls 需要同时提供 cert_file 与 key_file
  badtls2: swiftmqd 启动失败: listeners.amqp091[0].tls.min_version 取值非法: "1.0"（可选 1.2 / 1.3）
  badtls3: swiftmqd 启动失败: listeners.amqp091[0].tls 无效: 加载服务端证书失败（cert=... key=...）: open ...: The system cannot find the path specified.
  ```
- **怎么验证（正/反向）**：TLS 客户端能连、明文客户端连 TLS 端口会被拒。
  **【已验证】** 本机起 TLS 实例（`amqp091` 走 TLS），用真实客户端探针：

  ```
  === 正向：TLS 客户端跑全量探针 ===      全部通过（25/25）: ...
  === 反向：明文客户端连 TLS 端口（应失败）===  FAIL ... 拨号失败: Exception (501) Reason: "EOF"   (exit=-1)
  ```
- [ ] 对外协议端口已启用 TLS

### B-2. `min_version` 至少 1.2 【已验证】

- **为什么**：禁用过旧 TLS 版本；默认即 `1.2`，可选 `1.2` / `1.3`。
- **怎么验证**：把 `min_version` 写成 `1.0`，启动应报错（见 B-1 的 `badtls2` 输出）。
- [ ] `min_version` 为 `1.2` 或 `1.3`

### B-3. 双向认证 `client_auth: require_and_verify`（mTLS） 【部分验证】

- **为什么**：要求客户端出示并验证证书，防止未授权客户端接入协议端口。
- **怎么做**：配 `ca_file` + `client_auth: require_and_verify`（后两者要求同时提供 `ca_file`）。
- **【已验证】**：TLS 端到端与被拒路径已用真实客户端验证（B-1）。**mTLS（要求并校验客户端证书）本机未单独演练**。
- [ ] 需要 mTLS 的端口已配 `require_and_verify` + `ca_file`

### B-4. 管理面 TLS 【未验证】

- **为什么**：管理面走 Basic Auth 传口令，必须加密。
- **怎么做**：`management.tls` 用与协议监听相同的字段。
- **【未验证】**：本机演练把管理面绑在本机明文端口，未单独起管理面 HTTPS。
- [ ] 管理面已启用 TLS（或严格限制在可信网络内）

---

## C. 暴露面收敛

### C-1. 管理面监听范围收敛 【已验证（监听地址实测）】

- **为什么**：管理面默认 `:15672`（所有网卡）。对外部署应绑到内网/回环地址，或用防火墙限制来源。
- **怎么做**：`management.addr` 配成 `127.0.0.1:15672` 或内网地址；或将 `management.enabled=false` 彻底关闭
  （关闭后无管理端口，但 `swiftmqctl` 也随之不可用）。
- **怎么验证**：
  **【已验证】** 本机把管理面配成 `127.0.0.1:15677`，实测监听地址确为回环：

  ```
  LocalAddress LocalPort
  ------------ ---------
  127.0.0.1        15677
  ```
- [ ] 管理面绑定地址已收敛（或已停用）

### C-2. 协议端口只开必需的 【未验证】

- **为什么**：默认同时开 AMQP `5672` 与 MQTT `1883`；不用 MQTT 就关掉，减少攻击面。
- **怎么做**：`plugins.mqtt.enabled=false`（或从 `listeners` 里去掉）；停用走的是**关闭真实端口**，不只是改状态位。
- **怎么验证**：停用后对应端口不再监听（`Get-NetTCPConnection -State Listen` 看不到）。
  **【未验证】**：本机演练两种协议都开着，未单独验证关闭后的端口消失。
- [ ] 未使用的协议插件已禁用

---

## D. 容器运行加固

仓库镜像事实（`Dockerfile`）：静态链接二进制 + alpine，**以非 root（uid 10001，用户 `swiftmq`）运行**，
数据目录 `/var/lib/swiftmq` 为卷。`docker-compose.yml` 使用**命名卷**持久化、配置**只读挂载**、日志轮转。

### D-1. 非 root 运行 【未验证（本机未跑 Docker）】

- **为什么**：最小权限，降低容器逃逸后的影响面。
- **怎么做**：镜像默认已是 uid 10001；**不要**用 `--user root` 覆盖。
- **怎么验证**：`docker compose run -T --rm broker id` 应显示 `uid=10001`。（`run` 非交互环境必须加 `-T`）
- [ ] 容器以非 root 运行（未以 root 覆盖）

### D-2. 只读根文件系统 + 资源限额 + 能力裁剪（建议，仓库 compose 未默认开启） 【未验证】

- **为什么**：只读根文件系统可阻止运行期篡改二进制；资源限额防单容器拖垮宿主；裁剪 capabilities 减少内核攻击面。
- **怎么做**（示例，按需合并到 compose 的 `broker` 服务）：

```yaml
services:
  broker:
    read_only: true
    tmpfs:
      - /tmp
    security_opt:
      - no-new-privileges:true
    cap_drop: ["ALL"]
    deploy:
      resources:
        limits:
          cpus: "4"
          memory: 8g
```

- **怎么验证**：容器内尝试写根路径应失败（只读）；`docker inspect` 可见资源限额。
  **【未验证】**（本机未跑 Docker）；且**只读根文件系统需确认 `data_dir` 在可写卷上**，否则内核无法落盘。
- [ ] 已评估只读根文件系统与资源限额（注意 `data_dir` 必须在可写卷）

---

## E. 已知限制（当前确实做不到，不要指望）

以下均为**事实性缺口**，请在安全设计里显式承认，不要假设它们存在：

1. **口令是明文存储与复制的**。`meta/state.json` 里 `password` 字段明文（**【已验证】** 实测可见
   `"password":"drillpass"`）；配置文件中也是明文。**没有**口令哈希（哈希与外部认证后端留给认证插件）。
   → 后果：**数据目录与备份文件等同敏感凭据**，必须做文件权限与加密保护。
2. **没有审计日志**。管理面的增删改会打常规日志（如 `管理面更新用户 actor=... user=...`），
   但**没有**独立的、不可篡改的审计流，也没有"谁在什么时候改了什么"的合规级记录。
3. **没有 LDAP / OAuth2 / JWT 等外部认证**。v1 内置仅 `PLAIN` / `AMQPLAIN`
   （`auth.Store.Mechanisms()` 实测只返回这两个）。
4. **SASL `EXTERNAL` 未实现**：即使配了 mTLS，协议层**仍走 PLAIN 口令认证**
   （"用客户端证书免口令"这一步没有）。证书只是传输层校验。
5. **`remote_access` 无法经 API 设置**：管理 API/CLI 建的账号一律允许远端登录（见 A-4），
   无法把单个账号限制为仅本机。
6. **管理面无独立来源白名单 / 无限流**：只能靠绑定地址、防火墙、TLS 收敛暴露面。
7. **无插件沙箱**：A 形态插件与内核同进程；B 形态外部插件虽有进程隔离，但**数据面走本机连接代理**、
   且插件可调用内核语义（受 vhost 与权限校验约束），**不是**安全沙箱。
8. **管理面标签仅有 `administrator`/`management`/`monitoring` 三档**，没有更细的 per-resource RBAC。

---

## F. 汇总清单

- [ ] A-1 默认账号已改/已删 【已验证流程】
- [ ] A-2 业务账号最小权限（正则），无管理员标签 【已验证 403 路径】
- [ ] A-3 `administrator` 标签仅授予运维账号 【已验证隐式权限口径】
- [ ] A-4 特权账号来源限制已评估（注意 API 建号默认放开远端）
- [ ] B-1 对外端口启用 TLS，配错即拒绝启动 【已验证】
- [ ] B-2 `min_version` ≥ 1.2 【已验证】
- [ ] B-3 需要 mTLS 的端口配 `require_and_verify` + `ca_file`
- [ ] B-4 管理面启用 TLS
- [ ] C-1 管理面绑定地址收敛 【已验证监听地址】
- [ ] C-2 未用的协议插件禁用
- [ ] D-1 容器以非 root 运行
- [ ] D-2 只读根文件系统 / 资源限额 / 能力裁剪已评估
- [ ] E 已知限制（明文口令、无审计、无 LDAP/OAuth2、SASL EXTERNAL 未实现）已在安全设计中承认
