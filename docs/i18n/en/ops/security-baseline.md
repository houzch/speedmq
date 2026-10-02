# SwiftMQ Security Hardening Baseline (checklist)

> Principle: **document only capabilities this repository actually has**. Each item gives "why do it + how to verify it was done", and every verification command can be run.
> Items marked **【Verified】** mean they were **actually executed** on this machine (Windows + PowerShell 5.1, `1.0.0`);
> **【Not verified】** means not executed or currently not possible — never pretend.
> All commands are given as a `/bin/sh`-style curl version, with a PowerShell version attached (for PowerShell 5.1 use
> `Invoke-WebRequest ... -UseBasicParsing`).

---

## A. Authentication and access control

### A-1. Change the default account `guest/guest` 【Verified】

- **Why**: `guest/guest` (tag `administrator`) is built in by default, and exposing it externally is like leaving the door wide open.
- **How**: change the password / delete the account at runtime; do **not** edit the config file (`users` takes effect only on first bootstrap).

```bash
# Change the password
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/users/guest \
     -H 'Content-Type: application/json' -d '{"password":"<新口令>","tags":["administrator"]}'
# Or delete the default account directly (make sure a new administrator has been created first)
curl -u guest:guest -X DELETE http://127.0.0.1:15672/api/users/guest
```

- **How to verify**: after the change, the old password must get 401 and the new password 200.
  **【Verified】** Output measured on this machine:

  ```
  --- 改密前 guest/guest ---          HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  PUT /api/users/guest -> 204
  --- 改密后 guest/guest（应 401）---  HTTP 401
  --- 改密后 guest/s3cret（应 200）--- HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  ```
- [ ] Default account changed/deleted

### A-2. Least privilege: per-vhost `configure` / `write` / `read` regexes 【Verified】

- **Why**: the three categories align with RabbitMQ — `configure` governs topology declaration/deletion, `write` governs publishing and binding, and `read` governs consuming and fetching;
  unauthorized access returns 403. Grant a business account only what it needs.
- **How**: `PUT /api/permissions/{vhost}/{user}`, for example read-only consumption: `{"configure":"^$","write":"^$","read":".*"}`.

```bash
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/permissions/%2F/appuser \
     -H 'Content-Type: application/json' -d '{"configure":"^$","write":"^$","read":".*"}'
```

- **How to verify**: use a restricted account to attempt an unauthorized operation; you should get 403 `ACCESS_REFUSED`.
  **【Verified】** On this machine, a real AMQP client using a user with `configure="^$"` attempted to declare an exchange, measured:

  ```
  FAIL 声明 durable 交换机失败: Exception (403) Reason: "ACCESS_REFUSED - access to configure 'r.ex' refused for user 'restricted'"
  ```
- [ ] Each business account is granted only the necessary regexes and is not given the `administrator`/`management` tag

### A-3. The semantics of the `administrator` tag (implicit full permissions) — grant with caution 【Verified】

- **Why**: **a user with the `administrator` tag has full permissions on all vhosts visible to them, with no permission records needed**
  (aligned with the measured RabbitMQ semantics; see the README / design M8-7). In other words, as soon as this tag is granted,
  the permission regexes no longer apply — it is the highest privilege.
- **How**: grant `administrator` only to management-plane/operations accounts; give business accounts no tags at all and rely solely on permission regexes.

- **How to verify (demonstrating implicit permissions)**: for a new vhost with **no permission records at all**, an `administrator` user should be able to use it directly.
  **【Verified】** Measured on this machine: after creating the vhost `drillvh` (with no permission records created), `guest` (administrator) successfully declared a topology on it:

  ```
  --- administrator 隐式权限：guest 在 drillvh 上声明拓扑 ---
  OK  已声明 durable 交换机 a.ex / 队列 a.q，并绑定 key=k   (exit=0)
  ```
- [ ] The `administrator` tag is granted to only a very small number of operations accounts

### A-4. `remote_access`: restrict an account to local login only 【Not verified (a remote source cannot be simulated on the same machine)】

- **Why**: aligned with RabbitMQ, the built-in `guest` allows only local login by default; when deploying externally, ensure that privileged accounts' sources are restricted.
- **How / semantics (important limitation)**:
  - `remote_access` can only be written in the **config file** under `users.<name>.remote_access`, and it **takes effect only on first bootstrap**;
  - **Accounts created via the management API / `swiftmqctl` always have `remote_access=true`** (allowing login from any source) —
    based on the `UpsertUser` comment in `internal/broker/observe.go` and the measured `"remote_access":true` in
    `meta/state.json`. That is, **the API currently cannot restrict a given account to local-only**.
- **How to verify**: connect from **another host** (not `127.0.0.1`) with that account; it should get 403, while a local connection should succeed.
  **【Not verified】**: this machine's environment cannot construct a real remote source, so it was not measured.
- [ ] Privileged accounts' source restrictions have been evaluated per the semantics above (note that accounts created via the API allow remote access by default)

---

## B. Transport security (TLS)

TLS configuration options (the access layer and management plane **share** the same set of fields): `cert_file` / `key_file` / `ca_file` / `client_auth` / `min_version`.

### B-1. Enable TLS; misconfiguration refuses to start 【Verified】

- **Why**: certificates are read and validated **at startup** — a misconfiguration refuses to start immediately, rather than being exposed only when the first client connects.
- **How**: provide `cert_file` + `key_file` under `listeners.<plugin>[].tls` or `management.tls` (**both must be given** for it to be enabled).

- **How to verify**: start with a misconfiguration; it should fail immediately.
  **【Verified】** Three misconfigurations were measured on this machine, all with `exit=1`, refusing to start:

  ```
  badtls1: swiftmqd 启动失败: listeners.amqp091[0].tls 需要同时提供 cert_file 与 key_file
  badtls2: swiftmqd 启动失败: listeners.amqp091[0].tls.min_version 取值非法: "1.0"（可选 1.2 / 1.3）
  badtls3: swiftmqd 启动失败: listeners.amqp091[0].tls 无效: 加载服务端证书失败（cert=... key=...）: open ...: The system cannot find the path specified.
  ```
- **How to verify (positive/negative)**: a TLS client can connect, and a plaintext client connecting to the TLS port is refused.
  **【Verified】** A TLS instance was started on this machine (`amqp091` over TLS) and probed with a real client:

  ```
  === 正向：TLS 客户端跑全量探针 ===      全部通过（25/25）: ...
  === 反向：明文客户端连 TLS 端口（应失败）===  FAIL ... 拨号失败: Exception (501) Reason: "EOF"   (exit=-1)
  ```
- [ ] TLS is enabled on externally exposed protocol ports

### B-2. `min_version` at least 1.2 【Verified】

- **Why**: disable overly old TLS versions; the default is `1.2`, with `1.2` / `1.3` available.
- **How to verify**: set `min_version` to `1.0`; startup should report an error (see the `badtls2` output in B-1).
- [ ] `min_version` is `1.2` or `1.3`

### B-3. Mutual authentication `client_auth: require_and_verify` (mTLS) 【Partially verified】

- **Why**: require clients to present and validate a certificate, preventing unauthorized clients from accessing protocol ports.
- **How**: configure `ca_file` + `client_auth: require_and_verify` (the latter two require `ca_file` to also be provided).
- **【Verified】**: the TLS end-to-end and refusal paths were verified with a real client (B-1). **mTLS (requiring and validating client certificates) was not drilled separately on this machine**.
- [ ] Ports needing mTLS are configured with `require_and_verify` + `ca_file`

### B-4. Management-plane TLS 【Not verified】

- **Why**: the management plane transmits passwords via Basic Auth, so it must be encrypted.
- **How**: `management.tls` uses the same fields as the protocol listeners.
- **【Not verified】**: the drill on this machine bound the management plane to a local plaintext port and did not set up management-plane HTTPS separately.
- [ ] Management-plane TLS is enabled (or it is strictly restricted to a trusted network)

---

## C. Attack surface reduction

### C-1. Narrow the management-plane listen scope 【Verified (listen address measured)】

- **Why**: the management plane defaults to `:15672` (all network interfaces). For external deployment, bind it to an internal/loopback address, or restrict sources with a firewall.
- **How**: set `management.addr` to `127.0.0.1:15672` or an internal address; or shut it down entirely with `management.enabled=false`
  (after shutdown there is no management port, but `swiftmqctl` becomes unusable as well).
- **How to verify**:
  **【Verified】** On this machine the management plane was set to `127.0.0.1:15677`, and the measured listen address was indeed loopback:

  ```
  LocalAddress LocalPort
  ------------ ---------
  127.0.0.1        15677
  ```
- [ ] The management-plane bind address is narrowed (or it is disabled)

### C-2. Open only the required protocol ports 【Not verified】

- **Why**: by default both AMQP `5672` and MQTT `1883` are open; if you do not use MQTT, turn it off to reduce the attack surface.
- **How**: `plugins.mqtt.enabled=false` (or remove it from `listeners`); disabling **actually closes the real port**, not just flips a status flag.
- **How to verify**: after disabling, the corresponding port is no longer listening (not visible via `Get-NetTCPConnection -State Listen`).
  **【Not verified】**: in the drill on this machine both protocols were left enabled, so the port disappearing after disabling was not verified separately.
- [ ] Unused protocol plugins are disabled

---

## D. Container runtime hardening

Repository image facts (`Dockerfile`): statically linked binary + alpine, **running as non-root (uid 10001, user `swiftmq`)**,
with the data directory `/var/lib/swiftmq` as a volume. `docker-compose.yml` uses a **named volume** for persistence, a **read-only mount** for the configuration, and log rotation.

### D-1. Non-root execution 【Not verified (Docker was not run on this machine)】

- **Why**: least privilege, reducing the impact of a container escape.
- **How**: the image is already uid 10001 by default; do **not** override it with `--user root`.
- **How to verify**: `docker compose run -T --rm broker id` should show `uid=10001`. (`run` must be given `-T` in a non-interactive environment)
- [ ] The container runs as non-root (not overridden to root)

### D-2. Read-only root filesystem + resource limits + capability dropping (recommended; not enabled by default in the repository's compose) 【Not verified】

- **Why**: a read-only root filesystem prevents runtime tampering with the binary; resource limits prevent a single container from dragging down the host; dropping capabilities reduces the kernel attack surface.
- **How** (example; merge into the compose `broker` service as needed):

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

- **How to verify**: attempting to write to the root path inside the container should fail (read-only); resource limits are visible via `docker inspect`.
  **【Not verified】** (Docker was not run on this machine); also, **with a read-only root filesystem you must confirm that `data_dir` is on a writable volume**, otherwise the kernel cannot persist to disk.
- [ ] A read-only root filesystem and resource limits have been evaluated (note that `data_dir` must be on a writable volume)

---

## E. Known limitations (currently impossible; do not count on them)

The following are all **factual gaps**; explicitly acknowledge them in your security design and do not assume they exist:

1. **Passwords are stored and copied in plaintext**. The `password` field in `meta/state.json` is plaintext (**【Verified】** measured:
   `"password":"drillpass"` is visible); it is also plaintext in the config file. There is **no** password hashing (hashing and external authentication backends are left to authentication plugins).
   → Consequence: the **data directory and backup files are equivalent to sensitive credentials** and must be protected with file permissions and encryption.
2. **No audit log**. Create/update/delete operations on the management plane are written to the regular log (e.g. `管理面更新用户 actor=... user=...`),
   but there is **no** independent, tamper-proof audit stream and no compliance-grade record of "who changed what and when".
3. **No external authentication such as LDAP / OAuth2 / JWT**. v1 only has `PLAIN` / `AMQPLAIN` built in
   (`auth.Store.Mechanisms()` was measured to return only these two).
4. **SASL `EXTERNAL` is not implemented**: even with mTLS configured, the protocol layer **still uses PLAIN password authentication**
   (the "use the client certificate to skip the password" step is missing). The certificate is only a transport-layer check.
5. **`remote_access` cannot be set via the API**: accounts created via the management API/CLI always allow remote login (see A-4),
   and a single account cannot be restricted to local-only.
6. **The management plane has no independent source whitelist / no rate limiting**: the attack surface can only be reduced via the bind address, a firewall, and TLS.
7. **No plugin sandbox**: form A plugins share the process with the kernel; form B external plugins have process isolation, but
   their **data plane goes through a local connection proxy**, and plugins can invoke kernel semantics (subject to vhost and permission checks) — this is **not** a security sandbox.
8. **Management-plane tags have only three levels, `administrator`/`management`/`monitoring`**, with no finer per-resource RBAC.

---

## F. Summary checklist

- [ ] A-1 Default account changed/deleted 【flow verified】
- [ ] A-2 Least privilege for business accounts (regexes), no administrator tag 【403 path verified】
- [ ] A-3 The `administrator` tag is granted only to operations accounts 【implicit-permission semantics verified】
- [ ] A-4 Privileged accounts' source restrictions evaluated (note that accounts created via the API allow remote by default)
- [ ] B-1 TLS enabled on external ports; misconfiguration refuses to start 【Verified】
- [ ] B-2 `min_version` ≥ 1.2 【Verified】
- [ ] B-3 Ports needing mTLS configured with `require_and_verify` + `ca_file`
- [ ] B-4 Management-plane TLS enabled
- [ ] C-1 Management-plane bind address narrowed 【listen address verified】
- [ ] C-2 Unused protocol plugins disabled
- [ ] D-1 Container runs as non-root
- [ ] D-2 Read-only root filesystem / resource limits / capability dropping evaluated
- [ ] E Known limitations (plaintext passwords, no audit, no LDAP/OAuth2, SASL EXTERNAL not implemented) acknowledged in the security design
