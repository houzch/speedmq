# Baseline ng Security Hardening ng SpeedMQ (checklist na maaaring tsekan)

> Prinsipyo: **isulat lamang ang mga kakayahang tunay na mayroon ang repository na ito**. Ang bawat isa ay may "bakit kailangan gawin + paano i-verify na nagawa", at ang lahat ng verification command ay maaaring patakbuhin.
> Ang nakamarkahan ng **【na-verify na】** ay nangangahulugang **talagang naisagawa** sa makinang ito (Windows + PowerShell 5.1, `1.0.0`);
> Ang **【hindi pa na-verify】** ay nangangahulugang hindi pa naisagawa o kasalukuyang hindi kayang gawin, at hindi kailanman nagkukunwari.
> Ang lahat ng command ay ibinibigay sa curl na bersyon na `/bin/sh` style, kasama ang PowerShell na bersyon (para sa PowerShell 5.1 gamitin ang
> `Invoke-WebRequest ... -UseBasicParsing`).

---

## A. Authentication at access control

### A-1. Palitan ang default account na `guest/guest` 【na-verify na】

- **Bakit**: May built-in na default na `guest/guest` (tag na `administrator`); ang pag-expose nito sa labas ay katumbas ng pagbukas ng pintuan.
- **Paano**: Palitan ang password / burahin ang account sa runtime; **huwag** baguhin ang config file (epektibo lang ang `users` sa unang bootstrap).

```bash
# Palitan ang password
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/users/guest \
     -H 'Content-Type: application/json' -d '{"password":"<新口令>","tags":["administrator"]}'
# O direktang burahin ang default account (siguraduhing nakagawa na ng bagong administrator)
curl -u guest:guest -X DELETE http://127.0.0.1:15672/api/users/guest
```

- **Paano i-verify**: Pagkatapos ng pagbabago, ang lumang password ay dapat 401 at ang bago ay 200.
  **【na-verify na】** Aktwal na output sa makinang ito:

  ```
  --- 改密前 guest/guest ---          HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  PUT /api/users/guest -> 204
  --- 改密后 guest/guest（应 401）---  HTTP 401
  --- 改密后 guest/s3cret（应 200）--- HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  ```
- [ ] Napalitan/nabura na ang default account

### A-2. Least privilege: regex na `configure` / `write` / `read` bawat vhost 【na-verify na】

- **Bakit**: Ang tatlong kategorya ay naka-align sa RabbitMQ —— pinamamahalaan ng `configure` ang deklarasyon/pagbura ng topology, ng `write` ang publish at binding, ng `read` ang consume at fetch;
  ang paglabag ay nagbabalik ng 403. Bigyan lamang ang business account ng kailangan nito.
- **Paano**: `PUT /api/permissions/{vhost}/{user}`, halimbawa read-only consumption: `{"configure":"^$","write":"^$","read":".*"}`.

```bash
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/permissions/%2F/appuser \
     -H 'Content-Type: application/json' -d '{"configure":"^$","write":"^$","read":".*"}'
```

- **Paano i-verify**: Gamit ang restricted account, subukang gumawa ng labag sa permission; dapat makakuha ng 403 `ACCESS_REFUSED`.
  **【na-verify na】** Sa makinang ito, gumamit ng tunay na AMQP client at nagdeklara ng exchange gamit ang user na may `configure="^$"`, aktwal na nasubok:

  ```
  FAIL 声明 durable 交换机失败: Exception (403) Reason: "ACCESS_REFUSED - access to configure 'r.ex' refused for user 'restricted'"
  ```
- [ ] Ang bawat business account ay binibigyan lamang ng kinakailangang regex, at hindi binibigyan ng `administrator`/`management` tag

### A-3. Ang pagtrato sa `administrator` tag (implicit na buong permission) —— mag-ingat sa pagbibigay 【na-verify na】

- **Bakit**: **Ang user na may `administrator` tag ay may buong permission sa lahat ng vhost na nakikita nito, nang walang kinakailangang permission record**
  (naka-align sa aktwal na pagtrato ng RabbitMQ, tingnan ang README / design M8-7). Ibig sabihin, sa sandaling ibigay ang tag na ito,
  hindi na gumagana ang permission regex——ito ang pinakamataas na permission.
- **Paano**: Ang management/ops account lamang ang bibigyan ng `administrator`; ang lahat ng business account ay walang tag, at dumadaan lamang sa permission regex.

- **Paano i-verify (demonstrasyon ng implicit permission)**: Sa isang bagong vhost na **walang anumang permission record**, ang `administrator` user ay dapat direktang magagamit.
  **【na-verify na】** Aktwal na nasubok sa makinang ito: pagkatapos gumawa ng bagong vhost na `drillvh` (na walang ginawang anumang permission record), matagumpay na nagdeklara ng topology ang `guest` (administrator) sa ibabaw nito:

  ```
  --- administrator 隐式权限：guest 在 drillvh 上声明拓扑 ---
  OK  已声明 durable 交换机 a.ex / 队列 a.q，并绑定 key=k   (exit=0)
  ```
- [ ] Ang `administrator` tag ay ibinibigay lamang sa napakakaunting ops account

### A-4. `remote_access`: paglimita sa account na lokal na pag-login lamang 【hindi pa na-verify (hindi kayang gayahin ang remote source sa parehong makina)】

- **Bakit**: Naka-align sa RabbitMQ, ang built-in na `guest` ay pinapayagan lamang ang lokal na pag-login bilang default; sa deployment na nakaharap sa labas, dapat siguraduhing limitado ang pinagmulan ng privileged account.
- **Paano / pagtrato (mahalagang limitasyon)**:
  - Ang `remote_access` ay maisusulat lamang sa **config file** sa ilalim ng `users.<name>.remote_access`, at **epektibo lamang sa unang bootstrap**;
  - **Ang account na ginawa sa pamamagitan ng management API / `speedmqctl` ay laging `remote_access=true`** (pinapayagan ang pag-login mula sa kahit anong pinagmulan)——
    base sa `UpsertUser` comment sa `internal/broker/observe.go` at sa aktwal na nasubok sa `meta/state.json` na
    `"remote_access":true`. Ibig sabihin, **sa kasalukuyan ay hindi kayang limitahan ng API ang isang account sa lokal lamang**.
- **Paano i-verify**: Mula sa **ibang host** (hindi `127.0.0.1`), kumonekta gamit ang account na iyon; dapat makatanggap ng 403; ang lokal na koneksyon ay dapat magtagumpay.
  **【hindi pa na-verify】**: Hindi kayang bumuo ng tunay na remote source ang kapaligiran ng makinang ito, kaya hindi pa naisagawa.
- [ ] Nasuri na ang paglimita sa pinagmulan ng privileged account ayon sa pagtrato sa itaas (tandaan: binuksan ng API ang remote bilang default kapag gumagawa ng account)

---

## B. Security ng transmission (TLS)

Mga TLS config item (parehong **ginagamit** ng access layer at management plane ang parehong set ng fields): `cert_file` / `key_file` / `ca_file` / `client_auth` / `min_version`.

### B-1. I-enable ang TLS at tanggihan ang pag-start kapag mali ang config 【na-verify na】

- **Bakit**: Binabasa at vini-verify ang certificate sa **oras ng pag-start** —— agad na tinatanggihan ang pag-start kapag mali, sa halip na ihayag lamang kapag nakakonekta na ang unang client.
- **Paano**: Magbigay ng `cert_file` + `key_file` sa `listeners.<plugin>[].tls` o `management.tls` (**parehong ibigay** bago ito mag-enable).

- **Paano i-verify**: Simulan gamit ang maling config; dapat agad na mabigo.
  **【na-verify na】** Tatlong maling config ang aktwal na nasubok sa makinang ito, lahat `exit=1` at tinanggihan ang pag-start:

  ```
  badtls1: speedmqd 启动失败: listeners.amqp091[0].tls 需要同时提供 cert_file 与 key_file
  badtls2: speedmqd 启动失败: listeners.amqp091[0].tls.min_version 取值非法: "1.0"（可选 1.2 / 1.3）
  badtls3: speedmqd 启动失败: listeners.amqp091[0].tls 无效: 加载服务端证书失败（cert=... key=...）: open ...: The system cannot find the path specified.
  ```
- **Paano i-verify (positibo/negatibo)**: Nakakakonekta ang TLS client, at ang plaintext client na kumokonekta sa TLS port ay tatanggihan.
  **【na-verify na】** Nagpatakbo ng TLS instance sa makinang ito (`amqp091` sa TLS), gamit ang tunay na client probe:

  ```
  === 正向：TLS 客户端跑全量探针 ===      全部通过（25/25）: ...
  === 反向：明文客户端连 TLS 端口（应失败）===  FAIL ... 拨号失败: Exception (501) Reason: "EOF"   (exit=-1)
  ```
- [ ] Naka-enable ang TLS sa mga protocol port na nakaharap sa labas

### B-2. `min_version` na hindi bababa sa 1.2 【na-verify na】

- **Bakit**: Hindi pinapagana ang lumang TLS version; ang default ay `1.2`, at maaaring pumili ng `1.2` / `1.3`.
- **Paano i-verify**: Isulat ang `min_version` bilang `1.0`; dapat mag-error ang pag-start (tingnan ang `badtls2` output sa B-1).
- [ ] Ang `min_version` ay `1.2` o `1.3`

### B-3. Mutual authentication `client_auth: require_and_verify` (mTLS) 【bahagyang na-verify】

- **Bakit**: Hinihiling ang client na magpakita at magpapatunay ng certificate, upang maiwasan ang hindi awtorisadong client mula sa pagkonekta sa protocol ports.
- **Paano**: I-configure ang `ca_file` + `client_auth: require_and_verify` (ang huling dalawa ay nangangailangan ng sabay na `ca_file`).
- **【na-verify na】**: Ang end-to-end ng TLS at ang tinatanggihang path ay na-verify gamit ang tunay na client (B-1). **Ang mTLS (paghiling at pag-verify ng client certificate) ay hindi hiwalay na naisagawa sa makinang ito**.
- [ ] Ang mga port na nangangailangan ng mTLS ay may `require_and_verify` + `ca_file`

### B-4. TLS ng management plane 【hindi pa na-verify】

- **Bakit**: Ang management plane ay gumagamit ng Basic Auth upang magpadala ng password, kaya obligadong i-encrypt.
- **Paano**: Ang `management.tls` ay gumagamit ng parehong fields tulad ng protocol listeners.
- **【hindi pa na-verify】**: Ang drill sa makinang ito ay nag-bind ng management plane sa lokal na plaintext port, at hindi hiwalay na nagpatakbo ng management plane HTTPS.
- [ ] Naka-enable ang TLS sa management plane (o mahigpit na limitado sa loob ng pinagkakatiwalaang network)

---

## C. Pagliit ng attack surface

### C-1. Pagliit ng listen scope ng management plane 【na-verify na (aktwal na sinubok ang listen address)】

- **Bakit**: Ang management plane ay default na `:15672` (lahat ng network interface). Sa deployment na nakaharap sa labas, dapat i-bind sa intranet/loopback address, o limitahan ang pinagmulan gamit ang firewall.
- **Paano**: I-configure ang `management.addr` bilang `127.0.0.1:15672` o isang intranet address; o ganap na isara gamit ang `management.enabled=false`
  (kapag isinara, walang management port, ngunit hindi na rin magagamit ang `speedmqctl`).
- **Paano i-verify**:
  **【na-verify na】** Ni-configure ang management plane sa makinang ito bilang `127.0.0.1:15677`, at ang aktwal na listen address ay talagang loopback:

  ```
  LocalAddress LocalPort
  ------------ ---------
  127.0.0.1        15677
  ```
- [ ] Nabawasan na ang bind address ng management plane (o hindi na ginagamit)

### C-2. Buksan lamang ang kinakailangang protocol port 【hindi pa na-verify】

- **Bakit**: Bilang default, sabay na binubuksan ang AMQP `5672` at MQTT `1883`; kung hindi gagamitin ang MQTT, isara ito upang mabawasan ang attack surface.
- **Paano**: `plugins.mqtt.enabled=false` (o alisin mula sa `listeners`); ang pag-disable ay **pagsasara ng tunay na port**, hindi lamang pagbabago ng status bit.
- **Paano i-verify**: Pagkatapos ng pag-disable, hindi na nakikinig ang katumbas na port (hindi makikita sa `Get-NetTCPConnection -State Listen`).
  **【hindi pa na-verify】**: Ang drill sa makinang ito ay parehong bukas ang dalawang protocol, at hindi hiwalay na na-verify ang pagkawala ng port pagkatapos isara.
- [ ] Na-disable na ang hindi ginagamit na protocol plugin

---

## D. Hardening ng pagtakbo ng container

Mga katotohanan ng image ng repository (`Dockerfile`): statically linked binary + alpine, **tumatakbo bilang non-root (uid 10001, user na `speedmq`)**,
at ang data directory na `/var/lib/speedmq` ay isang volume. Ang `docker-compose.yml` ay gumagamit ng **named volume** para sa persistence, **read-only mount** ng config, at log rotation.

### D-1. Pagtakbo bilang non-root 【hindi pa na-verify (hindi pinatakbo ang Docker sa makinang ito)】

- **Bakit**: Least privilege, binabawasan ang epekto kapag naka-escape ang container.
- **Paano**: Ang image ay default nang uid 10001; **huwag** i-override gamit ang `--user root`.
- **Paano i-verify**: Ang `docker compose run -T --rm broker id` ay dapat magpakita ng `uid=10001`. (obligadong may `-T` ang `run` sa non-interactive na kapaligiran)
- [ ] Tumatakbo ang container bilang non-root (hindi na-override ng root)

### D-2. Read-only root filesystem + resource limits + capability trimming (rekomendasyon, hindi naka-enable bilang default sa compose ng repository) 【hindi pa na-verify】

- **Bakit**: Ang read-only root filesystem ay pumipigil sa runtime na pagbabago ng binary; ang resource limits ay pumipigil sa iisang container na makapinsala sa host; ang pag-trim ng capabilities ay nagpapababa ng kernel attack surface.
- **Paano** (halimbawa, pagsamahin sa `broker` service ng compose kung kinakailangan):

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

- **Paano i-verify**: Ang pagsubok na magsulat sa root path sa loob ng container ay dapat mabigo (read-only); makikita ang resource limits sa `docker inspect`.
  **【hindi pa na-verify】** (hindi pinatakbo ang Docker sa makinang ito); at **ang read-only root filesystem ay kailangang kumpirmahin na ang `data_dir` ay nasa writable volume**, kung hindi ay hindi makakasulat sa disk ang kernel.
- [ ] Nasuri na ang read-only root filesystem at resource limits (tandaan: obligadong nasa writable volume ang `data_dir`)

---

## E. Mga kilalang limitasyon (kasalukuyang tunay na hindi kaya, huwag umasa)

Ang mga sumusunod ay lahat **faktwal na kakulangan**; tahasang kilalanin ang mga ito sa disenyo ng seguridad, at huwag ipagpalagay na mayroon ang mga ito:

1. **Ang mga password ay nakaimbak at nakokopya bilang plaintext**. Ang `password` field sa `meta/state.json` ay plaintext (**【na-verify na】** aktwal na makikita ang
   `"password":"drillpass"`); plaintext din sa config file. **Walang** password hashing (ang hashing at external authentication backend ay iniiwan sa auth plugin).
   → Bunga: **ang data directory at backup file ay katumbas ng sensitibong credential**, kaya obligado ang proteksyon sa file permissions at encryption.
2. **Walang audit log**. Ang mga pagdaragdag/pagbura/pagbabago sa management plane ay nagre-record ng regular na log (tulad ng `管理面更新用户 actor=... user=...`),
   ngunit **walang** independiyente, hindi mababagong audit stream, at walang compliance-level na record ng "sino ang nagbago ng ano at kailan".
3. **Walang external authentication tulad ng LDAP / OAuth2 / JWT**. Ang built-in ng v1 ay `PLAIN` / `AMQPLAIN` lamang
   (ang `auth.Store.Mechanisms()` ay aktwal na nagbabalik lamang ng dalawang ito).
4. **Hindi pa naipatupad ang SASL `EXTERNAL`**: kahit naka-configure ang mTLS, ang protocol layer ay **gumagamit pa rin ng PLAIN password authentication**
   (wala pa ang hakbang na "laktawan ang password gamit ang client certificate"). Ang certificate ay verification sa transport layer lamang.
5. **Hindi maisasaayos ang `remote_access` sa pamamagitan ng API**: Ang account na ginawa ng management API/CLI ay laging pinapayagan ang remote login (tingnan ang A-4),
   at hindi kayang limitahan ang isang account sa lokal lamang.
6. **Walang independiyenteng source whitelist / walang rate limiting ang management plane**: maaari lamang mabawasan ang attack surface sa pamamagitan ng bind address, firewall, at TLS.
7. **Walang plugin sandbox**: Ang form A plugin ay kasa-process ng kernel; bagaman may process isolation ang form B external plugin, ang **data plane ay dumadaan sa lokal na connection proxy**,
   at ang plugin ay makakapagtawag ng kernel semantics (limitado ng vhost at permission verification), **hindi** ito security sandbox.
8. **Ang management plane tags ay may tatlong antas lamang na `administrator`/`management`/`monitoring`**, walang mas detalyadong per-resource RBAC.

---

## F. Checklist ng buod

- [ ] A-1 Napalitan/nabura na ang default account 【na-verify na ang proseso】
- [ ] A-2 Least privilege ng business account (regex), walang administrator tag 【na-verify na ang 403 path】
- [ ] A-3 Ang `administrator` tag ay ibinibigay lamang sa ops account 【na-verify na ang pagtrato sa implicit permission】
- [ ] A-4 Nasuri na ang paglimita sa pinagmulan ng privileged account (tandaan: binuksan ng API ang remote bilang default kapag gumagawa ng account)
- [ ] B-1 Naka-enable ang TLS sa mga port na nakaharap sa labas, at tinatanggihan ang pag-start kapag mali ang config 【na-verify na】
- [ ] B-2 `min_version` ≥ 1.2 【na-verify na】
- [ ] B-3 Ang mga port na nangangailangan ng mTLS ay may `require_and_verify` + `ca_file`
- [ ] B-4 Naka-enable ang TLS sa management plane
- [ ] C-1 Nabawasan ang bind address ng management plane 【na-verify na ang listen address】
- [ ] C-2 Na-disable ang hindi ginagamit na protocol plugin
- [ ] D-1 Tumatakbo ang container bilang non-root
- [ ] D-2 Nasuri na ang read-only root filesystem / resource limits / capability trimming
- [ ] E Ang mga kilalang limitasyon (plaintext password, walang audit, walang LDAP/OAuth2, hindi pa naipatupad ang SASL EXTERNAL) ay kinilala na sa disenyo ng seguridad
