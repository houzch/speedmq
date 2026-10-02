# Baseline di hardening della sicurezza di SwiftMQ (checklist con caselle)

> Principio: **descrivere solo le capacità realmente presenti in questo repository**. Per ciascuna voce si indica "perché farlo + come verificare di averlo fatto"; i comandi di verifica sono tutti eseguibili.
> La dicitura **【verificato】** indica che è stato **davvero eseguito** sulla macchina locale (Windows + PowerShell 5.1, `1.0.0`);
> **【non verificato】** indica che non è stato eseguito o che al momento è impossibile, senza mai fingere.
> Tutti i comandi sono forniti nella versione curl in stile `/bin/sh`, con annessa la versione PowerShell (con PowerShell 5.1 usare
> `Invoke-WebRequest ... -UseBasicParsing`).

---

## A. Autenticazione e controllo degli accessi

### A-1. Modifica dell'account predefinito `guest/guest` 【verificato】

- **Perché**: per impostazione predefinita è integrato `guest/guest` (tag `administrator`); esporlo all'esterno equivale a spalancare la porta.
- **Come fare**: modifica la password / elimina l'account a runtime; **non** modificare il file di configurazione (`users` ha effetto solo al primo bootstrap).

```bash
# Modifica la password
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/users/guest \
     -H 'Content-Type: application/json' -d '{"password":"<新口令>","tags":["administrator"]}'
# Oppure elimina direttamente l'account predefinito (assicurati prima di aver creato il nuovo amministratore)
curl -u guest:guest -X DELETE http://127.0.0.1:15672/api/users/guest
```

- **Come verificare**: dopo la modifica, la vecchia password deve restituire 401 e la nuova 200.
  **【verificato】** Output misurato localmente:

  ```
  --- 改密前 guest/guest ---          HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  PUT /api/users/guest -> 204
  --- 改密后 guest/guest（应 401）---  HTTP 401
  --- 改密后 guest/s3cret（应 200）--- HTTP 200 {"auth_backend":"internal","name":"guest","tags":"administrator"}
  ```
- [ ] L'account predefinito è stato modificato/eliminato

### A-2. Privilegio minimo: regex `configure` / `write` / `read` per vhost 【verificato】

- **Perché**: la tripartizione è allineata a RabbitMQ — `configure` governa la dichiarazione/eliminazione della topologia, `write` la pubblicazione e i binding, `read` il consumo e il prelievo;
  un accesso non autorizzato restituisce 403. Agli account applicativi concedi solo ciò che serve.
- **Come fare**: `PUT /api/permissions/{vhost}/{user}`, ad esempio consumo in sola lettura: `{"configure":"^$","write":"^$","read":".*"}`.

```bash
curl -u guest:guest -X PUT http://127.0.0.1:15672/api/permissions/%2F/appuser \
     -H 'Content-Type: application/json' -d '{"configure":"^$","write":"^$","read":".*"}'
```

- **Come verificare**: con un account limitato prova un'operazione non autorizzata; deve restituire 403 `ACCESS_REFUSED`.
  **【verificato】** Sulla macchina locale, dichiarando un exchange con un client AMQP reale e un utente con `configure="^$"`, si è misurato:

  ```
  FAIL 声明 durable 交换机失败: Exception (403) Reason: "ACCESS_REFUSED - access to configure 'r.ex' refused for user 'restricted'"
  ```
- [ ] Ogni account applicativo ha solo le regex necessarie e non ha i tag `administrator`/`management`

### A-3. Il criterio del tag `administrator` (permessi completi impliciti) — concederlo con cautela 【verificato】

- **Perché**: **un utente con il tag `administrator` ha permessi completi su tutti i vhost a lui visibili, senza bisogno di record di permessi**
  (allineato al criterio misurato di RabbitMQ, vedi README / progetto M8-7). In altre parole, una volta assegnato questo tag
  le regex dei permessi non hanno più effetto — è il privilegio massimo.
- **Come fare**: assegna `administrator` solo agli account di gestione/operativi; agli account applicativi non dare alcun tag e usa solo le regex dei permessi.

- **Come verificare (dimostrazione dei permessi impliciti)**: su un nuovo vhost **senza alcun record di permessi**, l'utente `administrator` deve poter operare direttamente.
  **【verificato】** Misurato localmente: dopo aver creato il vhost `drillvh` (senza alcun record di permessi), `guest` (administrator) ha dichiarato con successo una topologia su di esso:

  ```
  --- administrator 隐式权限：guest 在 drillvh 上声明拓扑 ---
  OK  已声明 durable 交换机 a.ex / 队列 a.q，并绑定 key=k   (exit=0)
  ```
- [ ] Il tag `administrator` è concesso solo a pochissimi account operativi

### A-4. `remote_access`: limitare l'account al solo login locale 【non verificato (sulla stessa macchina non è possibile simulare una sorgente remota)】

- **Perché**: in linea con RabbitMQ, l'utente integrato `guest` per impostazione predefinita consente il login solo dalla macchina locale; in una distribuzione esposta all'esterno occorre assicurarsi che l'origine degli account privilegiati sia limitata.
- **Come fare / criterio (limitazione importante)**:
  - `remote_access` può essere scritto solo nel **file di configurazione**, in `users.<name>.remote_access`, e **ha effetto solo al primo bootstrap**;
  - **gli account creati tramite l'API di gestione / `swiftmqctl` hanno sempre `remote_access=true`** (consentono il login da qualsiasi origine) —
    base: il commento di `UpsertUser` in `internal/broker/observe.go` e `"remote_access":true` misurato in `meta/state.json`.
    In altre parole **al momento l'API non può limitare un account al solo accesso locale**.
- **Come verificare**: connettendosi con quell'account da **un'altra macchina** (diversa da `127.0.0.1`) si deve ricevere 403; la connessione locale deve riuscire.
  **【non verificato】**: l'ambiente locale non consente di costruire una vera origine remota, quindi non è stato misurato.
- [ ] La limitazione dell'origine degli account privilegiati è stata valutata secondo il criterio sopra (attenzione: gli account creati via API abilitano di default l'accesso remoto)

---

## B. Sicurezza del trasporto (TLS)

Le voci di configurazione TLS (il livello di ingresso e il piano di gestione **condividono** lo stesso insieme di campi): `cert_file` / `key_file` / `ca_file` / `client_auth` / `min_version`.

### B-1. Abilitazione di TLS e rifiuto dell'avvio in caso di configurazione errata 【verificato】

- **Perché**: i certificati vengono letti e validati **all'avvio** — in caso di configurazione errata l'avvio viene rifiutato immediatamente, invece di rivelare il problema solo quando si collega il primo client.
- **Come fare**: fornisci `cert_file` + `key_file` in `listeners.<plugin>[].tls` o `management.tls` (l'attivazione richiede di **fornirli entrambi**).

- **Come verificare**: avvia con una configurazione errata; deve fallire immediatamente.
  **【verificato】** Tre configurazioni errate misurate localmente, tutte con `exit=1` e avvio rifiutato:

  ```
  badtls1: swiftmqd 启动失败: listeners.amqp091[0].tls 需要同时提供 cert_file 与 key_file
  badtls2: swiftmqd 启动失败: listeners.amqp091[0].tls.min_version 取值非法: "1.0"（可选 1.2 / 1.3）
  badtls3: swiftmqd 启动失败: listeners.amqp091[0].tls 无效: 加载服务端证书失败（cert=... key=...）: open ...: The system cannot find the path specified.
  ```
- **Come verificare (diretto/inverso)**: un client TLS riesce a connettersi, mentre un client in chiaro che si connette alla porta TLS viene rifiutato.
  **【verificato】** Sulla macchina locale è stata avviata un'istanza TLS (`amqp091` su TLS) e sondata con client reali:

  ```
  === 正向：TLS 客户端跑全量探针 ===      全部通过（25/25）: ...
  === 反向：明文客户端连 TLS 端口（应失败）===  FAIL ... 拨号失败: Exception (501) Reason: "EOF"   (exit=-1)
  ```
- [ ] TLS è abilitato sulle porte dei protocolli esposte all'esterno

### B-2. `min_version` almeno 1.2 【verificato】

- **Perché**: disabilita versioni TLS troppo vecchie; il valore predefinito è già `1.2`, con `1.2` / `1.3` disponibili.
- **Come verificare**: impostando `min_version` a `1.0`, l'avvio deve dare errore (vedi l'output `badtls2` di B-1).
- [ ] `min_version` è `1.2` o `1.3`

### B-3. Autenticazione reciproca `client_auth: require_and_verify` (mTLS) 【parzialmente verificato】

- **Perché**: richiede che il client presenti e validi un certificato, impedendo a client non autorizzati di accedere alle porte dei protocolli.
- **Come fare**: configura `ca_file` + `client_auth: require_and_verify` (gli ultimi due richiedono di fornire anche `ca_file`).
- **【verificato】**: il TLS end-to-end e il percorso di rifiuto sono stati verificati con client reali (B-1). **mTLS (richiesta e validazione del certificato client) non è stato provato separatamente sulla macchina locale**.
- [ ] Le porte che richiedono mTLS hanno `require_and_verify` + `ca_file`

### B-4. TLS del piano di gestione 【non verificato】

- **Perché**: il piano di gestione trasmette la password tramite Basic Auth e deve quindi essere cifrato.
- **Come fare**: `management.tls` usa gli stessi campi dei listener di protocollo.
- **【non verificato】**: nella prova locale il piano di gestione è stato associato a una porta in chiaro locale; non è stato avviato separatamente un piano di gestione HTTPS.
- [ ] Il piano di gestione ha TLS abilitato (o è rigorosamente limitato a una rete fidata)

---

## C. Riduzione della superficie esposta

### C-1. Riduzione dell'ambito di ascolto del piano di gestione 【verificato (indirizzo di ascolto misurato)】

- **Perché**: per impostazione predefinita il piano di gestione usa `:15672` (tutte le schede di rete). In una distribuzione esposta all'esterno occorre associarlo a un indirizzo di rete interna/loopback, oppure limitare le origini con un firewall.
- **Come fare**: imposta `management.addr` a `127.0.0.1:15672` o a un indirizzo di rete interna; oppure disattivalo del tutto con `management.enabled=false`
  (una volta chiuso non c'è più la porta di gestione, ma anche `swiftmqctl` diventa inutilizzabile).
- **Come verificare**:
  **【verificato】** Sulla macchina locale il piano di gestione è stato configurato su `127.0.0.1:15677` e l'indirizzo di ascolto misurato è effettivamente il loopback:

  ```
  LocalAddress LocalPort
  ------------ ---------
  127.0.0.1        15677
  ```
- [ ] L'indirizzo di bind del piano di gestione è stato ristretto (o il piano è stato disattivato)

### C-2. Aprire solo le porte di protocollo necessarie 【non verificato】

- **Perché**: per impostazione predefinita si aprono sia AMQP `5672` sia MQTT `1883`; se MQTT non serve, disattivalo per ridurre la superficie di attacco.
- **Come fare**: `plugins.mqtt.enabled=false` (o rimuovilo da `listeners`); la disattivazione comporta la **chiusura effettiva della porta**, non solo il cambio di un flag di stato.
- **Come verificare**: dopo la disattivazione la porta corrispondente non è più in ascolto (non compare in `Get-NetTCPConnection -State Listen`).
  **【non verificato】**: nella prova locale entrambi i protocolli erano attivi e non è stata verificata separatamente la scomparsa della porta dopo la chiusura.
- [ ] I plugin di protocollo non utilizzati sono disabilitati

---

## D. Hardening dell'esecuzione in container

Fatti sull'immagine del repository (`Dockerfile`): binario con link statico + alpine, **eseguita come non root (uid 10001, utente `swiftmq`)**,
con la directory dei dati `/var/lib/swiftmq` come volume. `docker-compose.yml` usa un **volume con nome** per la persistenza, monta la configurazione **in sola lettura** e applica la rotazione dei log.

### D-1. Esecuzione come non root 【non verificato (Docker non è stato eseguito sulla macchina locale)】

- **Perché**: privilegio minimo, per ridurre l'impatto in caso di fuga dal container.
- **Come fare**: l'immagine è già uid 10001 per impostazione predefinita; **non** sovrascriverlo con `--user root`.
- **Come verificare**: `docker compose run -T --rm broker id` deve mostrare `uid=10001`. (In un ambiente non interattivo `run` deve includere `-T`)
- [ ] Il container viene eseguito come non root (non sovrascritto con root)

### D-2. Filesystem root in sola lettura + limiti di risorse + riduzione delle capability (consigliato, non attivo di default nel compose del repository) 【non verificato】

- **Perché**: un filesystem root in sola lettura impedisce manomissioni del binario a runtime; i limiti di risorse evitano che un singolo container metta in ginocchio l'host; la riduzione delle capability riduce la superficie di attacco del kernel.
- **Come fare** (esempio, da unire secondo necessità al servizio `broker` del compose):

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

- **Come verificare**: un tentativo di scrittura sul percorso root all'interno del container deve fallire (sola lettura); `docker inspect` mostra i limiti di risorse.
  **【non verificato】** (Docker non è stato eseguito sulla macchina locale); inoltre **con un filesystem root in sola lettura occorre assicurarsi che `data_dir` sia su un volume scrivibile**, altrimenti il kernel non può scrivere su disco.
- [ ] È stato valutato l'uso di filesystem root in sola lettura e limiti di risorse (attenzione: `data_dir` deve trovarsi su un volume scrivibile)

---

## E. Limiti noti (al momento realmente impossibili, da non dare per scontati)

Tutti i seguenti sono **carenze fattuali**: riconoscile esplicitamente nel progetto di sicurezza e non presumere che esistano:

1. **Le password sono memorizzate e copiate in chiaro**. Il campo `password` in `meta/state.json` è in chiaro (**【verificato】** misurato: si vede
   `"password":"drillpass"`); anche nel file di configurazione è in chiaro. **Non** esiste alcun hashing delle password (hashing e backend di autenticazione esterni sono lasciati ai plugin di autenticazione).
   → Conseguenza: **la directory dei dati e i file di backup equivalgono a credenziali sensibili** e devono essere protetti con permessi di file e cifratura.
2. **Non esiste un log di audit**. Le aggiunte/modifiche/eliminazioni sul piano di gestione producono log ordinari (ad esempio `管理面更新用户 actor=... user=...`),
   ma **non** esiste un flusso di audit separato e non manomettibile, né una registrazione a livello di conformità di "chi ha modificato cosa e quando".
3. **Nessuna autenticazione esterna LDAP / OAuth2 / JWT**. La v1 integra solo `PLAIN` / `AMQPLAIN`
   (`auth.Store.Mechanisms()` misurato restituisce solo questi due).
4. **SASL `EXTERNAL` non implementato**: anche con mTLS configurato, il livello di protocollo **utilizza comunque l'autenticazione con password PLAIN**
   (il passo "nessuna password con certificato client" non esiste). Il certificato è solo una verifica a livello di trasporto.
5. **`remote_access` non può essere impostato tramite API**: gli account creati tramite l'API/CLI di gestione consentono sempre il login remoto (vedi A-4),
   e non è possibile limitare un singolo account al solo accesso locale.
6. **Il piano di gestione non ha una whitelist delle origini separata / un rate limiting**: per ridurre la superficie esposta si può contare solo sull'indirizzo di bind, sul firewall e su TLS.
7. **Nessuna sandbox per i plugin**: i plugin in forma A sono nello stesso processo del kernel; i plugin esterni in forma B, pur avendo isolamento di processo,
   **passano dal proxy di connessione locale sul piano dati** e possono invocare semantiche del kernel (vincolate dalla validazione di vhost e permessi), ma **non** costituiscono una sandbox di sicurezza.
8. **I tag del piano di gestione sono solo tre livelli: `administrator`/`management`/`monitoring`**, senza RBAC più granulare per risorsa (per-resource).

---

## F. Checklist riepilogativa

- [ ] A-1 Account predefinito modificato/eliminato 【processo verificato】
- [ ] A-2 Privilegio minimo per gli account applicativi (regex), senza tag amministratore 【percorso 403 verificato】
- [ ] A-3 Il tag `administrator` concesso solo agli account operativi 【criterio dei permessi impliciti verificato】
- [ ] A-4 Valutata la limitazione dell'origine degli account privilegiati (attenzione: gli account creati via API abilitano di default l'accesso remoto)
- [ ] B-1 TLS abilitato sulle porte esposte; configurazione errata = avvio rifiutato 【verificato】
- [ ] B-2 `min_version` ≥ 1.2 【verificato】
- [ ] B-3 Le porte che richiedono mTLS configurate con `require_and_verify` + `ca_file`
- [ ] B-4 TLS abilitato sul piano di gestione
- [ ] C-1 Indirizzo di bind del piano di gestione ristretto 【indirizzo di ascolto verificato】
- [ ] C-2 Plugin di protocollo non utilizzati disabilitati
- [ ] D-1 Container eseguito come non root
- [ ] D-2 Valutati filesystem root in sola lettura / limiti di risorse / riduzione delle capability
- [ ] E Limiti noti (password in chiaro, assenza di audit, assenza di LDAP/OAuth2, SASL EXTERNAL non implementato) riconosciuti nel progetto di sicurezza
