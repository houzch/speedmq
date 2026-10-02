# Backup e ripristino di SwiftMQ

> Le conclusioni "misurate" di questo documento provengono tutte da una prova reale su **Windows + PowerShell 5.1** (con `data_dir` e porte temporanei).
> I comandi della prova e gli output principali sono riportati così come sono al §6. Le parti **【non verificate】** sono contrassegnate esplicitamente (backup/ripristino del cluster, backup dei volumi Docker, ecc.).

---

## 1. Cosa occorre salvare

Sotto `data_dir` **va salvato tutto integralmente**; le parti essenziali sono le seguenti (la struttura è descritta in `upgrade.md` §3):

| Percorso | Funzione | Conseguenze se perso |
| --- | --- | --- |
| `meta/state.json` | Snapshot dei metadati in modalità standalone: vhost / exchange / code / binding / utenti / permessi / policy | Si perdono completamente topologia e account |
| `meta/raft.log`, `meta/raft.state`, `meta/snapshot.json` | 【cluster】log Raft / voto di termine / snapshot + tabella dei membri | Si perdono l'identità del cluster e la coerenza dei metadati |
| `meta/users.seeded`, `meta/vhosts.seeded` | Marcatori di bootstrap | Se persi, gli users/vhosts della configurazione vengono **riseminati** (gli account/vhost eliminati tornano in vita) |
| `msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/0000NN.seg` + `index/0000NN.idx` | Dati e indici dei messaggi delle code classiche | Si perdono i messaggi persistenti |
| `quorum/<safe(vhost)>/<safe(queue)>/` | 【cluster】log/snapshot Raft delle code quorum | Si perdono i dati delle code quorum |
| File dei certificati (i PEM puntati da `cert_file`/`key_file`/`ca_file` nella configurazione) | Certificati TLS | Da salvare separatamente da `data_dir`; altrimenti dopo il riavvio il TLS non parte |

> Lo stato volatile (messaggi non confermati, consumatori, conteggi prefetch) **risiede solo in memoria** e non viene scritto su disco; il backup **non li include** né dovrebbe includerli.

---

## 2. Requisiti di coerenza: **occorre prima arrestare il processo**; il backup a caldo **non è sicuro**

### 2.1 Conclusione

- ✅ **Prassi sicura**: **arrestare il processo broker** (l'uscita ordinata esegue il flush finale), poi copiare `data_dir`.
- ❌ **Backup a caldo (copiare i file con il processo in esecuzione): non sicuro, nessuna garanzia.**

### 2.2 Perché il backup a caldo non è sicuro

Lo storage dei messaggi è composto da **due file** (il file a segmenti `*.seg` e il file di indice `index/*.idx`) e i due **non costituiscono un commit atomico**:

- Al ripristino si **fa fede sull'indice** per determinare "quali messaggi sono vivi", poi si legge dal file a segmenti in base a `(numero di segmento, offset, lunghezza)` presente nell'indice.
- Un backup a caldo può copiare uno stato intermedio in cui **l'indice fa già riferimento a record non ancora scritti completamente nel file a segmenti** (o viceversa):
  - l'indice fa riferimento a un record inesistente nel segmento → quel messaggio **fallisce la lettura e viene saltato** (equivale a perdere un messaggio persistente già confermato);
  - il segmento contiene un record ma l'indice non lo referenzia → quel messaggio **non viene ripristinato**.
- Sebbene il ripristino utilizzi il CRC32 per scartare i **record parzialmente scritti in coda**, questo copre solo "una scrittura corrotta in coda a un singolo file" e **non può riparare la desincronizzazione tra indice e segmenti**.

### 2.3 Su "quando la scrittura arriva su disco" (osservazioni misurate)

- Con `fsync: os` + `flush_interval_ms: 200` predefiniti: i messaggi vengono `write()` al sistema operativo dalla goroutine di flush in background entro **al massimo circa 200 ms** (senza fsync), e la publisher confirm ritorna dopo di ciò.
- Misurato: dopo aver pubblicato un messaggio persistente, controllando **immediatamente** la dimensione del file a segmenti si vedono già i dati (`t=0ms seg=832`); ovvero i "byte visibili all'OS" sono sostanzialmente sincroni con la confirm.
- **Nota bene**: questo indica solo che i dati "sono arrivati nel buffer dell'OS"; **uccidere violentemente il processo non li perde** (il buffer dell'OS non si perde con l'uccisione del processo), ma **un'interruzione di corrente li perde**.
  Per avere "fsync su disco già al ricevimento della confirm", imposta `storage.fsync` su `batch` / `always`. **【scenario di interruzione di corrente non misurato】**

---

## 3. Procedura di backup

### 3.1 Standalone (consigliato)

```powershell
# 1) Arresta il processo (in primo piano: Ctrl+C; in background: Stop-Process)
Stop-Process -Name swiftmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) Copia l'intero data_dir (con timestamp)
$data = "C:\swiftmq\data"
Copy-Item -Recurse -Force $data "C:\backup\swiftmq-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) (opzionale) Verifica che lo snapshot dei metadati nel backup sia analizzabile
Get-Content "C:\backup\swiftmq-...\meta\state.json" -Raw | ConvertFrom-Json | Select-Object -ExpandProperty VHosts
```

### 3.2 Cluster

- **Ogni nodo salva il proprio `data_dir`** (i metadati sono replicati a tutti tramite Raft, i dati dei messaggi risiedono sul nodo Owner e le repliche delle code quorum nelle rispettive directory Raft).
- Ordine di arresto: **arresta un solo nodo alla volta**; non arrestare più membri votanti contemporaneamente (vedi `upgrade.md` §7.2).
- Per ottenere uno **snapshot coerente dell'intero cluster** occorre arrestare tutti i nodi in sequenza e copiare ciascuno; in produzione è più comune "arresta/copia/avvia nodo per nodo".
- **【non verificato】** Non è stata eseguita una prova reale di backup/ripristino del cluster sulla macchina locale.

### 3.3 Docker (volume con nome)

```powershell
# Dopo aver arrestato il container, usa un container usa-e-getta per impacchettare e copiare il contenuto del volume
docker compose down
docker run --rm -v swiftmq-data:/data -v ${PWD}:/backup alpine `
  tar czf /backup/swiftmq-data.tar.gz -C /data .
```
> **【non verificato】** (Docker non è stato eseguito sulla macchina locale).

---

## 4. Procedura di ripristino

### 4.1 Standalone

```powershell
# 1) Verifica che il processo sia arrestato
Get-Process -Name swiftmqd -ErrorAction SilentlyContinue

# 2) Sposta (o elimina) l'attuale data_dir, per evitare che vecchi e nuovi file si mescolino
Move-Item "C:\swiftmq\data" "C:\swiftmq\data.broken"

# 3) Ripristina dal backup
Copy-Item -Recurse -Force "C:\backup\swiftmq-YYYYMMDD-HHMMSS" "C:\swiftmq\data"

# 4) Avvia
& "C:\swiftmq\swiftmqd.exe" -config "C:\swiftmq\configs\swiftmqd.json" -log-level info
```

Punti chiave:
- **Occorre prima spostare la vecchia directory**, non si può "sovrapporre i file di backup su una directory parzialmente rimasta";
- il `data_dir` ripristinato deve avere **lo stesso insieme di vhost/code** del momento del backup (i nomi delle directory sono codificati e utilizzabili tra macchine diverse);
- **non** approfittare del ripristino per modificare `vhosts`/`users` nel file di configurazione (hanno effetto solo al primo bootstrap; modificarli non serve, vedi `upgrade.md` §4.2).

### 4.2 Cluster

- Ripristino di un singolo nodo: ripristina il `data_dir` del nodo come al §4.1 e avvialo; esso si riunisce come membro già esistente e si allinea al log Raft.
- Ripristino dell'intero cluster: **ripristina e avvia prima i nodi della maggioranza** (≥ metà dei membri votanti), così il cluster può eleggere un leader; poi ripristina gli altri nodi.
- **【non verificato】** Il ripristino del cluster non è stato misurato.

---

## 5. Metodo di verifica dopo il ripristino

Verifica incrociata con l'API di gestione e con client reali (si consiglia di fare tutto):

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }

# a) Totale degli oggetti e totale dei messaggi (numero di code/exchange/binding/utenti; messages/ready/unacked)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# b) Verifica per coda di messages / messages_ready (confrontabile con i valori registrati prima del backup)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/queues' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# c) Verifica che vhost / utenti / policy siano tutti presenti
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/vhosts' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/users' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/policies' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# d) Cluster (in standalone restituisce enabled=false / mode=local / role=single)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/cluster' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

- **Controlla il log di avvio**: devono comparire `已从磁盘恢复队列消息 ... messages=N` e `队列已恢复持久化消息 ... messages=N`; N deve coincidere con il valore precedente al backup.
- **Avvisi nei log**: se in precedenza una coda ha avuto messaggi consumati/svuotati, al ripristino possono comparire
  `恢复消息失败，已跳过 ... seq=K err="读取记录头失败: EOF"` e `恢复时清理了无存活消息的段`.
  Questi sono residui nell'indice di **record già regolati (ack/purge)** e costituiscono un **rumore di log noto che non compromette la correttezza dei dati** (vedi §7).
- **Client reale**: recupera i messaggi dalla coda e verifica quantità/contenuto (vedi §6, passo (7)).

---

## 6. Prova misurata (comandi e output reali)

> Ambiente: `data_dir` in una directory temporanea, AMQP `127.0.0.1:5676`, piano di gestione `127.0.0.1:15677`, MQTT `127.0.0.1:1884`,
> account predefinito `guest/guest`. Log di avvio:
> ```
> level=INFO msg="SwiftMQ 启动中" version=1.0.0 ... data_dir=...\data ... fsync=os
> level=INFO msg=管理面已启动 component=management addr=127.0.0.1:15677
> ```

**(1) Creazione di una topologia durable + invio di 5 messaggi persistenti (client reale `amqp091-go`)**

```
OK  已声明 durable 交换机 persist.ex / 队列 persist.q，并绑定 key=k
OK  已发布 5 条持久消息（delivery-mode=2）并收到全部 confirm
```

**(2) Creazione di utente / vhost / autorizzazioni / policy (API di gestione)**

```
vhost PUT -> 201
user PUT -> 201
perm PUT -> 204
policy PUT -> 201
```

**(3) Stato prima del backup (API di gestione)**

```
=== /api/overview ===
"object_totals":{"connections":0,"channels":0,"queues":1,"consumers":0,"exchanges":13}
"queue_totals":{"messages":5,"messages_ready":5,"messages_unacknowledged":0}

=== /api/queues/%2F/persist.q ===
"durable":true,"effective_policy_definition":{"max-length":100},"messages":5,"messages_ready":5,
"name":"persist.q","policy":"drillpol","type":"classic","vhost":"/"

=== /api/vhosts ===  名称: ["/","drillvh"]
=== /api/users ===   名称: ["drilluser","guest"]
=== /api/policies === [{"apply-to":"queues","definition":{"max-length":100},"name":"drillpol","pattern":"persist.*","priority":1,"vhost":"/"}]
```

**(4) File su disco prima del backup**

```
data\meta\state.json                                             (1118 B)
data\meta\users.seeded                                           (37 B)
data\meta\vhosts.seeded                                          (37 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\000001.seg       (1152 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\index\000001.idx (1023 B)
```

**(5) Arresto del processo → backup → svuotamento → ripristino**

```
listeners still up: 0                       # 5676/1884/15677 均已关闭
=== 备份内容 ===   （与 (4) 完全一致，逐字节复制）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
data 目录是否存在(应为 False): False         # 已删除原 data_dir，模拟数据丢失
=== 恢复后内容 ===   （从备份复制回来，与 (4) 一致）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
```

**(6) Log di ripristino dopo il riavvio (righe chiave)**

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=persist.q messages=5 segments=2
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=persist.q queue=persist.q messages=5
level=INFO msg=元数据层已打开 component=broker mode=local ... queues=1 exchanges=1 bindings=1 users=2
```
> Compaiono inoltre alcune righe `level=WARN msg=恢复消息失败，已跳过 ... seq=1..13 err="读取记录头失败: EOF"`:
> questi sono residui nell'indice lasciati dai messaggi **precedentemente eliminati con purge** in questa prova (già regolati, dati del segmento già recuperati) e **non incidono sul ripristino dei 5 messaggi seguenti**.

**(7) Verifica dopo il ripristino: API di gestione + client reale**

```
=== 恢复后 /api/overview ===
"object_totals":{"queues":1,"exchanges":13,"consumers":0},"queue_totals":{"messages":5,"messages_ready":5,"messages_unacknowledged":0}

=== 恢复后 /api/queues/%2F/persist.q ===
"durable":true,"effective_policy_definition":{"max-length":100},"messages":5,"name":"persist.q","policy":"drillpol","type":"classic"

=== 恢复后 /api/vhosts (name) ===  / , drillvh
=== 恢复后 /api/users (name) ===   drilluser , guest
=== 恢复后 /api/policies ===       [{... "name":"drillpol","pattern":"persist.*" ...}]

=== 真实客户端断言 ===
OK  拓扑仍在：交换机 persist.ex / 队列 persist.q（声明时 message_count=5）
OK  取回 5 条持久消息: [persist-0 persist-1 persist-2 persist-3 persist-4]
```

**Conclusione**: la topologia durable (exchange + code + binding), i 5 messaggi persistenti, l'utente, il vhost, le autorizzazioni e le policy sono stati **tutti ripristinati**,
e il client reale riesce a recuperare tutti i messaggi esattamente come prima. **Prova superata.**

### 6.1 Confronto: il ripristino di code che non hanno subito consumo/purge è più "silenzioso"

Per stabilire se i WARN precedenti siano un fenomeno generale, è stata fatta una **controprova controllata**: creare una coda durable `clean.q`, inviare 3 messaggi persistenti, **senza consumare né svuotare**, arrestare il processo e riavviare:

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=clean.q messages=3 segments=1
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=clean.q queue=clean.q messages=3
```
**Nessun WARN**. Ciò mostra che i WARN compaiono solo nello scenario in cui "l'indice conserva ancora record già regolati" (vedi §7).

---

## 7. Problemi e limiti noti (registrati fedelmente)

1. **Rumore nel log di ripristino (osservato realmente)**: quando su una coda si sono storicamente verificati consumi/svuotamenti (messaggi già ack/purge),
   il suo indice conserva ancora riferimenti a record già recuperati, e al ripristino viene stampato **un WARN `恢复消息失败，已跳过` per ciascuno di essi**,
   creando/pulendo inoltre un `000000.seg` vuoto (log `恢复时清理了无存活消息的段`).
   **Non compromette la correttezza dei dati** (i messaggi non confermati ancora vivi vengono ripristinati correttamente), ma **sporca i log** e con code grandi/alto throughput può riempire lo schermo.
   Consiglio: fai riferimento a `已从磁盘恢复队列消息 ... messages=N` e ignora questi WARN relativi a record già regolati;
   se il volume dei log è inaccettabile, segnalalo ai manutentori del kernel (questo documento non modifica il codice).
2. **Il backup a caldo non è sicuro** (§2): non copiare direttamente `data_dir` mentre il processo è in esecuzione.
3. **`fsync: os` non garantisce l'assenza di perdite in caso di interruzione di corrente**: per "fsync su disco già alla confirm" usa `batch` / `always`.
4. **Password in chiaro**: in `meta/state.json` la password dell'utente è **in chiaro** (misurata: si vede `"password":"drillpass"`) —
   i file di backup devono quindi **essere trattati come dati sensibili** (controllo degli accessi, archiviazione cifrata). Vedi `security-baseline.md`.
5. **【non verificato】** Backup/ripristino del cluster, backup/ripristino dei volumi Docker, scenario di interruzione di corrente, scritture concorrenti durante il ripristino.
