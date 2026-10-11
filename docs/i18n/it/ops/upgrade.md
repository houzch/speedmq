# Piano di aggiornamento e migrazione di SpeedMQ

> Versione applicabile: `1.0.0` (`broker.Version`, vedi `speedmq_build_info` in `/metrics`).
> Tutte le conclusioni "misurate" di questo documento provengono da esecuzioni reali sulla macchina locale; tutto ciò che non è stato misurato è contrassegnato esplicitamente con **【non verificato】**.
> Ambiente locale: Windows + PowerShell 5.1, Go 1.27.1 windows/386, `data_dir` temporaneo + porte non predefinite.

---

## 1. Migrazione (passaggio da RabbitMQ a SpeedMQ)

Questo progetto è posizionato come **compatibilità a livello di protocollo AMQP 0-9-1**, pertanto la "migrazione" consiste essenzialmente nel **cambiare l'indirizzo di connessione**:

- Zero modifiche al codice applicativo, si cambiano solo `host/port/vhost` (documento di progettazione G3 «migrazione a costo zero»).
- La toolchain di gestione (`rabbitmqadmin`, UI di gestione, script di monitoraggio) deve solo puntare alla porta del piano di gestione; la forma delle interfacce è allineata a RabbitMQ (convenzioni come `amq.default`, `%2F`, `{error, reason}` riprese identiche).
- Le porte predefinite coincidono con RabbitMQ: AMQP `5672`, piano di gestione `15672`; MQTT `1883`, RPC tra nodi `25672`.

**Differenze semantiche da verificare prima della migrazione** (tutte volute in questo repository, in base al README / ai documenti di progettazione):

| Voce | Comportamento di SpeedMQ | Impatto sulla migrazione |
| --- | --- | --- |
| Code transient (non persistenti e non esclusive) | **Dichiarazione rifiutata** (541), `auto_delete` non fa eccezione | I client vecchi che dipendono da tali code falliranno; occorre passare a durable o exclusive |
| vhost predefinito `/` | **Non eliminabile** (400), RabbitMQ lo consente | Gli script di automazione che eliminano il vhost predefinito falliranno (unico vincolo di sicurezza attivo) |
| Dati delle code classiche | **Non replicati**, i dati risiedono solo sul nodo Owner | Per la ridondanza tra nodi usa le code quorum `x-queue-type=quorum` |
| Code quorum | Supportano l'aumento delle repliche, **non la riduzione** | Pianifica bene fin dall'inizio |
| Plugin | Nessun ecosistema di plugin Erlang; AMQP 1.0 / STOMP non implementati | Gli scenari che usano questi protocolli non sono ancora migrabili |

**Migrazione dei dati**: il formato di storage di SpeedMQ non è compatibile con RabbitMQ e **non viene fornito alcuno strumento di trasferimento dati online/offline**.
La modalità di migrazione è "creare un nuovo SpeedMQ vuoto → esecuzione in parallelo per verifica → switch del traffico graduale". **【non verificato】** Questo documento non contiene alcuna prova reale di trasferimento dati da RabbitMQ.

---

## 2. Principi generali dell'aggiornamento

1. **Backup prima** (vedi `backup-restore.md`) — rete di sicurezza in caso di aggiornamento fallito.
2. **Prima arresta il processo, poi sostituisci** (la directory dei dati ha un vincolo di scrittore singolo, vedi §4.2).
3. **Dopo l'aggiornamento è obbligatorio verificare**: il processo parte, `/api/overview` è leggibile, `/metrics` è raccoglibile, il numero di messaggi nelle code coincide con il valore precedente al backup.
4. L'aggiornamento del cluster è **rolling nodo per nodo**, un solo nodo alla volta (vedi §5).

---

## 3. Struttura della directory dei dati (base fattuale per aggiornamento/migrazione)

Struttura di `data_dir` **misurata** su un'istanza standalone locale:

```
data/
├── meta/
│   ├── state.json        # Snapshot dei metadati in modalità standalone (vhost/exchange/code/binding/utenti/permessi/policy)
│   ├── users.seeded      # Marcatore di bootstrap: gli users nella configurazione sono già stati seminati
│   ├── vhosts.seeded     # Marcatore di bootstrap: i vhosts nella configurazione sono già stati seminati
│   ├── raft.state        # 【modalità cluster】termine/voto Raft
│   ├── raft.log          # 【modalità cluster】log Raft
│   └── snapshot.json     # 【modalità cluster】snapshot Raft + tabella dei membri
├── msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/
│   ├── 000001.seg        # File a segmenti (corpo del messaggio + attributi), formato del record: <len u32><crc32 u32><payload>
│   └── index/000001.idx  # Indice della coda: seq-id → (numero di segmento, offset nel segmento, lunghezza, stato)
└── quorum/<safe(vhost)>/<safe(queue)>/   # 【cluster】code quorum: un gruppo Raft per coda (log/snapshot)
```

**Attenzione (due punti controintuitivi, entrambi basati su codice/misure)**:

- In modalità cluster i file di persistenza Raft **sono posti direttamente sotto `meta/`** (`raft.state` / `raft.log` / `snapshot.json`),
  **non esiste la sottodirectory `meta/raft/`**. Base: le costanti dei nomi di file in `internal/raft/log.go` + `internal/broker/cluster.go`
  con `Dir: filepath.Join(b.cfg.DataDir, "meta")`. **【verificato（2026-10-10, cluster a 3 nodi su Docker Desktop locale）】** —— Il contenuto del backup del volume di un nodo del cluster mostra direttamente `meta/raft.log`, `meta/raft.state`, `meta/*.seeded` (nessuna sottodirectory `meta/raft/` visibile).
- I nomi delle directory **non sono i nomi originali di vhost / code**, ma il risultato della codifica `store.SafeDirName`: prefisso `q_` e byte diversi da `[A-Za-z0-9._-]` codificati come `%XX`.
  Misurato: vhost `/` → directory `q_%2F`, coda `persist.q` → directory `q_persist.q`.
  Questa scelta serve a evitare path traversal e i nomi di dispositivo riservati di Windows (`con`/`nul` ecc.).

---

## 4. Compatibilità dei dati

### 4.1 I vecchi dati possono essere letti direttamente — sì

- **Formato dell'indice compatibile in avanti**: M8-1 ha aggiunto il campo "numero di segmento" (25 byte) nel record di indice; **il vecchio formato (21 byte, senza numero di segmento) resta leggibile così com'è**,
  e in lettura equivale a "un solo segmento (seg=1)", quindi **l'aggiornamento non richiede script di migrazione**.
  Base: le costanti `indexEntrySize` / `legacyIndexEntrySize` e la logica `recover()` in `internal/store/store.go`; README M8-1.
- **Semantica di crash invariata**: ogni record ha prefisso di lunghezza + CRC32; al ripristino i **record parzialmente scritti/corrotti in coda vengono scartati** e il file troncato.
  Misurato (vedi `backup-restore.md` §6): dopo l'arresto e il riavvio del processo, i 5 messaggi persistenti della coda durable sono **tutti ripristinati**,
  e nel log compare `已从磁盘恢复队列消息 ... messages=5`.

**【verificato（2026-10-10, macchina locale）】** La lettura tra versioni maggiori è stata misurata: i dati di una coda durable scritti con il tag `v1.1.0` vengono ripristinati tali e quali sotto `1.1.05` (comandi e output, vedi §5).

### 4.2 Il criterio di `vhosts` / `users` nella configurazione (la trappola più frequente in fase di aggiornamento)

- Entrambi **hanno effetto solo al primo bootstrap**: al primo avvio i vhosts/users della configurazione vengono scritti nei metadati e viene creato il file marcatore
  `meta/vhosts.seeded` / `meta/users.seeded`; **da quel momento si fa fede sui metadati**.
- Pertanto **in fase di aggiornamento/cambio di configurazione, non aspettarti di aggiungere o rimuovere account o vhost modificando il file di configurazione** — le modifiche non avranno effetto;
  usa l'API di gestione o `speedmqctl`.
- Viceversa, l'aggiornamento **non** sovrascrive con la configurazione gli account esistenti: una password modificata a runtime non viene riportata dal riavvio al vecchio valore della configurazione,
  e un account eliminato a runtime non torna in vita. Base: la logica dei marcatori di seeding in `cluster.go`; README M8-4 / M8-7.

**【verificato（2026-10-10, macchina locale）】** Le modifiche a runtime non vengono effettivamente sovrascritte dalla configurazione: prima dell'aggiornamento la password di `guest` è stata cambiata in `rotated` tramite l'API di gestione; dopo il riavvio con `1.1.05` + lo stesso `data_dir`, `guest/guest` (la vecchia password della configurazione) restituisce **401** e `guest/rotated` restituisce **200**.

### 4.3 Rotazione dei segmenti e recupero dello spazio su disco

- I messaggi sono suddivisi in segmenti in base alla dimensione (predefinito 8 MiB); **quando tutti i messaggi di un segmento sono stati ack e il segmento è stato chiuso, l'intero segmento viene eliminato**, e l'indice viene compresso e riscritto di conseguenza.
- L'aggiornamento non modifica questo comportamento; i file a segmento singolo lasciati dalla vecchia istanza continuano a funzionare normalmente con la nuova logica di rotazione dei segmenti.

---

## 5. Aggiornamento del binario (bare metal)

> **【verificato（2026-10-10, macchina locale）】** Prova reale tra versioni maggiori: il vecchio binario costruito con il tag `v1.1.0` ha scritto exchange/coda/binding durable + 5 messaggi persistenti + vhost/account/permessi/policy e ha cambiato la password a runtime; dopo l'arresto del processo è stato riavviato con il binario attuale `1.1.05` + **lo stesso `data_dir` + la stessa porta** —— il processo parte normalmente, gli oggetti e il numero di messaggi di `/api/queues` coincidono, un vero client AMQP recupera tutti i 5 messaggi persistenti e `/metrics` riporta `speedmq_build_info{version="1.1.05"}`. I passi seguenti restano una procedura generica applicabile a qualsiasi versione adiacente; **downgrade** vedi §8.

### 5.1 Passi

```powershell
$base = "C:\speedmq"
$data = "$base\data"

# 1) Arresta il processo (l'uscita ordinata esegue il flush finale; vedi §4 «coerenza»)
#    Se è eseguito in primo piano: Ctrl+C; se come servizio: Stop-Service / Stop-Process
Stop-Process -Name speedmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) Esegui il backup della directory dei dati (assolutamente dopo l'arresto del processo)
Copy-Item -Recurse -Force $data "$base\backup-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) Sostituisci il binario (metti il nuovo speedmqd.exe / speedmqctl.exe nel percorso originale)
#    Copy-Item .\new\speedmqd.exe $base\speedmqd.exe -Force

# 4) Avvia
& "$base\speedmqd.exe" -config "$base\configs\speedmqd.json" -log-level info

# 5) Verifica: processo attivo + API di gestione leggibile
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

### 5.2 Checklist di verifica dopo l'aggiornamento

- Nel log di avvio compaiono `SpeedMQ 启动中 ... version=<新版本>` e `管理面已启动`;
- `object_totals` / `queue_totals` di `/api/overview` coincidono con quelli precedenti al backup (confronta `backup-restore.md` §5);
- per ogni coda durable in `/api/queues`, `messages` / `messages_ready` coincidono con i valori precedenti al backup;
- `/metrics` è raccoglibile e presenta `speedmq_plugin_up{name="amqp091"} 1`, `{name="mqtt"} 1`.

---

## 6. Aggiornamento dell'immagine (container)

L'immagine è di circa 13 MB (binario con link statico + alpine), **eseguita come non root (uid 10001)**, con la directory dei dati montata su `/var/lib/speedmq`.

```powershell
# 1) Scarica/costruisci la nuova immagine (usa il nuovo numero di versione come tag, per evitare confusione old/new)
docker build -t speedmq:1.0.0 .

# 2) Arresta il vecchio container (compose conserva il volume con nome speedmq-data)
docker compose down

# 3) Avvia la nuova versione (modifica image nel file compose impostando il nuovo tag)
docker compose up -d

# 4) Stato e log
docker compose ps
docker compose logs -f --tail 100
```

> **Attività usa-e-getta all'interno del container** (ad esempio eseguire `speedmqctl` nel container): in un ambiente non interattivo il `run` di `docker compose ...` deve includere `-T`,
> altrimenti fallisce nel richiedere il TTY:
> ```powershell
> docker compose run -T --rm broker speedmqctl -user guest -pass guest status
> ```

La persistenza dei dati si basa sul **volume con nome** `speedmq-data` di compose; ricreando il container i dati non si perdono (a partire da M4 scrivono realmente su disco).
Se prima dell'aggiornamento serve eseguire il backup del contenuto del volume, equivale a salvare `/var/lib/speedmq` (vedi `backup-restore.md` §3.2).

> **【verificato（2026-10-10, Docker Desktop locale）】** È stata eseguita una prova reale di aggiornamento dell'immagine: sono state costruite localmente la vecchia immagine `speedmq:1.1.0-old` (git tag `v1.1.0`) e la nuova `speedmq:1.1.05-new` (codice sorgente attuale); con un compose a nodo singolo è stata avviata la vecchia immagine → creazione della coda durable `persist.q` e pubblicazione di 5 messaggi persistenti (`delivery_mode=2`) → `docker compose down` (**senza `-v`**, mantenendo il volume con nome `speedmq-data`) → dopo aver cambiato il tag image nel compose sulla nuova immagine, `docker compose up -d`.
> Risultato: il nuovo container parte normalmente, nel log di avvio compare `已从磁盘恢复队列消息 ... queue=persist.q messages=5`, `/metrics` riporta `speedmq_build_info{version="1.1.05"}`, `queue_totals.messages=5` di `/api/overview` e `messages=5` della coda `persist.q` coincidono prima e dopo l'aggiornamento, e un vero client AMQP (`rabbitmq/amqp091-go`) recupera uno a uno tutti i 5 messaggi persistenti (`deliveryMode=2`).
> Nota: `v1.1.0` è l'immagine del marchio `speedmq` precedente al cambio di nome (binario `speedmqd`, metrica `speedmq_build_info`, directory dati predefinita `/var/lib/speedmq`), con denominazione diversa dalla versione attuale; nella prova si è fatto riferimento alla configurazione `data_dir=/var/lib/speedmq` e si è preimpostato il proprietario del volume con nome (nella vecchia immagine quel percorso scrivibile non esiste). Ciò non inficia la conclusione "ricreare il container cambiando il tag non perde i dati del volume con nome".

---

## 7. Rilascio graduale e rollback

### 7.1 Standalone

- **Rilascio graduale**: SpeedMQ in modalità standalone non ha una capacità integrata di "due versioni nello stesso processo". Un rilascio graduale praticabile è lo **shadowing su percorso laterale**:
  l'istanza della nuova versione si aggancia dapprima alla stessa sorgente di traffico upstream con un **consumo in sola lettura/coda shadow**, si osserva, e solo dopo aver confermato che tutto è corretto si cambia il lato di scrittura.
- **Rollback**:
  1. arresta il processo della nuova versione;
  2. ripristina il vecchio binario;
  3. se la nuova versione ha già scritto dati, **occorre ripristinare `data_dir` con il backup eseguito prima dell'aggiornamento** (vedi sotto).
  **Non** esiste la garanzia che "ciò che ha scritto la nuova versione venga letto direttamente dalla vecchia" — per il downgrade tra versioni vedi §8.

### 7.2 Cluster (aggiornamento rolling)

La piattaforma non offre un "aggiornamento rolling con un solo comando"; occorre operare manualmente nodo per nodo nell'ordine seguente:

1. **Aggiorna un solo nodo alla volta**: arresta il nodo → esegui il backup del suo `data_dir` → sostituisci il binario → avvia → attendi che si riunisca e si allinei
   (`speedmqctl cluster_status` / `GET /api/cluster` per vedere `role`, `commit_index`/`last_applied`).
2. **Ordine consigliato**: prima aggiorna i **learner / membri non votanti** (nessun impatto sulla maggioranza), poi i **follower**, infine il **leader**
   (l'aggiornamento del leader innesca un'elezione del leader, durante la quale c'è una breve indisponibilità in scrittura).
3. **Impatto dell'arresto sulla maggioranza** (cruciale):
   - Cluster a 3 nodi: **è possibile arrestare al massimo 1** membro votante alla volta; arrestandone 2 si perde la maggioranza e con `pause_minority` **l'intero cluster sospende il servizio**.
   - Cluster a 2 nodi: arrestandone 1 si perde la maggioranza e **non è possibile l'aggiornamento rolling** (si consigliano almeno 3 nodi).
   - Pertanto durante l'aggiornamento rolling è **severamente vietato arrestare più membri votanti contemporaneamente**.
4. **Non eseguire insieme cambio di membri e aggiornamento**: il cambio di membri **non ha joint consensus** e consente un solo cambiamento di configurazione non ancora confermato alla volta;
   durante l'aggiornamento evita di eseguire contemporaneamente `add_member` / `remove_member`.
5. Al termine dell'aggiornamento, verifica che `object_totals` di `GET /api/cluster` coincida con il valore precedente all'aggiornamento.

> **【verificato（2026-10-10, cluster a 3 nodi su Docker Desktop locale）】** È stata eseguita una prova reale di aggiornamento rolling **tra versioni** di un cluster reale: la base era `speedmq:1.1.0-old` costruita localmente (git tag `v1.1.0`), aggiornando nodo per nodo (n1 → n2 → n3, uno alla volta: arresta, esegui il backup del suo volume con nome, cambia l'immagine e avvia) fino all'attuale `1.1.05` (costruita localmente `speedmq:1.1.05-new`, compose cambiava l'immagine con un override per nodo). Per tutta la durata, il cluster ha sempre mantenuto un leader e la maggioranza —— all'arresto del leader corrente è stato eletto un nuovo leader in circa 0,6 s, `pause_minority` non ha attivato la sospensione e non ci sono state interruzioni esterne durante l'arresto; dopo l'aggiornamento gli `object_totals` di `GET /api/cluster` coincidono con i precedenti (`queues:1`), il conteggio dei 25 messaggi persistenti della coda quorum a 3 repliche `qroll` resta invariato e sono rileggibili uno a uno, e dopo la pubblicazione di 5 nuovi messaggi tutti i 30 sono recuperabili.
> L'ordine sopra esposto resta valido in base ai vincoli generali di Raft; questa prova copre lo scenario a 3 nodi. **【verificato（2026-10-10, Docker Desktop locale）2 nodi: arrestarne 1 fa già perdere la maggioranza】** È stato inoltre creato un cluster minimo a 2 nodi (n1/n2, i cui `peers` contengono solo questi due, `partition_policy=pause_minority`) ed è stato misurato: con entrambi i nodi online, `GET /api/cluster` è `has_quorum=true / paused=false` (n1 leader, n2 follower); dopo `docker stop` di n2, n1 passa in circa 2 s a `has_quorum=false / paused=true` —— ossia arrestare 1 membro votante fa già perdere la maggioranza e `pause_minority` sospende il servizio, **confermando che 2 nodi non hanno capacità di aggiornamento rolling**; dopo `docker start` per ripristinare n2, n1 torna in circa 2 s a `has_quorum=true / paused=false` (il servizio si ripristina automaticamente).

> **【verificato（2026-10-11, cluster a 3 nodi su Docker Desktop locale）】L'aggiornamento rolling in condizioni di partizione di rete è stato misurato** (tra versioni `v1.1.0` → `1.1.05`; la partizione ha usato una topologia a doppia rete: l'RPC Raft tra nodi passa solo su `cluster-net` con IP statici, il traffico client/interfaccia di gestione passa su `client-net`, e `docker network disconnect/connect` attiva/disattiva la partizione):
> - **Follower partizionato**: il lato di maggioranza ha mantenuto il quorum per tutta la partizione, ha pubblicato/consumato normalmente e ha completato "arresta → cambia immagine → avvia → riallinea" di un nodo di maggioranza mentre la partizione persisteva; il cluster a versioni miste (1.1.0 + 1.1.05) è rimasto operativo. Al ripristino della partizione, il nodo isolato (vecchia versione) si è riunito e riallineato; la riconciliazione dei messaggi per insieme di ID univoci ha dato 35/35 senza perdite.
> - **Leader partizionato**: la maggioranza ha eletto un nuovo leader in circa **630 ms**; il vecchio leader isolato ha mantenuto `role=leader` ma con `has_quorum=false / paused=true` (`pause_minority` attivo, senza elevare proattivamente il proprio term); l'aggiornamento di un altro nodo del lato di maggioranza durante la partizione è riuscito; dopo il ripristino il vecchio leader si è riunito senza split-brain né divergenza di dati.
> - **Comportamento atteso senza PreVote (registrato fedelmente)**: il follower isolato ha avviato subito campagne ripetute, con il term in aumento di circa +2 ogni 2 s (i due gruppi Raft, metadati e coda, che si elevano alternandosi); all'istante del ripristino ha **conquistato la leadership** con il suo term più alto (eletto entro 1,4 s nella prova) — disturbo da cambio di leader, ma nessun problema di correttezza.
> - **Due insidie operative**: ① se `peers` usa nomi host e i container sono collegati anche a una seconda rete raggiungibile, il traffico Raft devia e causa una "falsa partizione" (il nodo isolato mostrava ancora `has_quorum=true` dopo oltre un minuto) — l'RPC tra nodi deve passare su una sola rete (`peers` che punta direttamente agli IP statici di quella rete); ② al ripristino, `docker network connect` deve portare `--ip <IP originale>`, altrimenti il nodo ottiene un nuovo IP mentre la maggioranza continua a chiamare il vecchio indirizzo, e il nodo isolato — con un log arretrato — avvia campagne ripetute con un term gonfiato (nella prova ha trascinato il term del cluster da 320 a 488 e il commit dei metadati da 13 a 97), e può apparire uno stato transitorio in cui il leader mostra `quorum=true` ma per breve tempo `paused=true`.
> - Durante la partizione, l'API di gestione del lato di minoranza restava leggibile (utile per l'osservazione), mentre la pubblicazione AMQP veniva rifiutata (`pause_minority` attivo).

---

## 8. Parti non supportate / non verificate (elencate esplicitamente)

- **Downgrade tra versioni maggiori: non supportato, non verificato**. Se la nuova versione ha già scritto dati con un nuovo formato/nuova semantica, **non** esiste la garanzia di "tornare al vecchio binario e leggerli così come sono";
  il rollback può contare solo sul backup eseguito prima dell'aggiornamento.
- **Formato di configurazione invariato**: resta JSON + variabili d'ambiente `SPEEDMQ_*`. **La configurazione YAML non è ancora supportata** (richiederebbe l'introduzione di una dipendenza di parsing; M8-17 da valutare);
  l'aggiornamento non introduce YAML.
- **Aggiornamento a caldo di plugin/protocolli online**: i plugin vengono compilati insieme al kernel (forma A) oppure avviati con `spawn` secondo la configurazione (forma B),
  aggiornare il kernel = riavviare il processo; **non** esiste un meccanismo di sostituzione a caldo sul posto del binario.
- **Migrazione sul posto del motore di storage**: la rotazione dei segmenti/compressione dell'indice è un comportamento in background a runtime; **non** esiste un comando separato di "migrazione/compressione dei dati".
- **Aggiornamento del cluster con disco pieno**: **non verificato**. (L'aggiornamento rolling in condizioni di partizione di rete è stato misurato il 2026-10-11; vedi la nota in fondo al §7.)
- Questo documento **non contiene** alcuna verifica di trasferimento dati tra SpeedMQ e altri broker (RabbitMQ).
