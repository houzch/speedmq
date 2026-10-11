# Sauvegarde et restauration de SpeedMQ

> Les conclusions « mesurées » de ce document proviennent toutes d'un exercice réel réalisé sur **Windows + PowerShell 5.1** (avec un `data_dir` temporaire et des ports temporaires).
> Les commandes de l'exercice et les sorties clés sont reproduites telles quelles au §6. Les parties **【non vérifié】** sont explicitement signalées (par ex. une vraie coupure d'alimentation physique).

> **【vérifié (2026-10-11, cluster à 3 nœuds sur Docker Desktop local)】** L'arrêt forcé de processus (`docker kill`) et les écritures concurrentes pendant la restauration ont été mesurés ;
> les enregistrements sont au §6.2 / §6.3 ; la formulation sur la « coupure d'alimentation » du §2.3 et la liste des états de vérification du §7 ont été mises à jour en conséquence.

---

## 1. Ce qu'il faut sauvegarder

Sous `data_dir`, **la sauvegarde doit être complète** ; les éléments essentiels sont les suivants (agencement au §3 de `upgrade.md`) :

| Chemin | Rôle | Conséquence en cas de perte |
| --- | --- | --- |
| `meta/state.json` | Instantané des métadonnées en mode autonome : vhost / exchanges / files / bindings / utilisateurs / permissions / politiques | Perte totale de la topologie et des comptes |
| `meta/raft.log`, `meta/raft.state`, `meta/snapshot.json` | 【cluster】Journal Raft / vote de mandat / instantané + table des membres | Perte de l'identité du cluster et de la cohérence des métadonnées |
| `meta/users.seeded`, `meta/vhosts.seeded` | Marqueurs d'amorçage | En cas de perte, les users/vhosts de la configuration sont **ré-amorcés** (les comptes/vhosts supprimés réapparaissent) |
| `msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/0000NN.seg` + `index/0000NN.idx` | Données et index des messages des files classiques | Perte des messages persistants |
| `quorum/<safe(vhost)>/<safe(queue)>/` | 【cluster】Journal/instantané Raft des files quorum | Perte des données des files quorum |
| Fichiers de certificat (les PEM pointés par `cert_file`/`key_file`/`ca_file` dans la configuration) | Certificats TLS | À sauvegarder séparément de `data_dir`, sinon TLS ne démarre pas après redémarrage |

> L'état volatil (messages non acquittés, consommateurs, compteurs prefetch) réside **uniquement en mémoire** et n'est pas écrit sur disque ; la sauvegarde **ne le contient pas** et ne doit pas le contenir.

---

## 2. Exigence de cohérence : **l'arrêt du processus est indispensable** ; la sauvegarde à chaud est **non sûre**

### 2.1 Conclusion

- ✅ **Méthode sûre** : **arrêter le processus broker** (un arrêt gracieux effectue un vidage final sur disque), puis copier `data_dir`.
- ❌ **Sauvegarde à chaud (copier les fichiers pendant que le processus tourne) : non sûre, aucune garantie.**

### 2.2 Pourquoi la sauvegarde à chaud n'est pas sûre

Le stockage des messages repose sur **deux fichiers** (le fichier de segment `*.seg` et le fichier d'index `index/*.idx`), et les deux **ne forment pas une validation atomique** :

- À la restauration, c'est **l'index qui fait foi** pour déterminer « quels messages sont vivants », puis on lit dans le fichier de segment selon le `(numéro de segment, offset, longueur)` présent dans l'index.
- Une sauvegarde à chaud peut capturer un état intermédiaire où **l'index référence déjà un enregistrement que le fichier de segment n'a pas encore entièrement écrit** (ou l'inverse) :
  - l'index référence un enregistrement absent du segment → ce message **échoue à la lecture et est ignoré** (soit la perte d'un message persistant déjà acquitté) ;
  - le segment contient un enregistrement que l'index ne référence pas → ce message **n'est pas restauré**.
- La restauration utilise certes le CRC32 pour écarter les **enregistrements partiellement écrits en fin de fichier**, mais cela ne couvre qu'une « écriture corrompue en fin d'un fichier unique » et **ne peut pas corriger la désynchronisation entre l'index et le segment**.

### 2.3 À propos du « moment où l'écriture atteint le disque » (observations mesurées)

- Par défaut `fsync: os` + `flush_interval_ms: 200` : les messages sont `write()` vers le système d'exploitation par une goroutine de vidage en arrière-plan en **au plus environ 200 ms**
  (sans fsync) ; le publisher confirm est renvoyé après cela.
- Mesuré : après publication d'un message persistant, la taille du fichier de segment est **immédiatement** consultable et les données sont déjà visibles (`t=0ms seg=832`) ; autrement dit, « les octets visibles par l'OS » sont pratiquement synchrones avec le confirm.
- **Attention** : cela signifie seulement que « les données ont atteint le tampon de l'OS » ; **tuer le processus brutalement ne les perd pas** (l'arrêt forcé du processus ne perd pas le tampon de l'OS), mais **une coupure d'alimentation les perd**.
  Pour garantir « au moment de recevoir le confirm, les données sont déjà fsync sur disque », passez `storage.fsync` à `batch` / `always`.
  **【vérifié (2026-10-11, cluster à 3 nœuds sur Docker Desktop local)】** L'arrêt forcé d'un nœud par `docker kill` (SIGKILL) en cours d'écriture a été mesuré aux deux réglages sans perte de messages confirmés :
  `fsync: os` en tuant un follower (50 000 messages tous confirmés, réconciliation missing=0), `fsync: always` en tuant le leader de la file quorum (200 000 messages tous confirmés,
  missing=0 ; 1 355 messages supplémentaires écrits sur disque mais non confirmés ont été renvoyés, ce que la sémantique at-least-once autorise ; la déduplication par ID de message côté consommateur rend le traitement idempotent).
  Bonus : au réglage par défaut, un `wsl --shutdown` a arrêté brutalement toute la VM Docker en cours de publication (ce qui s'apparente à une coupure d'alimentation de la machine entière avec perte du tampon de l'OS) ; après la restauration, les 69 669 messages confirmés
  ont été réconciliés un par un par ID avec missing=0. **Toujours non vérifié** : la vraie coupure d'alimentation physique (perte d'alimentation de l'hôte, couche de cache disque), impossible à simuler sur cette machine.

---

## 3. Étapes de sauvegarde

### 3.1 Mode autonome (recommandé)

```powershell
# 1) arrêter le processus (au premier plan : Ctrl+C ; en arrière-plan : Stop-Process)
Stop-Process -Name speedmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) copier tout le data_dir (avec horodatage)
$data = "C:\speedmq\data"
Copy-Item -Recurse -Force $data "C:\backup\speedmq-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) (facultatif) vérifier que l'instantané des métadonnées de la sauvegarde est analysable
Get-Content "C:\backup\speedmq-...\meta\state.json" -Raw | ConvertFrom-Json | Select-Object -ExpandProperty VHosts
```

### 3.2 Cluster

- **Chaque nœud sauvegarde son propre `data_dir`** (les métadonnées sont répliquées à tous via Raft, les données de messages résident sur le nœud Owner, les répliques des files quorum dans leur propre répertoire Raft).
- Ordre d'arrêt : **un seul nœud à la fois** ; n'arrêtez pas plusieurs membres votants simultanément (voir §7.2 de `upgrade.md`).
- Pour obtenir un **instantané cohérent de tout le cluster**, il faut arrêter tous les nœuds dans l'ordre puis copier chacun ; en production, il est plus courant de procéder « nœud par nœud : arrêt / copie / redémarrage ».
- **【vérifié (2026-10-10, cluster à 3 nœuds sur Docker Desktop local)】** Sur un cluster compose à 3 nœuds, exécution « arrêt / copie / redémarrage » nœud par nœud : le volume nommé de chaque nœud a été empaqueté avec un conteneur alpine jetable
  (`docker run --rm -v <volume>:/data -v <répertoire hôte>:/backup alpine tar czf ...`), l'archive contenait `meta/raft.log`, `meta/raft.state`, `meta/*.seeded`, `quorum/...` ;
  après la restauration, ce nœud a pu rejoindre de nouveau le cluster en tant que membre existant et rattraper son retard (voir §4.2, même preuve qu'au §7.2 de `upgrade.md`).

### 3.3 Docker (volume nommé)

```powershell
# après l'arrêt du conteneur, empaqueter et extraire le contenu du volume avec un conteneur jetable
docker compose down
docker run --rm -v speedmq-data:/data -v ${PWD}:/backup alpine `
  tar czf /backup/speedmq-data.tar.gz -C /data .
```
> **【vérifié (2026-10-10, Docker Desktop local)】** Cette commande a été réellement exécutée sur les volumes nommés d'un cluster à 3 nœuds (l'archive contenait `meta/`, `quorum/`),
> et a servi à réaliser un exercice de restauration du volume d'un seul nœud (voir §4.2).

---

## 4. Étapes de restauration

### 4.1 Mode autonome

```powershell
# 1) vérifier que le processus est arrêté
Get-Process -Name speedmqd -ErrorAction SilentlyContinue

# 2) déplacer (ou supprimer) le data_dir actuel pour éviter de mélanger les anciens et nouveaux fichiers
Move-Item "C:\speedmq\data" "C:\speedmq\data.broken"

# 3) restaurer à partir de la sauvegarde
Copy-Item -Recurse -Force "C:\backup\speedmq-YYYYMMDD-HHMMSS" "C:\speedmq\data"

# 4) démarrer
& "C:\speedmq\speedmqd.exe" -config "C:\speedmq\configs\speedmqd.json" -log-level info
```

Points clés :
- **Il faut d'abord déplacer l'ancien répertoire** ; il est interdit de « superposer les fichiers de sauvegarde sur un répertoire partiellement résiduel » ;
- Le `data_dir` restauré doit correspondre au **même ensemble de vhost/files** qu'au moment de la sauvegarde (les noms de répertoires sont encodés, donc utilisables d'une machine à l'autre) ;
- **Ne profitez pas** de la restauration pour modifier `vhosts`/`users` dans le fichier de configuration (ils ne prennent effet qu'au premier amorçage, les modifier est inutile, voir §4.2 de `upgrade.md`).

### 4.2 Cluster

- Restauration d'un seul nœud : restaurez le `data_dir` de ce nœud en suivant §4.1 puis démarrez ; il rejoint le cluster en tant que membre existant et rattrape le journal Raft.
- Restauration du cluster entier : **restaurez et démarrez d'abord les nœuds majoritaires** (≥ la moitié des membres votants) pour que le cluster puisse élire un leader ; restaurez ensuite les autres nœuds.
- **【vérifié (2026-10-10, cluster à 3 nœuds sur Docker Desktop local)】** Mesure de « arrêt de n3 → vidage de son volume → écrasement par la sauvegarde du §3.3 → redémarrage » :
  n3 a rejoint de nouveau en tant que follower et rattrapé le leader (`commit_index == last_applied`) ; le nombre de messages des files quorum et la table des membres sont restés inchangés.

---

## 5. Méthode de vérification après restauration

Recoupez via l'API d'administration et un vrai client (il est recommandé de tout faire) :

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }

# a) totaux d'objets et de messages (nombre de files/exchanges/bindings/utilisateurs ; messages/ready/unacked)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# b) vérifier queue par queue messages / messages_ready (à comparer aux relevés d'avant la sauvegarde)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/queues' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# c) vérifier la présence des vhosts / utilisateurs / politiques
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/vhosts' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/users' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/policies' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content

# d) cluster (en mode autonome, renvoie enabled=false / mode=local / role=single)
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/cluster' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

- **Consultez les logs de démarrage** : doivent apparaître `已从磁盘恢复队列消息 ... messages=N` et `队列已恢复持久化消息 ... messages=N` ; N doit correspondre à la valeur d'avant la sauvegarde.
- **Avertissements dans les logs** : si une file a déjà vu des messages consommés/vidés auparavant, la restauration peut produire
  `恢复消息失败，已跳过 ... seq=K err="读取记录头失败: EOF"` et `恢复时清理了无存活消息的段`.
  Il s'agit de résidus d'index d'**enregistrements déjà réglés (ack/purge)**, un **bruit de journal connu qui n'affecte pas la correction des données** (voir §7).
- **Vrai client** : récupérez les messages de la file et vérifiez le nombre/contenu (voir étape (7) du §6).

---

## 6. Exercice mesuré (commandes et sorties réelles)

> Environnement : `data_dir` dans un répertoire temporaire, AMQP `127.0.0.1:5676`, interface d'administration `127.0.0.1:15677`, MQTT `127.0.0.1:1884`,
> compte par défaut `guest/guest`. Logs de démarrage :
> ```
> level=INFO msg="SpeedMQ 启动中" version=1.0.0 ... data_dir=...\data ... fsync=os
> level=INFO msg=管理面已启动 component=management addr=127.0.0.1:15677
> ```

**(1) Créer une topologie durable + publier 5 messages persistants (vrai client `amqp091-go`)**

```
OK  已声明 durable 交换机 persist.ex / 队列 persist.q，并绑定 key=k
OK  已发布 5 条持久消息（delivery-mode=2）并收到全部 confirm
```

**(2) Créer utilisateur / vhost / permission / politique (API d'administration)**

```
vhost PUT -> 201
user PUT -> 201
perm PUT -> 204
policy PUT -> 201
```

**(3) État avant sauvegarde (API d'administration)**

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

**(4) Fichiers disque avant sauvegarde**

```
data\meta\state.json                                             (1118 B)
data\meta\users.seeded                                           (37 B)
data\meta\vhosts.seeded                                          (37 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\000001.seg       (1152 B)
data\msg_stores\vhosts\q_%2F\queues\q_persist.q\index\000001.idx (1023 B)
```

**(5) Arrêt du processus → sauvegarde → vidage → restauration**

```
listeners still up: 0                       # les ports 5676/1884/15677 sont tous fermés
=== 备份内容 ===   （与 (4) 完全一致，逐字节复制）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
data 目录是否存在(应为 False): False         # le data_dir d'origine a été supprimé pour simuler une perte de données
=== 恢复后内容 ===   （从备份复制回来，与 (4) 一致）
data\meta\state.json  (1118 B) ... 000001.seg (1152 B) ... index\000001.idx (1023 B)
```

**(6) Logs de restauration après redémarrage (lignes clés)**

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=persist.q messages=5 segments=2
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=persist.q queue=persist.q messages=5
level=INFO msg=元数据层已打开 component=broker mode=local ... queues=1 exchanges=1 bindings=1 users=2
```
> On voit aussi apparaître plusieurs lignes `level=WARN msg=恢复消息失败，已跳过 ... seq=1..13 err="读取记录头失败: EOF"` :
> ce sont des résidus d'index laissés par des messages **précédemment purgés** lors de cet exercice (déjà réglés, données de segment récupérées), qui **n'affectent pas la restauration des 5 messages ci-dessous**.

**(7) Assertions après restauration : API d'administration + vrai client**

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

**Conclusion** : la topologie durable (exchange + file + binding), les 5 messages persistants, les utilisateurs, les vhosts, les permissions et les politiques sont **tous restaurés**,
et le vrai client peut récupérer l'intégralité des messages à l'identique. **Exercice réussi.**

### 6.1 Comparaison : la restauration d'une file n'ayant subi ni consommation ni purge est plus « silencieuse »

Pour déterminer si les WARN ci-dessus sont un phénomène général, un **essai contrôlé** a été réalisé en parallèle : créer une file durable `clean.q`, publier 3 messages persistants,
**sans consommation ni purge**, arrêter le processus puis redémarrer :

```
level=INFO msg=已从磁盘恢复队列消息 component=broker vhost=/ queue=clean.q messages=3 segments=1
level=INFO msg=队列已恢复持久化消息 component=broker vhost=/ queue=clean.q queue=clean.q messages=3
```
**Aucun WARN**. Cela montre que les WARN n'apparaissent que dans le cas où « l'index conserve encore des enregistrements réglés » (voir §7).

### 6.2 Arrêt forcé d'un nœud en cours d'écriture (`docker kill`, 2026-10-11)

Cluster à 3 nœuds (`partition_policy=pause_minority`), file quorum à 3 répliques `pwr.q` ; le client active les publisher confirms, chaque message porte un ID unique, et les ID confirmés sont consignés sur disque pour la réconciliation.

**Manche 1 (`fsync: os`, arrêt du follower n3)** : `docker kill pwr-n3` pendant la publication de 50 000 messages :

```
DONE published=50000 confirmed=50000 nacked=0 retries=0 elapsed=28.267s   # 杀后速率无可见变化
docker start pwr-n3 → 三节点 commit_index==last_applied==12，has_quorum=true
DRAIN total=50000 unique=50000 duplicates=0
confirmed_unique=50000 consumed_unique=50000 missing=0 extra=0
```

**Manche 2 (`fsync: always`, arrêt du leader quorum n1)** : `docker kill pwr-n1` pendant la publication de 200 000 messages :

```
DONE published=200000 confirmed=200000 nacked=3843 retries=3843 elapsed=2m7.494s
# nack 来自转发到已死 leader 的发布（"与节点 n1 通信失败"），客户端重投全部成功
docker start pwr-n1 → 三节点 commit_index==last_applied==16
DRAIN 对账：confirmed_unique=436785 consumed_unique=438140 missing=0 extra=1355
# extra=已落盘但 confirm 未返回的消息，at-least-once 允许，按 ID 去重即幂等
```

**Manche 1b (`fsync: os`, `wsl --shutdown` arrête brutalement toute la VM Docker, ce qui s'apparente à une coupure d'alimentation de la machine entière)** : arrêt brutal en cours de publication ; l'hôte avait enregistré 69 669 lignes confirmées ;
après reconstruction de la VM et rétablissement du cluster :

```
{"name":"pwr.q","messages":69669,"messages_ready":69669}
DRAIN total=69669 unique=69669 duplicates=0 ；missing=0 extra=0
```

Attention : `wsl --shutdown` ne fait que s'approcher de la « perte du tampon de l'OS » ; ce n'est **pas l'équivalent d'une coupure d'alimentation physique** (la perte d'alimentation de l'hôte et la couche de cache disque ne sont pas couvertes et restent non vérifiées).

### 6.3 Écritures concurrentes pendant la restauration (2026-10-11)

Sur un cluster en `fsync: always` : `docker stop` du follower n2 → publication d'un arriéré de 50 000 messages (tous confirmés) → `docker start pwr-n2`,
et **dès la même seconde**, poursuite de la publication de 20 000 messages vers les nœuds du côté majoritaire :

```
DONE published=20000 confirmed=20000 nacked=0 retries=0 elapsed=5.978s   # 约 3,300~6,400 条/秒
n2 日志：msg=已安装快照 component=broker index=4096 term=4              # 约 4 秒经快照追平
三节点 commit_index==last_applied==20；queue_totals.messages=82000       # 与四批发布总数精确相符
DRAIN total=82000 unique=80000 missing=0 extra=0                          # 2,000 重复为同 ID 重发批次的预期重复
```

Conclusion : les écritures concurrentes pendant le rattrapage ne sont ni perdues ni corrompues ; les compteurs sont exacts.

---

## 7. Problèmes et limites connus (recensés fidèlement)

1. **Bruit dans les logs de restauration (réellement observé)** : lorsqu'une file a connu par le passé des consommations/vidages (messages déjà ack/purge),
   son index conserve encore des références à des enregistrements récupérés ; à la restauration, un WARN **`恢复消息失败，已跳过` est imprimé pour chaque enregistrement**,
   et un `000000.seg` vide est créé/nettoyé (log `恢复时清理了无存活消息的段`).
   **Cela n'affecte pas la correction des données** (les messages non acquittés survivants sont correctement restaurés), mais **pollue les logs** et peut les inonder sur de grandes files / à haut débit.
   Recommandation : se fier à `已从磁盘恢复队列消息 ... messages=N` et ignorer ces WARN concernant des enregistrements réglés ;
   si le volume de logs est inacceptable, signalez-le aux mainteneurs du noyau (ce document ne modifie pas le code).
2. **La sauvegarde à chaud n'est pas sûre** (§2) : ne copiez pas directement `data_dir` pendant que le processus tourne.
3. **`fsync: os` ne garantit pas l'absence de perte en cas de coupure d'alimentation** : pour « confirm implique écriture sur disque », utilisez `batch` / `always`.
4. **Mots de passe en clair** : dans `meta/state.json`, les mots de passe des utilisateurs sont **en clair** (mesuré : `"password":"drillpass"` visible) —
   les fichiers de sauvegarde **doivent donc être traités comme des données sensibles** (contrôle d'accès, stockage chiffré). Voir `security-baseline.md`.
5. **【état de vérification】** L'arrêt forcé de processus (y compris l'arrêt du leader quorum en cours d'écriture, aux deux réglages `fsync: os`/`always`) et les écritures concurrentes pendant la restauration (publication à environ 4 000–6 000 msg/s pendant le rattrapage, rattrapage par instantané en 4 secondes, compteurs exacts, réconciliation missing=0) —— **【vérifié (2026-10-11, cluster à 3 nœuds sur Docker Desktop local)】** ; un `wsl --shutdown` arrêtant brutalement toute la VM Docker (s'apparentant à une coupure d'alimentation de la machine entière) a montré zéro perte de messages confirmés sur un seul échantillon, mais **la vraie coupure d'alimentation physique reste non vérifiée** ; la sauvegarde/restauration de cluster nœud par nœud et la sauvegarde/restauration du volume nommé Docker —— **【vérifié (2026-10-10, cluster à 3 nœuds sur Docker Desktop local)】** (voir §3.2 / §3.3 / §4.2).
6. **Observations annexes (consignées fidèlement lors de l'exercice du 2026-10-11, non bloquantes)** : ① après l'arrêt forcé du leader quorum, le chemin de transfert des publications des followers est resté pointé vers le leader mort pendant environ 76 secondes (les nouvelles publications ont été nackées pendant cette fenêtre ; les renvois du client ont tous réussi ; aucun message confirmé n'a été perdu) — il existe une fenêtre de gigue de disponibilité ; ② la vidange en une passe d'une grande file (438 000 messages) a montré une amplification de renvoi d'environ 153× (la sémantique at-least-once tient, et la file s'est finalement vidée exactement à zéro) ; ③ pendant le rattrapage d'une grande file, le démarrage d'un consommateur via un follower a occasionnellement rencontré `i/o timeout`, tandis qu'une connexion directe au leader fonctionnait.
