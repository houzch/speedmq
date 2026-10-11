# Plan de mise à niveau et de migration de SpeedMQ

> Version applicable : `1.0.0` (`broker.Version`, voir `speedmq_build_info` dans `/metrics`).
> Toutes les conclusions « mesurées » de ce document proviennent d'une exécution réelle sur cette machine ; celles qui ne l'ont pas été sont explicitement marquées **【non vérifié】**.
> Environnement de cette machine : Windows + PowerShell 5.1, Go 1.27.1 windows/386, `data_dir` temporaire + ports non standards.

---

## 1. Migration (de RabbitMQ vers SpeedMQ)

Ce projet vise la **compatibilité au niveau du protocole AMQP 0-9-1**, la « migration » consiste donc principalement à **changer l'adresse de connexion** :

- Aucune modification du code métier, seul `host/port/vhost` change (document de conception G3 « migration à coût nul »).
- La chaîne d'outils d'administration (`rabbitmqadmin`, UI d'administration, scripts de surveillance) pointe simplement vers le port de l'interface d'administration ; la forme des interfaces est alignée sur RabbitMQ (les conventions `amq.default`, `%2F`, `{error, reason}` etc. sont reprises telles quelles).
- Les ports par défaut sont identiques à ceux de RabbitMQ : AMQP `5672`, interface d'administration `15672` ; MQTT `1883`, RPC inter-nœuds `25672`.

**Différences sémantiques à vérifier avant la migration** (toutes intentionnelles dans ce dépôt, d'après le README / le document de conception) :

| Élément | Comportement de SpeedMQ | Impact sur la migration |
| --- | --- | --- |
| File transitoire (ni durable ni exclusive) | **Déclaration refusée** (541), `auto_delete` ne l'exempte pas | Les anciens clients qui dépendent de ce type de file échoueront ; il faut passer à durable ou exclusive |
| vhost par défaut `/` | **Non supprimable** (400), alors que RabbitMQ l'autorise | Les scripts d'automatisation qui suppriment le vhost par défaut échoueront (c'est la seule contrainte de sécurité volontaire) |
| Données des files classiques | **Non répliquées**, les données résident uniquement sur le nœud Owner | Pour une redondance inter-nœuds, utilisez plutôt une file quorum `x-queue-type=quorum` |
| Files quorum | Extension du nombre de répliques prise en charge, **réduction non prise en charge** | À dimensionner correctement dès le départ |
| Plugins | Pas d'écosystème de plugins Erlang, AMQP 1.0 / STOMP non implémentés | Les scénarios utilisant ces protocoles ne sont pas migrables pour l'instant |

**Migration des données** : les formats de stockage de SpeedMQ et RabbitMQ sont incompatibles ; **aucun outil de transfert de données en ligne/hors ligne n'est fourni**.
La méthode de migration est « créer un SpeedMQ vide → exécution en double pour validation → bascule progressive du trafic ». **【non vérifié】** Ce document ne contient aucun exercice réel de transfert de données RabbitMQ.

---

## 2. Principes généraux de mise à niveau

1. **Sauvegarder d'abord** (voir `backup-restore.md`) — filet de sécurité en cas d'échec de la mise à niveau.
2. **Arrêter le processus avant de remplacer** (le répertoire de données impose un écrivain unique, voir §4.2).
3. **Vérification obligatoire après la mise à niveau** : le processus démarre, `/api/overview` est lisible, `/metrics` est récupérable, le nombre de messages des files correspond à celui d'avant la sauvegarde.
4. La mise à niveau d'un cluster se fait **en rolling nœud par nœud**, un seul nœud à la fois (voir §5).

---

## 3. Agencement du répertoire de données (base factuelle pour la mise à niveau/migration)

Agencement du `data_dir` **mesuré** sur l'instance autonome de cette machine :

```
data/
├── meta/
│   ├── state.json        # instantané des métadonnées en mode autonome (vhost/exchanges/files/bindings/utilisateurs/permissions/politiques)
│   ├── users.seeded      # marqueur d'amorçage : les users du fichier de configuration ont déjà été amorcés
│   ├── vhosts.seeded     # marqueur d'amorçage : les vhosts du fichier de configuration ont déjà été amorcés
│   ├── raft.state        # 【mode cluster】mandat/vote Raft
│   ├── raft.log          # 【mode cluster】journal Raft
│   └── snapshot.json     # 【mode cluster】instantané Raft + table des membres
├── msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/
│   ├── 000001.seg        # fichier de segment (corps du message + attributs), format d'enregistrement : <len u32><crc32 u32><payload>
│   └── index/000001.idx  # index de file : seq-id → (numéro de segment, offset dans le segment, longueur, état)
└── quorum/<safe(vhost)>/<safe(queue)>/   # 【cluster】une file quorum = un groupe Raft (journal/instantané)
```

**Attention (deux points contre-intuitifs, tous deux fondés sur le code/mesures)** :

- En mode cluster, les fichiers persistants Raft sont **placés directement sous `meta/`** (`raft.state` / `raft.log` / `snapshot.json`) ;
  il **n'existe pas de sous-répertoire `meta/raft/`**. Références : les constantes de nom de fichier de `internal/raft/log.go` + `Dir: filepath.Join(b.cfg.DataDir, "meta")` dans `internal/broker/cluster.go`. **【vérifié（2026-10-10, cluster à 3 nœuds Docker Desktop local）】** —— Le contenu de la sauvegarde du volume d'un nœud du cluster montre directement `meta/raft.log`, `meta/raft.state`, `meta/*.seeded` (aucun sous-répertoire `meta/raft/` observé).
- Les noms de répertoires **ne sont pas les noms bruts vhost / file**, mais un encodage `store.SafeDirName` : préfixe `q_` ajouté, les octets hors `[A-Za-z0-9._-]` échappés en `%XX`.
  Mesuré : vhost `/` → répertoire `q_%2F`, file `persist.q` → répertoire `q_persist.q`.
  Cette conception vise à éviter la traversée de chemin et les noms de périphériques réservés de Windows (`con`/`nul` etc.).

---

## 4. Compatibilité des données

### 4.1 Les anciennes données sont-elles lisibles directement — oui

- **Format d'index rétrocompatible** : M8-1 a ajouté un champ « numéro de segment » aux enregistrements d'index (25 octets) ; **l'ancien format (21 octets, sans numéro de segment) reste lisible tel quel**,
  la lecture équivaut à « un seul segment (seg=1) » ; **la mise à niveau ne nécessite aucun script de migration**.
  Références : les constantes `indexEntrySize` / `legacyIndexEntrySize` et la logique `recover()` de `internal/store/store.go` ; README M8-1.
- **Sémantique de crash inchangée** : chaque enregistrement comporte un préfixe de longueur + un CRC32 ; à la restauration, les **enregistrements partiellement écrits/corrompus en fin de fichier sont écartés** et le fichier tronqué.
  Mesuré (voir §6 de `backup-restore.md`) : après arrêt puis redémarrage du processus, les 5 messages persistants de la file durable sont **tous restaurés**,
  et le log affiche `已从磁盘恢复队列消息 ... messages=5`.

**【vérifié（2026-10-10, machine locale）】** La lecture entre versions majeures a été mesurée : les données d'une file durable écrites avec le tag `v1.1.0` sont restaurées telles quelles sous `1.1.05` (commandes et sortie, voir §5).

### 4.2 Portée des `vhosts` / `users` dans la configuration (le piège le plus courant lors d'une mise à niveau)

- Les deux **ne prennent effet qu'au premier amorçage** : au premier démarrage, les vhosts/users de la configuration sont écrits dans les métadonnées et les fichiers marqueurs
  `meta/vhosts.seeded` / `meta/users.seeded` sont déposés ; **ensuite, ce sont les métadonnées qui font foi**.
- Par conséquent, **lors d'une mise à niveau/changement de configuration, ne comptez pas sur la modification du fichier de configuration pour ajouter ou supprimer des comptes ou des vhosts** — cela ne prend pas effet ;
  utilisez l'API d'administration ou `speedmqctl`.
- Inversement, la mise à niveau **n'**écrase **pas** les comptes existants avec la configuration : un mot de passe modifié pendant l'exécution n'est pas ramené à l'ancienne valeur de la configuration par un redémarrage,
  et un compte supprimé pendant l'exécution ne réapparaît pas. Références : la logique des marqueurs d'amorçage de `cluster.go` ; README M8-4 / M8-7.

**【vérifié（2026-10-10, machine locale）】** Les modifications à chaud ne sont effectivement pas écrasées par la configuration : avant la mise à niveau, le mot de passe de `guest` a été changé en `rotated` via l'API d'administration ; après un redémarrage avec `1.1.05` + le même `data_dir`, `guest/guest` (l'ancien mot de passe de la configuration) renvoie **401** et `guest/rotated` renvoie **200**.

### 4.3 Rotation des segments et récupération d'espace disque

- Les messages sont segmentés par taille (8 MiB par défaut) ; **lorsque tous les messages d'un segment sont acquittés et que le segment est scellé, le segment entier est supprimé**, et l'index est compressé et réécrit en conséquence.
- La mise à niveau ne modifie pas ce comportement ; un fichier mono-segment laissé par une ancienne instance continue de fonctionner normalement sous la nouvelle logique de rotation.

---

## 5. Mise à niveau du binaire (machine physique)

> **【vérifié（2026-10-10, machine locale）】** Exercice réel entre versions majeures : l'ancien binaire construit avec le tag `v1.1.0` a écrit un exchange/une file/des bindings durable + 5 messages persistants + vhost/compte/permissions/politiques et a modifié le mot de passe à chaud ; après arrêt du processus, redémarrage avec le binaire actuel `1.1.05` + **le même `data_dir` + le même port** —— le processus démarre normalement, les objets et le nombre de messages de `/api/queues` concordent, un vrai client AMQP récupère les 5 messages persistants, et `/metrics` renvoie `speedmq_build_info{version="1.1.05"}`. Les étapes ci-dessous restent une procédure générique applicable à toute version adjacente ; **rétrogradation** voir §8.

### 5.1 Étapes

```powershell
$base = "C:\speedmq"
$data = "$base\data"

# 1) arrêter le processus (un arrêt gracieux effectue un vidage final ; voir §4 « Cohérence »)
#    si lancé au premier plan : Ctrl+C ; si lancé comme service : Stop-Service / Stop-Process
Stop-Process -Name speedmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) sauvegarder le répertoire de données (impérativement après l'arrêt du processus)
Copy-Item -Recurse -Force $data "$base\backup-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) remplacer le binaire (placer les nouvelles versions speedmqd.exe / speedmqctl.exe aux chemins d'origine)
#    Copy-Item .\new\speedmqd.exe $base\speedmqd.exe -Force

# 4) démarrer
& "$base\speedmqd.exe" -config "$base\configs\speedmqd.json" -log-level info

# 5) vérifier : processus actif + API d'administration lisible
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

### 5.2 Liste de vérifications après mise à niveau

- Les logs de démarrage affichent `SpeedMQ 启动中 ... version=<新版本>` et `管理面已启动` ;
- Les `object_totals` / `queue_totals` de `/api/overview` correspondent à ceux d'avant la sauvegarde (comparer avec §5 de `backup-restore.md`) ;
- Dans `/api/queues`, les `messages` / `messages_ready` de chaque file durable correspondent à ceux d'avant la sauvegarde ;
- `/metrics` est récupérable et `speedmq_plugin_up{name="amqp091"} 1`, `{name="mqtt"} 1`.

---

## 6. Mise à niveau de l'image (conteneur)

L'image fait environ 13 MB (binaire lié statiquement + alpine), **s'exécute en non-root (uid 10001)**, le répertoire de données est monté sur `/var/lib/speedmq`.

```powershell
# 1) tirer/construire la nouvelle image (utiliser le nouveau numéro de version comme tag pour éviter toute confusion old/new)
docker build -t speedmq:1.0.0 .

# 2) arrêter l'ancien conteneur (compose conserve le volume nommé speedmq-data)
docker compose down

# 3) démarrer la nouvelle version (modifier image dans le fichier compose pour le nouveau tag)
docker compose up -d

# 4) état et logs
docker compose ps
docker compose logs -f --tail 100
```

> **Tâche ponctuelle dans le conteneur** (par exemple exécuter `speedmqctl` dans le conteneur) : le `run` de `docker compose ...` doit être accompagné de `-T` en environnement non interactif,
> sinon il échoue en demandant un TTY :
> ```powershell
> docker compose run -T --rm broker speedmqctl -user guest -pass guest status
> ```

La persistance des données repose sur le **volume nommé** `speedmq-data` de compose ; la reconstruction du conteneur ne perd pas les données (véritable écriture sur disque depuis M4).
Si vous devez sauvegarder le contenu du volume avant la mise à niveau, cela équivaut à sauvegarder `/var/lib/speedmq` (voir §3.2 de `backup-restore.md`).

> **【vérifié（2026-10-10, Docker Desktop local）】** Un exercice réel de mise à niveau d'image a été effectué : construction locale de l'ancienne image `speedmq:1.1.0-old` (git tag `v1.1.0`) et de la nouvelle `speedmq:1.1.05-new` (code source actuel) ; démarrage de l'ancienne image avec un compose mono-nœud → création de la file durable `persist.q` et publication de 5 messages persistants (`delivery_mode=2`) → `docker compose down` (**sans `-v`**, le volume nommé `speedmq-data` étant conservé) → après passage du tag image du compose à la nouvelle image, `docker compose up -d`.
> Résultat : le nouveau conteneur démarre normalement, les logs de démarrage affichent `已从磁盘恢复队列消息 ... queue=persist.q messages=5`, `/metrics` renvoie `speedmq_build_info{version="1.1.05"}`, `queue_totals.messages=5` de `/api/overview` et `messages=5` de la file `persist.q` concordent avant et après la mise à niveau, et un vrai client AMQP (`rabbitmq/amqp091-go`) récupère un à un les 5 messages persistants (`deliveryMode=2`).
> Note : `v1.1.0` est l'image de la marque `speedmq` d'avant le changement de nom (binaire `speedmqd`, métrique `speedmq_build_info`, répertoire de données par défaut `/var/lib/speedmq`), de nomenclature différente de la version actuelle ; dans l'exercice, la configuration `data_dir=/var/lib/speedmq` fait foi et le propriétaire du volume nommé a été préconfiguré (l'ancienne image n'a pas ce chemin inscriptible). Cela n'affecte pas la conclusion « recréer le conteneur en changeant le tag ne perd pas les données du volume nommé ».

---

## 7. Bascule progressive et restauration (rollback)

### 7.1 Mode autonome

- **Bascule progressive** : SpeedMQ en mode autonome n'a pas de capacité intégrée de « double version ancienne/nouvelle dans le même processus ». La bascule progressive praticable est le **shadowing en dérivation** :
  l'instance de la nouvelle version s'attache d'abord en **consommation en lecture seule / file miroir** au même flux amont pour observation, puis on bascule le côté écriture après confirmation.
- **Rollback** :
  1. arrêter le processus de la nouvelle version ;
  2. remettre l'ancien binaire ;
  3. si la nouvelle version a déjà écrit des données, **il faut restaurer le `data_dir` à partir de la sauvegarde d'avant la mise à niveau** (voir ci-dessous).
  Il **n'**existe **aucune** garantie du type « la nouvelle version a écrit, l'ancienne lit directement » — la rétrogradation inter-versions est traitée au §8.

### 7.2 Cluster (mise à niveau progressive)

La plateforme ne fournit pas de « mise à niveau progressive en un clic » ; il faut opérer manuellement nœud par nœud dans l'ordre suivant :

1. **Ne mettre à niveau qu'un seul nœud à la fois** : arrêter ce nœud → sauvegarder son `data_dir` → remplacer le binaire → démarrer → attendre qu'il rejoigne et rattrape
   (`speedmqctl cluster_status` / `GET /api/cluster` pour voir `role`, `commit_index`/`last_applied`).
2. **Ordre recommandé** : mettre à niveau d'abord les **learners / membres non votants** (sans impact sur la majorité), puis les **followers**, et enfin le **leader**
   (la mise à niveau du leader déclenche une élection, avec une brève indisponibilité en écriture).
3. **Impact de l'arrêt sur la majorité** (essentiel) :
   - Cluster à 3 nœuds : **au plus 1** membre votant arrêté simultanément ; en arrêter 2 fait perdre la majorité et, sous `pause_minority`, **tout le cluster suspend son service**.
   - Cluster à 2 nœuds : arrêter 1 nœud fait perdre la majorité ; il **n'offre pas de capacité de mise à niveau progressive** (au moins 3 nœuds sont recommandés).
   - Par conséquent, lors d'une mise à niveau progressive, **il est strictement interdit d'arrêter plusieurs membres votants à la fois**.
4. **Ne pas combiner changement de membres et mise à niveau** : le changement de membres **ne dispose pas de joint consensus** ; un seul changement de configuration non validé est autorisé à la fois ;
   pendant la mise à niveau, évitez d'appeler simultanément `add_member` / `remove_member`.
5. Après la mise à niveau, vérifier que les `object_totals` de `GET /api/cluster` correspondent à ceux d'avant la mise à niveau.

> **【vérifié（2026-10-10, cluster à 3 nœuds Docker Desktop local）】** Un exercice réel de mise à niveau progressive **inter-versions** d'un vrai cluster a été effectué : la base était `speedmq:1.1.0-old` construit localement (git tag `v1.1.0`), nœud par nœud (n1 → n2 → n3, un à la fois : arrêt, sauvegarde de son volume nommé, échange d'image puis démarrage) jusqu'à l'actuel `1.1.05` (construit localement `speedmq:1.1.05-new`, compose basculant l'image via un override par nœud). Tout au long de l'opération, le cluster a toujours conservé un leader et la majorité —— lors de l'arrêt du leader courant, un nouveau leader a été élu en environ 0,6 s, `pause_minority` n'a pas déclenché de suspension, et aucune interruption externe n'est survenue pendant l'arrêt ; après la mise à niveau, les `object_totals` de `GET /api/cluster` concordent avec ceux d'avant (`queues:1`), le décompte des 25 messages persistants de la file quorum à 3 répliques `qroll` reste inchangé et ceux-ci restent relisibles un à un, et après publication de 5 nouveaux messages les 30 au total sont récupérables.
> L'ordre ci-dessus reste valable selon les contraintes générales de Raft ; cet exercice couvre le scénario à 3 nœuds. **【vérifié（2026-10-10, Docker Desktop local）2 nœuds : en arrêter 1 fait déjà perdre la majorité】** Un cluster minimal à 2 nœuds a en outre été créé (n1/n2, dont les `peers` ne contiennent que ces deux, `partition_policy=pause_minority`) et mesuré : les deux nœuds en ligne, `GET /api/cluster` vaut `has_quorum=true / paused=false` (n1 leader, n2 follower) ; après `docker stop` de n2, n1 passe en environ 2 s à `has_quorum=false / paused=true` —— autrement dit, l'arrêt de 1 membre votant fait perdre la majorité et `pause_minority` suspend le service, **ce qui confirme que 2 nœuds n'offrent pas de capacité de mise à niveau progressive** ; après `docker start` pour restaurer n2, n1 revient en environ 2 s à `has_quorum=true / paused=false` (service rétabli automatiquement).

> **【vérifié（2026-10-11, cluster à 3 nœuds Docker Desktop local）】La mise à niveau progressive sous partition réseau a été mesurée** (inter-versions `v1.1.0` → `1.1.05` ; la partition utilisait une topologie à double réseau : le RPC Raft inter-nœuds ne passe que par `cluster-net` avec des IP statiques, le trafic clients/interface de gestion passe par `client-net`, et `docker network disconnect/connect` bascule la partition) :
> - **Follower partitionné** : le côté majoritaire a conservé le quorum pendant toute la partition, a publié/consommé normalement et a mené à bien l'« arrêt → échange d'image → démarrage → rattrapage » d'un nœud majoritaire pendant que la partition persistait ; le cluster en versions mixtes (1.1.0 + 1.1.05) est resté opérationnel. Après rétablissement de la partition, le nœud isolé (ancienne version) a rejoint le cluster et rattrapé son retard ; la réconciliation des messages par ensemble d'ID uniques a donné 35/35 sans perte.
> - **Leader partitionné** : la majorité a élu un nouveau leader en environ **630 ms** ; l'ancien leader isolé a conservé `role=leader` mais avec `has_quorum=false / paused=true` (`pause_minority` en vigueur, sans élévation proactive de son mandat) ; la mise à niveau d'un autre nœud du côté majoritaire pendant la partition a réussi ; après rétablissement, l'ancien leader a rejoint le cluster sans split-brain ni divergence de données.
> - **Comportement attendu sans PreVote (consigné fidèlement)** : le follower isolé a immédiatement mené des campagnes électorales répétées, son mandat augmentant d'environ +2 toutes les 2 s (les deux groupes Raft, métadonnées et file, s'élevant en alternance) ; à l'instant du rétablissement, il s'est **emparé du leadership** avec son mandat plus élevé (élu en 1,4 s lors de l'exercice) — une perturbation par changement de leader, mais aucun problème de correction.
> - **Deux pièges opérationnels** : ① si `peers` utilise des noms d'hôte et que les conteneurs sont aussi rattachés à un second réseau accessible, le trafic Raft fait un détour et provoque une « fausse partition » (le nœud isolé affichait encore `has_quorum=true` après plus d'une minute) — le RPC inter-nœuds ne doit passer que par un seul réseau (`peers` pointant directement vers les IP statiques de ce réseau) ; ② au rétablissement, `docker network connect` doit porter `--ip <IP d'origine>`, sinon le nœud obtient une nouvelle IP tandis que la majorité continue d'appeler l'ancienne adresse, et le nœud isolé — porteur d'un journal en retard — mène des campagnes répétées avec un mandat gonflé (lors de l'exercice, cela a fait passer le term du cluster de 320 à 488 et le commit des métadonnées de 13 à 97), et un état transitoire peut apparaître où le leader affiche `quorum=true` tout en étant brièvement `paused=true`.
> - Pendant la partition, l'API de gestion du côté minoritaire restait lisible (pratique pour l'observation), tandis que la publication AMQP était rejetée (`pause_minority` en vigueur).

---

## 8. Parties non prises en charge / non vérifiées (listées explicitement)

- **Rétrogradation entre versions majeures : non prise en charge, non vérifiée**. Si la nouvelle version a écrit des données dans un nouveau format/de nouvelles sémantiques, il **n'**existe **aucune** garantie de « retour à l'ancien binaire en lecture directe » ;
  le rollback ne peut reposer que sur la sauvegarde d'avant la mise à niveau.
- **Format de configuration inchangé** : toujours JSON + variables d'environnement `SPEEDMQ_*`. **La configuration YAML n'est pas encore prise en charge** (nécessite l'introduction d'une dépendance de parsing, M8-17 à évaluer) ;
  la mise à niveau n'apporte pas YAML.
- **Mise à niveau à chaud des plugins/protocoles** : le plugin est compilé avec le noyau (forme A) ou lancé via `spawn` selon la configuration (forme B) ;
  mettre à niveau le noyau = redémarrer le processus ; il **n'**existe **aucun** mécanisme de remplacement à chaud du binaire sur place.
- **Migration sur place du moteur de stockage** : la rotation des segments/compression de l'index est un comportement d'arrière-plan en cours d'exécution ; il **n'**existe **aucune** commande distincte de « migration/compression de données ».
- **Mise à niveau de cluster avec disque plein** : **non vérifiée**. (La mise à niveau progressive sous partition réseau a été mesurée le 2026-10-11 ; voir la note en fin de §7.)
- Ce document **ne contient** aucune validation de transfert de données entre SpeedMQ et d'autres brokers (RabbitMQ).
