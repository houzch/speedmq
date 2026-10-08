# Surveillance et alertes de SpeedMQ

Ce répertoire fournit des modèles de surveillance directement utilisables :

| Fichier | Rôle |
| --- | --- |
| `prometheus-alerts.yml` | Règles d'alerte Prometheus (`groups: - name: speedmq`) |
| `grafana-dashboard.json` | Tableau de bord Grafana importable (les panneaux couvrent les signaux clés ci-dessous) |
| `README.md` | Utilisation, liste des métriques, signification et traitement de chaque alerte, lacunes connues |

---

## 1. Comment l'utiliser

### 1.1 Collecte (Prometheus)

L'interface d'administration (par défaut `:15672`) expose `/metrics` au format texte Prometheus, **Basic Auth requis** :

```yaml
# prometheus.yml
scrape_configs:
  - job_name: speedmq
    metrics_path: /metrics
    basic_auth:
      username: guest
      password: guest
    static_configs:
      - targets: ["127.0.0.1:15672"]
```

> Il est recommandé de créer un compte en lecture seule dédié à la surveillance (l'étiquette `monitoring` suffit pour lire les métriques) et de ne pas réutiliser le mot de passe administrateur.

Vérifier que la collecte fonctionne (PowerShell) :

```powershell
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
(Invoke-WebRequest -Uri 'http://127.0.0.1:15672/metrics' -Headers @{Authorization="Basic $pair"} -UseBasicParsing).Content
curl.exe -s -u guest:guest http://127.0.0.1:15672/metrics
```

### 1.2 Règles d'alerte

Placez `prometheus-alerts.yml` dans le répertoire de règles de Prometheus, référencez-le dans `prometheus.yml` puis rechargez :

```yaml
rule_files:
  - "rules/speedmq-alerts.yml"
```

Les règles utilisent uniformément `job="speedmq"` ; si le nom de votre job diffère, remplacez-le partout.

### 1.3 Tableau de bord Grafana

`grafana-dashboard.json` s'importe via **Dashboards → Import → upload JSON**, en sélectionnant votre source de données Prometheus lors de l'import
(le tableau de bord la référence via la variable `${DS_PROMETHEUS}`). La variable de modèle `DS_PROMETHEUS` est renseignée dans le mappage d'import.

**【non vérifié】** Aucune instance Grafana n'a été démarrée sur cette machine, aucune validation d'import réelle n'a été faite ; ce JSON n'a fait l'objet que d'une validation syntaxique JSON (14 panneaux, analyse réussie).

---

## 2. Extrait réel de `/metrics` (preuve)

Voici la **sortie réelle** de `/metrics` de l'instance `1.0.0` de cette machine (une file durable `persist.q` ayant été créée,
les métriques per-queue portant les étiquettes `vhost`/`queue` apparaissent) :

```
# HELP speedmq_up 节点是否存活
# TYPE speedmq_up gauge
speedmq_up 1
# HELP speedmq_build_info 构建信息
# TYPE speedmq_build_info gauge
speedmq_build_info 1{version="1.0.0",node="speedmq@DESKTOP-HBDCVPA"}
# HELP speedmq_resource_blocked 资源水位是否阻塞了生产者（1=阻塞中）
# TYPE speedmq_resource_blocked gauge
speedmq_resource_blocked 0
# HELP speedmq_connections 当前连接数
# TYPE speedmq_connections gauge
speedmq_connections 0
# HELP speedmq_channels 当前通道数
# TYPE speedmq_channels gauge
speedmq_channels 0
# HELP speedmq_queues 当前队列数
# TYPE speedmq_queues gauge
speedmq_queues 0
# HELP speedmq_exchanges 当前交换机数
# TYPE speedmq_exchanges gauge
speedmq_exchanges 6
# HELP speedmq_consumers 当前消费者数
# TYPE speedmq_consumers gauge
speedmq_consumers 0
# HELP speedmq_queue_messages 就绪消息总数
# TYPE speedmq_queue_messages gauge
speedmq_queue_messages 0
# HELP speedmq_queue_messages_unacknowledged 未确认消息总数
# TYPE speedmq_queue_messages_unacknowledged gauge
speedmq_queue_messages_unacknowledged 0
# HELP speedmq_process_memory_bytes 本进程向操作系统申请的内存字节数
# TYPE speedmq_process_memory_bytes gauge
speedmq_process_memory_bytes 1564672
# HELP speedmq_memory_total_bytes 物理内存总量
# TYPE speedmq_memory_total_bytes gauge
speedmq_memory_total_bytes 34181279744
# HELP speedmq_disk_free_bytes 数据目录可用空间
# TYPE speedmq_disk_free_bytes gauge
speedmq_disk_free_bytes 240091688960
# HELP speedmq_memory_high_watermark 内存水位比例
# TYPE speedmq_memory_high_watermark gauge
speedmq_memory_high_watermark 0.4
# HELP speedmq_disk_free_limit_bytes 磁盘剩余空间下限
# TYPE speedmq_disk_free_limit_bytes gauge
speedmq_disk_free_limit_bytes 52428800
# HELP speedmq_queue_messages_ready 队列中的就绪消息数
# TYPE speedmq_queue_messages_ready gauge
# HELP speedmq_queue_messages_unacknowledged 队列中的未确认消息数
# TYPE speedmq_queue_messages_unacknowledged gauge
# HELP speedmq_queue_consumers 队列上的消费者数
# TYPE speedmq_queue_consumers gauge
# HELP speedmq_queue_memory_bytes 队列内存占用估算值
# TYPE speedmq_queue_memory_bytes gauge
# HELP speedmq_queue_messages_published_total 队列累计接收的消息数
# TYPE speedmq_queue_messages_published_total counter
# HELP speedmq_queue_messages_delivered_total 队列累计投递的消息数
# TYPE speedmq_queue_messages_delivered_total counter
# HELP speedmq_queue_messages_acked_total 队列累计确认的消息数
# TYPE speedmq_queue_messages_acked_total counter
speedmq_queue_messages_ready{vhost="/",queue="persist.q"} 5
speedmq_queue_messages_unacknowledged{vhost="/",queue="persist.q"} 0
speedmq_queue_consumers{vhost="/",queue="persist.q"} 0
speedmq_queue_memory_bytes{vhost="/",queue="persist.q"} 365
speedmq_queue_messages_published_total{vhost="/",queue="persist.q"} 5
speedmq_queue_messages_delivered_total{vhost="/",queue="persist.q"} 0
speedmq_queue_messages_acked_total{vhost="/",queue="persist.q"} 0
# HELP speedmq_plugin_info 插件元数据（value 恒为 1，状态见 state 标签）
# TYPE speedmq_plugin_info gauge
# HELP speedmq_plugin_up 插件是否在服务（1=enabled，0=其它状态：disabled/failed/down）
# TYPE speedmq_plugin_up gauge
speedmq_plugin_info{name="amqp091",version="0.1.0",api_version="v1",state="enabled"} 1
speedmq_plugin_up{name="amqp091",state="enabled"} 1
speedmq_plugin_info{name="mqtt",version="0.1.0",api_version="v1",state="enabled"} 1
speedmq_plugin_up{name="mqtt",state="enabled"} 1
```

---

## 3. Liste des métriques (toutes réellement présentes, source `internal/management/metrics.go`)

| Métrique | Type | Étiquettes | Sémantique |
| --- | --- | --- | --- |
| `speedmq_up` | gauge | — | Le processus se déclare vivant (actuellement toujours 1) |
| `speedmq_build_info` | gauge | `version`,`node` | Informations de build, value toujours 1 |
| `speedmq_resource_blocked` | gauge | — | Indique si le niveau de ressources bloque les producteurs (1=bloqué) |
| `speedmq_connections` | gauge | — | Nombre de connexions actuelles |
| `speedmq_channels` | gauge | — | Nombre de canaux actuels |
| `speedmq_queues` | gauge | — | Nombre de files actuelles |
| `speedmq_exchanges` | gauge | — | Nombre d'exchanges actuels |
| `speedmq_consumers` | gauge | — | Nombre de consommateurs actuels |
| `speedmq_queue_messages` | gauge | — | Nombre total de messages prêts **global** |
| `speedmq_queue_messages_unacknowledged` | gauge | — | Nombre total de messages non acquittés **global** |
| `speedmq_process_memory_bytes` | gauge | — | Mémoire **en cours d'utilisation** du processus (`HeapInuse+StackInuse`) |
| `speedmq_memory_total_bytes` | gauge | — | Mémoire physique totale |
| `speedmq_disk_free_bytes` | gauge | — | Espace disponible du répertoire de données |
| `speedmq_memory_high_watermark` | gauge | — | Ratio du seuil de mémoire |
| `speedmq_disk_free_limit_bytes` | gauge | — | Limite inférieure d'espace disque restant |
| `speedmq_queue_messages_ready` | gauge | `vhost`,`queue` | Nombre de messages prêts d'une file |
| `speedmq_queue_messages_unacknowledged` | gauge | `vhost`,`queue` | Nombre de messages non acquittés d'une file |
| `speedmq_queue_consumers` | gauge | `vhost`,`queue` | Nombre de consommateurs d'une file |
| `speedmq_queue_memory_bytes` | gauge | `vhost`,`queue` | Estimation de l'occupation mémoire d'une file |
| `speedmq_queue_messages_published_total` | counter | `vhost`,`queue` | Nombre cumulé de messages reçus par la file |
| `speedmq_queue_messages_delivered_total` | counter | `vhost`,`queue` | Nombre cumulé de messages distribués par la file |
| `speedmq_queue_messages_acked_total` | counter | `vhost`,`queue` | Nombre cumulé de messages acquittés par la file |
| `speedmq_plugin_info` | gauge | `name`,`version`,`api_version`,`state` | Métadonnées du plugin, value toujours 1 |
| `speedmq_plugin_up` | gauge | `name`,`state` | Indique si le plugin est en service (1=enabled, 0=autre) |

### 3.1 Points d'attention (pour éviter les erreurs)

- **Deux familles de séries de même nom mais de cardinalité différente** : `speedmq_queue_messages_unacknowledged` possède **à la fois** une série globale sans étiquette
  et une série per-queue avec étiquettes ; côté « prêt », la série globale s'appelle `speedmq_queue_messages` et la per-queue
  `speedmq_queue_messages_ready` (noms asymétriques). Lors de l'écriture de règles, utilisez `{queue=~".+"}` pour ne cibler explicitement que la famille per-queue.
- **Portée de `speedmq_process_memory_bytes`** : l'implémentation correspond à `MemStats.HeapInuse + StackInuse` (**mémoire en cours d'utilisation**),
  la même portée que la détermination du seuil de mémoire du noyau ; mais son texte `# HELP` indique « nombre d'octets de mémoire demandés au système d'exploitation », **un texte qui ne correspond pas à la portée réelle** ;
  c'est ce document qui fait foi.
- **Les séries per-queue n'apparaissent que si la file existe** : la série disparaît après suppression de la file (elle devient stale côté Prometheus).
  Pour les alertes du type « la file devrait exister mais n'a pas de données », associez `absent()` ou le `or vector(0)` de Grafana.
- **Les counter repartent de zéro après un redémarrage du processus** : les `*_total` sont des cumuls internes au processus et repartent de 0 au redémarrage ; utilisez `rate()`/`increase()`
  et ne fixez pas de seuil directement sur les valeurs absolues.
- **Signaux de cluster sans metrics** : voir §5.

---

## 4. Signification des alertes et traitement recommandé (correspondant à `prometheus-alerts.yml`)

| Alerte | Condition de déclenchement | Signification | Traitement recommandé |
| --- | --- | --- | --- |
| `SpeedMQScrapeDown` | `up{job="speedmq"} == 0` 1m | La cible de collecte est totalement injoignable | Vérifier processus/port/réseau/authentification ; redémarrer et consulter les logs de démarrage |
| `SpeedMQProcessNotUp` | `speedmq_up == 0` 1m | Collecte réussie mais le processus se déclare non vivant | Point de repli, consulter les logs de sortie anormale |
| `SpeedMQPluginDown` | `speedmq_plugin_up == 0` 2m | Plugin disabled/failed/down | `speedmqctl plugins show <name>` pour voir `runtime_note` ; un plugin externe avec `restart=always` se rétablit généralement seul |
| `SpeedMQResourceBlocked` | `speedmq_resource_blocked == 1` 5m | Seuil mémoire/disque déclenché, producteurs bloqués | Vérifier le seuil de mémoire et l'espace disque ; confirmer que les consommateurs progressent |
| `SpeedMQMemoryWatermarkHigh` | Ratio de mémoire utilisée > 0.9×seuil 10m | Approche du seuil de mémoire | Réduire l'arriéré/accélérer la consommation pour éviter le blocage |
| `SpeedMQDiskFreeLow` | Disponible < 1.5×limite disque 10m | Le répertoire de données est presque plein | Augmenter la capacité/nettoyer ; atteindre la limite bloque les producteurs |
| `SpeedMQQueueBacklogGrowing` | Prêts >10000 et croissance monotone sur 15m | Arriéré persistant dans la file | Augmenter les consommateurs / vérifier le côté consommation ; investiguer les anomalies de lettres mortes/TTL |
| `SpeedMQQueueNoConsumers` | Consommateurs=0 et messages prêts présents 15m | Aucune consommation | Vérifier le processus côté consommation ; confirmer que le consommateur n'est pas déconnecté |
| `SpeedMQUnackedPileUp` | Non acquittés >1000 15m | Consommateur bloqué/sans ack | Vérifier la logique de traitement du consommateur et le prefetch ; fermer la connexion et redélivrer si nécessaire |
| `SpeedMQConnectionSpike` | Connexions >10000 10m | Nombre de connexions anormal | Vérifier les fuites de connexions ; les clients doivent réutiliser les connexions |

> Les seuils (10000 / 1000 etc.) sont des **valeurs de départ** ; ajustez-les selon la taille de vos files et les caractéristiques de votre activité.

---

## 5. Lacune connue : le cluster « perd la majorité / sans leader » n'a actuellement aucune métrique

- **Fait** : `/metrics` **ne contient** aucune métrique de type cluster (pas de `speedmq_cluster_*`). L'état du cluster n'existe que dans le JSON de `GET /api/cluster` :
  `{"enabled":true,"mode":"raft","role":"leader","leader":"...","has_quorum":true,"paused":false,...}`.
- **Par conséquent**, `prometheus-alerts.yml` **n'écrit délibérément aucune** alerte basée sur des métriques de cluster — de telles alertes **ne se déclencheraient jamais**
  (Prometheus ne signale pas une erreur pour un nom de métrique inexistant), ce serait une livraison « qui semble correcte mais est en réalité inopérante ».
- **Solutions à construire soi-même** (choisir l'une des deux, à mettre en place en dehors de SpeedMQ, hors périmètre de ce dépôt) :
  1. Utiliser un exporter JSON générique pour collecter `/api/cluster`, le mapper en métriques personnalisées (par ex. `speedmq_cluster_has_quorum`), puis alerter sur cette métrique ;
  2. Utiliser un script de sondage appelant périodiquement `/api/cluster` et émettre une alerte lorsque `has_quorum=false` ou `paused=true`.
- Portée des seuils associés : `has_quorum=false` signifie une perte de contact avec la majorité ; sous `pause_minority` (par défaut), **le service se met alors en pause et déconnecte les connexions**.

---

## 6. Autres points **non vérifiés**

- Le tableau de bord Grafana **n'a pas été importé et validé dans un vrai Grafana** (seule la validation syntaxique JSON est réussie).
- Les règles d'alerte **n'ont pas été chargées et validées dans un vrai Prometheus/Alertmanager** (Prometheus n'a pas été démarré sur cette machine).
  Mais les **noms de métriques des règles ont été comparés un à un avec la sortie réelle de `/metrics`** (voir §2/§3) ; il n'y a donc pas de risque de « nom mal écrit empêchant tout déclenchement ».
