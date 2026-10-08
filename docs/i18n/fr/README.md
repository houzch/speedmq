<!-- i18n-switcher -->
[简体中文](../../../README.md) | [繁體中文](../zh-TW/README.md) | [English](../en/README.md) | [日本語](../ja/README.md) | [한국어](../ko/README.md) | [Español](../es/README.md) | [Deutsch](../de/README.md) | **Français** | [العربية](../ar/README.md) | [Русский](../ru/README.md) | [Italiano](../it/README.md) | [Nederlands](../nl/README.md) | [Português](../pt/README.md) | [Bahasa Indonesia](../id/README.md) | [ไทย](../th/README.md) | [Tiếng Việt](../vi/README.md) | [Bahasa Melayu](../ms/README.md) | [Filipino](../fil/README.md)

# SpeedMQ

Un intergiciel de messagerie **compatible RabbitMQ** écrit en Go. Les clients RabbitMQ existants peuvent s'y connecter **sans modifier le code ni changer de SDK**, il suffit de changer l'adresse de connexion.

## Présentation

- **Compatibilité protocolaire** : AMQP 0-9-1 (y compris les extensions RabbitMQ) et MQTT 3.1.1 ; la base de compatibilité est la **sémantique de RabbitMQ 4.3**.
- **Déploiement simple** : un seul binaire / un seul conteneur, l'UI d'administration est intégrée, aucun Nginx, base de données ou runtime Node supplémentaire n'est nécessaire.
- **Exploitation suffisante** : UI d'administration (files / exchanges / connexions / permissions des comptes / hôtes virtuels / politiques / limites / cluster), Prometheus `/metrics`, ligne de commande `speedmqctl`.
- **Ports par défaut** : `5672` (AMQP), `1883` (MQTT), `15672` (UI d'administration / API HTTP / métriques).

Capacités déjà disponibles : persistance (journal par segments + niveaux fsync + reprise après crash), accusés de publication, TTL / lettres mortes / limite de longueur, priorité des consommateurs, Direct Reply-To, cluster (métadonnées Raft + files quorum + transfert inter-nœuds), démarrage/arrêt à chaud des plugins.

***

## Démarrage rapide

### Méthode 1 : Docker (recommandée)

**Sans cloner le dépôt : récupérez l image et lancez-la directement.**

```bash
docker run -d --name speedmq \
  -p 5672:5672 -p 1883:1883 -p 15672:15672 \
  -v speedmq-data:/var/lib/speedmq \
  houzch/speedmq:1.1.03
```

L image est publiée à deux endroits avec un contenu identique (prenez le plus rapide) : Docker Hub `houzch/speedmq` et GitHub GHCR `ghcr.io/houzch/speedmq` ; les deux proposent `linux/amd64` et `linux/arm64`.

- Les données vont dans le volume nommé `speedmq-data`, conservé même si le conteneur est recréé.
- Arrêt / suppression : `docker stop speedmq`, `docker rm speedmq` (le volume de données est conservé).

**Pour modifier la configuration ou utiliser compose, clonez le dépôt :**

```bash
git clone https://github.com/houzch/speedmq.git
cd speedmq
docker compose pull && docker compose up -d   # Utilise l image publiée ; remplacez par up -d --build pour compiler localement

docker compose ps        # L état doit être Up (healthy)
docker compose logs -f   # Suivre les logs
```

- La configuration est montée en lecture seule depuis `configs/speedmqd.json` ; les modifications prennent effet après `docker compose restart`.
- Arrêt : `docker compose down` (conserve les données) ; `docker compose down -v` (supprime aussi les données).

### Méthode 2 : binaire local (nécessite Go 1.24+)

```bash
git clone https://github.com/houzch/speedmq.git
cd speedmq
go build -o bin/speedmqd ./cmd/speedmqd
go build -o bin/speedmqctl ./cmd/speedmqctl
./bin/speedmqd -config configs/speedmqd.json
```

> Les artefacts de build de l'UI d'administration ne sont pas versionnés. Si vous voulez utiliser l'UI, exécutez d'abord `npm ci && npm run build` dans `web/` ;
> sans ce build, le démarrage et l'envoi/réception de messages fonctionnent normalement, mais l'accès à `/` affiche le message « UI d'administration non construite ».

### Première connexion (changez impérativement le compte par défaut)

| Entrée | Adresse / identifiants |
| --- | --- |
| UI d'administration | <http://localhost:15672/> (nom d'utilisateur `guest`, mot de passe `guest`) |
| AMQP | `amqp://guest:guest@localhost:5672/` |
| MQTT | `localhost:1883` (mêmes identifiants) |

Sur une instance nouvellement installée, le compte racine est marqué « changement de mot de passe obligatoire à la première connexion » : après s'être connecté à l'UI d'administration, vous êtes **obligé de modifier à la fois le nom du compte et le mot de passe** ; l'accès à l'interface n'est possible qu'après.

Vous pouvez aussi le faire directement via l'API (adapté à l'automatisation) :

```bash
curl -u guest:guest -X POST -H 'Content-Type: application/json' \
  -d '{"name":"admin","password":"<新口令>"}' \
  http://127.0.0.1:15672/api/users/guest/credentials
```

> ⚠️ Le compte `guest/guest` par défaut se comporte comme dans RabbitMQ : **il n'autorise que les connexions locales**. Pour se connecter depuis l'extérieur du conteneur / à distance, il faut activer `remote_access` pour cet utilisateur dans la configuration (l'exemple de configuration l'a déjà activé pour le scénario conteneur).
> **Dès que le service est accessible de l'extérieur, changez immédiatement les identifiants.**

### Connecter votre application (il suffit de changer l'adresse de connexion)

```python
# Python (pika)
import pika
conn = pika.BlockingConnection(pika.ConnectionParameters("127.0.0.1"))
```

```go
// Go (amqp091-go)
conn, _ := amqp.Dial("amqp://guest:guest@127.0.0.1:5672/")
```

```bash
# MQTT (client mosquitto)
mosquitto_sub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/#' -q 1
mosquitto_pub -h 127.0.0.1 -p 1883 -u guest -P guest -t 'sensors/room1/temp' -m 21.5 -q 1
```

L'API HTTP d'administration est compatible avec `rabbitmqadmin` ; « créer une file / un exchange » dans l'UI d'administration correspond exactement à l'endpoint de déclaration standard, un script peut donc faire de même :

```bash
# déclarer une file (une file quorum s'exprime avec arguments: {"x-queue-type":"quorum"})
curl -u guest:guest -X PUT -H 'Content-Type: application/json' \
  -d '{"durable":true,"auto_delete":false,"arguments":{}}' \
  http://127.0.0.1:15672/api/queues/%2F/my.queue
```

### Exploitation courante

| Élément | Entrée |
| --- | --- |
| UI d'administration | <http://localhost:15672/> : files / exchanges / connexions / permissions des comptes / hôtes virtuels / politiques / limites / indicateurs de fonctionnalités / cluster ; en haut à droite, vous pouvez régler l'actualisation automatique et la **langue de l'interface** |
| Métriques de surveillance | <http://localhost:15672/metrics> (texte Prometheus, authentification requise) ; tableaux de bord et alertes dans [docs/ops/monitoring](ops/monitoring/README.md) |
| Ligne de commande | `./bin/speedmqctl status`, `list_queues`, `plugins list`, `plugins disable amqp091` (désactivation à chaud, le port se ferme immédiatement) |
| Vérification de santé | `nc -z 127.0.0.1 15672` (le healthcheck est intégré à compose) |
| Sauvegarde et restauration | [docs/ops/backup-restore.md](ops/backup-restore.md) |
| Mise à niveau | [docs/ops/upgrade.md](ops/upgrade.md) |
| Base de sécurité | [docs/ops/security-baseline.md](ops/security-baseline.md) |

Configuration courante (exemple complet dans [configs/speedmqd.json](../../../configs/speedmqd.json), également surchargeable via les variables d'environnement `SPEEDMQ_*`) :

| Élément de configuration | Description | Défaut |
| --- | --- | --- |
| `data_dir` | Répertoire de données (messages + métadonnées), **à persister impérativement** | `data` |
| `listeners` | Adresses d'écoute de chaque protocole, TLS configurable | AMQP `:5672` / MQTT `:1883` |
| `management.addr` | Adresse d'écoute de l'UI d'administration / de l'API | `:15672` |
| `management.language` | Langue par défaut de l'UI d'administration ; si vide, sélection automatique selon le fuseau horaire du lieu de déploiement | Automatique |
| `storage.fsync` | Niveau d'écriture sur disque `none / os / batch / always` (détermine aussi le moment du confirm) | `os` |
| `storage.memory_high_watermark`, `storage.disk_free_limit` | Niveaux de ressources : en cas de déclenchement, les producteurs sont bloqués, **aucun message perdu** | `0.4` / 50 MiB |
| `users` | Table d'utilisateurs intégrée (mot de passe + étiquettes + `remote_access`) | `guest/guest` |
| `cluster.enabled` + `cluster.peers` | Cluster multi-nœuds (désactivé par défaut), modification des membres via `speedmqctl add_member` | Désactivé |

> Un port peut être déjà utilisé : changez-le simplement via `listeners` / `management.addr`.

***

## Structure du projet

```
speedmq/
├── cmd/
│   ├── speedmqd/        # point d'entrée du processus broker (c'est lui qu'il faut exécuter)
│   └── speedmqctl/      # CLI d'exploitation (passe par l'API HTTP d'administration, découplé de la version du noyau)
├── internal/            # implémentation du noyau
│   ├── protocol/        # plugins de protocole : amqp091, mqtt (encodage/décodage / méthodes / sessions)
│   ├── broker/          # noyau : vhost, exchanges, files, lettres mortes, contrôle de flux, vues de l'interface d'administration
│   ├── store/           # persistance : journal par segments, index de file, reprise après crash
│   ├── raft/ meta/      # cluster : Raft maison et réplication des métadonnées
│   ├── management/      # API HTTP d'administration + métriques Prometheus + service statique de l'UI intégrée
│   ├── transport/ auth/ config/ plugin/
├── pkg/                 # contrat stable public : API de plugin (plugin) et protocole filaire des plugins en processus externe (sidecar)
├── web/                 # projet front-end de l'UI d'administration (Vue 3 + Vite), les artefacts sont intégrés au binaire via go:embed lors du build
├── configs/             # configuration d'exemple
├── docs/ops/            # documentation d'exploitation : sauvegarde-restauration / mise à niveau / base de sécurité / surveillance
├── Dockerfile、docker-compose.yml
└── speedmq-logo.PNG、1280X1280.PNG (QR code du groupe d'échange)
```

***

## Contribution

Les Issues et Pull Requests sont les bienvenues. La raison d'être de ce projet est la **compatibilité protocolaire**, donc :

- Pour corriger un bug, précisez le comportement RabbitMQ correspondant (version, client, étapes de reproduction) ;
- Pour toute modification touchant aux détails du protocole, joignez le résultat de la comparaison avec RabbitMQ ;
- Avant de soumettre, assurez-vous que `go build ./...`, `go vet ./...`, `go test ./...`, `gofmt -l .` passent tous.

***

## Licence

Ce projet est distribué sous [Apache License 2.0](../../../LICENSE).

L'utilisation, la modification et la distribution (y compris commerciale) sont autorisées, à condition de conserver les mentions de copyright et de licence ; aucune garantie n'est fournie.

Copyright 2026 houzch (voir [NOTICE](../../../NOTICE))

***

## Remerciements

La spécification du protocole AMQP 0-9-1 et la sémantique comportementale de [RabbitMQ](https://www.rabbitmq.com/) servent de référence pour le travail de compatibilité de ce projet. Ce projet est une implémentation indépendante, sans lien d'affiliation avec l'équipe officielle RabbitMQ, et n'utilise pas son code.

***

## Rejoindre le groupe d'échange

Scannez le code pour rejoindre le groupe d'échange SpeedMQ ; vous pouvez y poser directement vos questions :

![Groupe d'échange SpeedMQ](../../../1280X1280.PNG)
