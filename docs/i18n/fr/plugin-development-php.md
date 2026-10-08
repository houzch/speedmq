# Guide de développement des plugins en processus externe pour SpeedMQ —— PHP

> **Public** : développeurs qui écrivent en PHP des plugins en processus externe (sidecar) pour SpeedMQ.
> **À lire d'abord** : [Guide de développement des plugins en processus externe (sidecar)](plugin-development.md) (modèle mental / champs de configuration / tableau complet du protocole filaire).
> **Projet d'exemple** : espace de travail `speedmq-plugin/php/sidecar_plugin.php` (bibliothèque standard uniquement, **aucune dépendance composer**).

---

## 1. À quoi cela ressemble une fois lancé

```
内核进程 ──dial(tcp 本机地址)──► 你的 PHP 进程（stream_socket_server 监听本机地址）
   ▲                                    │
   │  握手 / 心跳 / 控制面调用（JSON）      │
   │◄───────────────────────────────────┤  反向调用 core.authenticate / session.*（JSON）
   │      客户端字节流（原始字节，不 base64） │
   └────────────────────────────────────┘
```

Trois points essentiels : **votre processus est le serveur** (il attend que le noyau vienne se connecter) ; **le port externe est ouvert par le noyau** (`protocols[].listeners`) ;
**`prefix` doit être non vide** (un préfixe vide = ne pas participer au sniffing, la connexion ne vous sera pas confiée ; mesuré : elle est immédiatement coupée, ≤ 8 octets ASCII).

---

## 2. Démarrage en trois étapes

### Étape 1 : configuration

`speedmqd.json` (**la configuration réelle est du JSON standard, sans commentaires possibles**) :

```json
{
  "plugins": {
    "php-sidecar": {
      "builtin": false,
      "enabled": true,
      "sidecar": {
        "address": "tcp://127.0.0.1:19021",
        "spawn": ["php", "/opt/speedmq/sidecar_plugin.php", "--addr", "0.0.0.0:19021", "--name", "php-sidecar"],
        "protocols": [
          { "name": "phpecho", "prefix": "PH",
            "listeners": [{ "name": "phpecho", "addr": ":19022" }] }
        ]
      }
    }
  }
}
```

### Étape 2 : lancer

```bash
php sidecar_plugin.php --addr 0.0.0.0:19021 --name php-sidecar --session-demo
```

### Étape 3 : vérifier

```bash
php -l sidecar_plugin.php                                  # 先过语法检查
curl -u guest:guest http://127.0.0.1:15672/api/plugins      # state 应为 enabled
printf 'PHhello\n' | nc 127.0.0.1 19022                     # 应回显 PHhello
```

---

## 3. Points d'implémentation

### 3.1 Découpage en trames

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)（包含 kind 字节）；单帧上限 16 MiB。
```

PHP utilise `pack`/`unpack` :

```php
// 写：pack('N', …) 就是大端 u32
fwrite($sock, pack('N', 1 + strlen($payload)) . chr($kind) . $payload);

// 读：先读 5 字节头，再读 payload
$head = read_exact($sock, 5);
$len  = unpack('N', substr($head, 0, 4))[1];
$kind = ord($head[4]);
$payload = read_exact($sock, $len - 1);
```

Charge utile d'une trame de données = `pack('N', $streamId) . octets bruts`.

### 3.2 Poignée de main et heartbeat

Le noyau **envoie d'abord Hello**, vous répondez `HelloAck` ; le noyau vérifie `name` et `api_version` (actuellement `v1`).
Ensuite, un `Ping` toutes les 2 s, répondez `Pong`.

### 3.3 Modèle de concurrence : pompe à trames réentrante (PHP n'a pas de threads)

Le CLI PHP est mono-thread et bloquant ; on n'utilise donc pas ici « un thread par flux », mais plutôt :

- la **boucle de lecture** (`serve()`) gère la poignée de main, le heartbeat, l'ouverture de flux, l'écho des données et le traitement des appels directs ;
- l'**écho** n'a pas besoin de machine à états supplémentaire : dès réception d'un `kindData`, réécrivez-le tel quel en `kindData` ;
- les **appels inverses** utilisent `callAndWait()` : après l'envoi d'un `kindCall`, on lit et distribue les trames
  jusqu'à lire **sa propre** réponse (avec `reverse=true` et `id` concordants), puis on renvoie.

```php
public function callAndWait(string $method, $params = null)
{
    $id = ++$this->nextId;
    $this->sendJson(KIND_CALL, ['id' => $id, 'method' => $method, 'reverse' => true, 'params' => $params]);
    for (;;) {
        [$kind, $payload] = $this->readFrame();
        if ($kind === KIND_REPLY) {
            $reply = json_decode($payload, true);
            if (!empty($reply['reverse']) && $reply['id'] === $id) {
                if (empty($reply['ok'])) throw new RuntimeException($reply['error'] ?? '调用失败');
                return $reply['data'] ?? null;
            }
            continue;
        }
        $this->dispatchOther($kind, $payload);   // 心跳/数据/正向调用照常处理
    }
}
```

Cela signifie que `dispatchOther()` **doit être réentrante** : elle peut être rappelée à l'intérieur d'un `callAndWait`
(par exemple, lors du traitement d'un `session.deliver`, il faut à nouveau `session.settle`). C'est ce que fait l'exemple.

### 3.4 Pont sémantique (authentification obligatoire d'abord)

```
core.authenticate → session.open → session.declare_queue / publish / consume / …
```

`core.authenticate` n'est pas optionnel : la surface d'opérations du noyau pour la connexion n'a pas d'identité avant l'authentification, et un `session.open` direct est refusé
(`ACCESS_REFUSED - access to vhost '/' refused for user ''`).

```php
$ident = $this->callAndWait('core.authenticate', [
    'stream'    => $streamId,
    'mechanism' => 'PLAIN',
    // SASL PLAIN 响应：\x00<user>\x00<password>，字节在 JSON 里走 base64
    'response'  => base64_encode("\x00{$user}\x00{$password}"),
]);
$this->callAndWait('session.open', ['stream' => $streamId, 'vhost' => '/']);
$q = $this->callAndWait('session.declare_queue', ['stream' => $streamId, 'exclusive' => true, 'auto_delete' => true]);
$this->callAndWait('session.consume', ['stream' => $streamId, 'queue' => $q['name'], 'prefetch' => 32]);
```

Les livraisons sont **renvoyées en sens direct** par le noyau (`method = "session.deliver"`), puis réglées avec `session.settle`
(`ack` / `requeue` / `reject` ; le numéro de livraison est unique globalement, sans numéro de flux).

---

## 4. Lecture guidée du code (projet d'exemple)

`speedmq-plugin/php/sidecar_plugin.php` compte environ 320 lignes :

| Emplacement | Rôle |
| --- | --- |
| `read_exact()` / `Conn::readFrame()` / `sendFrame()` | lecture/écriture des trames |
| `Conn::serve()` | boucle de lecture principale |
| `Conn::dispatchOther()` | distribution des trames hors poignée de main (réentrante) |
| `Conn::callAndWait()` | appel inverse (pompe à trames réentrante) |
| `Conn::handleHello()` | vérifie et répond HelloAck |
| `Conn::handleForwardCall()` / `handleMethod()` | appels directs (`session.deliver` + settle, `stats`) |
| `Conn::sessionDemo()` | authentification + déclaration + publication + consommation |

---

## 5. Mesures réelles (reproduction locale)

Windows + PHP 7.4 ; le noyau dans Docker (`speedmq:1.1.01`), le plugin sur la machine hôte (`tcp://host.docker.internal:19021`).

```
php -l sidecar_plugin.php  → No syntax errors detected
plugin=php-sidecar state=enabled          # /api/plugins
echo=[PHhello]                            # 客户端连内核端口 19022 发 "PHhello\n"

--- 插件 stdout ---
sidecar 已启动 plugin=php-sidecar addr=0.0.0.0:19021 version=0.1.0
内核已接入 plugin=php-sidecar peer=127.0.0.1:51006
握手完成 plugin=php-sidecar kernel=1.1.01
认证通过 user=guest
session 演示完成 queue=amq.gen-955afebb58d6b5307ba36e
流已打开 plugin=php-sidecar stream=1 remote=172.17.0.1:59664 local=172.17.0.2:19022
收到投递（session.deliver） queue=amq.gen-955afebb58d6b5307ba36e delivery_id=1 body=hello from php sidecar
```

Couverte : **poignée de main → authentification → pont sémantique → renvoi de livraison → règlement → écho du flux d'octets**.

---

## 6. Points d'attention spécifiques à PHP

- **PHP 7.4 n'a pas de type de retour `mixed`** (disponible seulement depuis PHP 8.0) : dans l'exemple, l'appel inverse renvoie « un type quelconque »,
  on **ne déclare donc pas de type de retour** (on utilise le commentaire `@return mixed`). Sur 7.4, écrire `: mixed` provoque directement une erreur de syntaxe.
- **Types numériques de JSON** : `json_decode($s, true)` décode par défaut les entiers en `int`, mais un grand entier peut devenir `float` ;
  le numéro de livraison ne pose pas de problème à l'échelle de cet exemple, mais si vos numéros sont très grands, envisagez `JSON_BIGINT_AS_STRING`.
- **base64 est indispensable** : `message.body` et `core.authenticate.response` sont des chaînes base64 dans le JSON
  (`base64_encode` / `base64_decode($s, true)`).
- **N'utilisez pas `pcntl_fork` pour la concurrence** : pcntl n'existe pas sous Windows, et après un fork l'hypothèse « un seul écrivain par connexion » est brisée ;
  le mono-thread + pompe à trames réentrante suffit déjà (sauf si vous devez faire de très gros calculs sur le flux, mieux vaut alors les déporter dans un service externe).
- **`stream_socket_accept` est bloquant** : le cycle de vie du processus est géré par le noyau (`spawn`) ou un superviseur ;
  pensez à traiter le retour `''` de `fread` (EOF) → terminer cette connexion et revenir à accept.
- **Tampon de sortie** : utilisez `fwrite(STDOUT, …)` pour les journaux, suivi d'un saut de ligne, pour faciliter leur relais ligne par ligne par le noyau dans le journal du noyau.

---

## 7. Pour aller plus loin

- Le plugin embarque sa propre interface d'administration : ajoutez `console_url` dans la configuration (document principal §5.8), la page « Gestion des plugins » de la console d'administration fera apparaître une entrée directe.
- Déploiement autonome : `spawn: []` + `address: "tcp://<nom du service>:19021"`, écoute sur `0.0.0.0` dans le conteneur.
- Pour une concurrence plus élevée, vous pouvez transformer le plugin en un résident de type Swoole / RoadRunner, mais **le protocole filaire ne change pas** : il faut seulement garantir :
  écriture sérialisée des trames, boucle de lecture non bloquante, appariement des appels inverses par `id` + `reverse`.
