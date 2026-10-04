# Guide de développement des plugins en processus externe (sidecar) pour SwiftMQ

> **Public** : développeurs qui ne veulent pas forker / recompiler le noyau et souhaitent étendre SwiftMQ dans **n'importe quel langage**.
> **Périmètre** : ce document ne traite que d'une seule forme de plugin — le **plugin en processus externe** (terme du noyau : `sidecar`). Les plugins de protocole intégrés au noyau (AMQP 0-9-1 / MQTT) ne sont pas couverts.
> **Comment lire** : les sections 1–2 posent le modèle mental, la section 3 montre le code, et **la section 5 explique « une fois le développement terminé, comment le brancher pour qu'il tourne avec le reste et serve les clients »** ;
> **pour les autres langages (Python / Node.js / PHP / Java), voir les guides par langage du §4** (chacun fournit un projet d'exemple complet et vérifié en conditions réelles).
> Le code de ce document est un squelette minimal exécutable, à copier comme point de départ. Le chinois simplifié est la langue source.

---

## 1. De quoi s'agit-il

Un **processus autonome** qui implémente un certain « protocole » (analyse le flux d'octets des clients) dans son propre processus.
Le noyau l'héberge selon la configuration : **les ports sont ouverts par le noyau, les connexions sont proxifiées par le noyau**, et l'enregistrement / le démarrage-arrêt / l'audit / l'isolation réutilisent tous les mécanismes existants du noyau.

Établissons d'abord trois modèles mentaux corrects (les points les plus faciles à confondre) :

1. **Le processus plugin est un « service local »** : il n'écoute qu'une **adresse locale** (TCP ou socket unix) et attend que **le noyau vienne se connecter**.
   Le sens de la connexion est **noyau (client) → plugin (serveur)**, et c'est aussi le noyau qui envoie la poignée de main en premier.
2. **Le port métier externe n'est pas ouvert par le plugin** : il est créé par **le noyau** selon `protocols[].listeners` de la configuration, puis exposé aux clients.
   Le client se connecte au **port du noyau**, et les octets sont proxifiés par le noyau vers le processus plugin. Le processus plugin **n'a pas besoin** d'ouvrir lui-même un port métier.
3. **La sémantique est optionnelle** : le plugin peut se contenter de « transporter des octets » (le protocole est entièrement à votre charge),
   ou atteindre la sémantique du noyau (files / routage / permissions / accusés) via des **appels inverses** `session.*`,
   avec **exactement la même sémantique** que les plugins de protocole intégrés (vhost, permissions, routage et comportement des files ne divergent donc pas).

| Avantages | Coûts |
| --- | --- |
| Étendre sans modifier ni recompiler le noyau | Une copie d'octets locale supplémentaire sur le plan de données (proxy du noyau, pas de passage de fd, comportement identique multiplateforme) |
| Implémentation dans n'importe quel langage (il suffit de savoir implémenter le protocole filaire) | Un RPC local supplémentaire par appel inverse (encodage/décodage JSON + copie) |
| Le plugin peut être publié / mis à niveau / redémarré indépendamment | Le sniffing reste côté noyau : il ne peut être reconnu que par « préfixe » ou « port dédié » |
| Un crash n'affecte que ce plugin : le noyau le marque `down` sans quitter ni planter | Seule la capacité `net.listen` prend réellement effet ; les autres valeurs de capacité sont réservées (voir §7) |

---

## 2. Principe de fonctionnement

Établissement de la connexion (**le noyau est le client, le plugin est le serveur**) :

```
内核：读配置 plugins.<名>.sidecar
      ├─（可选）spawn 拉起子进程
      └─ dial(address)
插件：sidecar.NewServer 监听 address（Accept）
```

Poignée de main :

```
内核 ──Hello(kind=1)─────►  插件 Handler.Hello
内核 ◄─HelloAck(kind=2)──   插件（拒绝接入时回 error，内核据此隔离该插件）
```

Début du service :

```
内核：按配置 protocols 注册协议 + 创建对外监听（对外端口由内核打开）

客户端 ─► 内核端口 ─嗅探(prefix)─► 建流 Open ─► 插件 Handler.Open(Stream)
                                                  │
  插件 ─反向调用 session.*（BridgeFromContext）─► 内核（队列 / 路由 / 权限 / 确认）
  内核 ─投递回推 session.deliver ──────────────► 插件 Handler.Call
  插件 ─结算 session.settle ───────────────────► 内核
```

**Cycle de vie** (hôte `internal/plugin/sidecar` côté noyau) :

```
Load → Init：spawn(可选) → dial(address) → 握手 → 按配置注册 protocols（此时才起对外监听）
     → Start：起监督协程（断线重连 / 退避）
     → [运行期] 断线 → 状态 down → 退避重连（restart=always）或等待运维（restart=never）
     → Stop（内核退出）：断开连接、回收桥上会话与未结算投递、终止由内核拉起的子进程
```

**Sémantique des états** (visible via `swiftmqctl plugins show`) :

| État | Signification | Action d'exploitation |
| --- | --- | --- |
| `enabled` | Connecté et en service | — |
| `failed` | **N'a jamais démarré** (configuration erronée / poignée de main refusée / processus impossible à lancer) | Consultez `RuntimeNote` et le journal du noyau, corrigez la configuration ou le plugin ; le noyau ne réessaie qu'après un redémarrage |
| `down` | **A déjà démarré, mais n'est plus là** (processus planté / connexion coupée) | Allez relancer le processus plugin ; le noyau se rétablit automatiquement selon la politique `restart` |
| `disabled` | `enabled=false` dans la configuration, ou désactivation à chaud par l'exploitant | Rétablir avec `plugins enable <nom>` |

---

## 3. Développement (Go)

### 3.1 Créer le projet

Le plugin est un **module Go autonome** qui ne dépend que de deux paquets de contrat publics :

- `github.com/houzch/swiftmq/pkg/sidecar` — le protocole filaire et l'implémentation côté plugin (**obligatoire**)
- `github.com/houzch/swiftmq/pkg/plugin` — uniquement si vous avez besoin de types tels que `plugin.Message` / `plugin.Error` (facultatif)

```
my-sidecar/
├── go.mod          # module my-sidecar；require github.com/houzch/swiftmq（或 replace 指到本地源码）
├── main.go         # 启动 sidecar.Server
└── handler.go      # 实现 sidecar.Handler
```

```bash
go mod init my-sidecar
go get github.com/houzch/swiftmq@v1.1.02
# 本地联调时可改用 replace 指向源码：
#   go mod edit -replace github.com/houzch/swiftmq=../swiftmq
```

> Pour le débogage conjoint avec `replace`, le plugin et le noyau doivent utiliser **la même copie des sources** ; sinon, la version d'API (`v1`) peut concorder alors que les types diffèrent.

### 3.2 Implémenter `Handler` (trois méthodes)

Toute la surface métier du processus plugin se résume à `Hello` / `Call` / `Open` de `sidecar.Handler`.
La poignée de main, le heartbeat, le multiplexage et le découpage en blocs sont gérés par `pkg/sidecar` ; vous n'avez jamais à manipuler les trames.

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/houzch/swiftmq/pkg/sidecar"
)

const (
	pluginName = "my-sidecar" // 必须与内核配置里的插件名一致
	version    = "0.1.0"
	apiVersion = "v1" // 必须等于 sidecar / plugin 的 APIVersion
	protocol   = "myproto" // 应与配置里 protocols[].name 一致
)

func main() {
	addr := ":19001" // 内核来连的本机地址；可用 -addr 覆盖
	handler := &handler{}

	srv, err := sidecar.NewServer(handler, sidecar.ServerOptions{
		Address: "tcp://" + addr,
		Logger:  stdLogger{log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "my-sidecar 监听失败: %v\n", err)
		os.Exit(1)
	}
	// Address() 能读回实际地址（配置里写 :0 时有用）。
	log.Printf("my-sidecar 已启动 name=%s version=%s addr=%s", pluginName, version, srv.Address())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := srv.Serve(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "my-sidecar 服务退出: %v\n", err)
		os.Exit(1)
	}
}

type handler struct {
	streams atomic.Int64
}

var _ sidecar.Handler = (*handler)(nil)

// Hello 处理握手：返回的 HelloAck 发给内核。返回 error 即拒绝接入（内核会明确隔离该插件）。
func (h *handler) Hello(_ context.Context, hello sidecar.Hello) (sidecar.HelloAck, error) {
	// 内核版本过旧时可以拒绝，避免带着不兼容跑起来。
	if hello.Plugin != pluginName {
		return sidecar.HelloAck{}, fmt.Errorf("插件名不匹配：内核声明 %q，本插件是 %q", hello.Plugin, pluginName)
	}
	if hello.APIVersion != apiVersion {
		return sidecar.HelloAck{}, fmt.Errorf("插件 API 版本不匹配：内核 %q，插件 %q", hello.APIVersion, apiVersion)
	}
	return sidecar.HelloAck{
		Name:         pluginName,
		Version:      version,
		APIVersion:   apiVersion,
		Capabilities: []string{"net.listen"},
		Protocols:    []string{protocol}, // 展示用；真正生效的是内核配置里的 protocols
		Methods:      []string{"stats"},  // 展示用；内核不提供通用调用入口
	}, nil
}

// Call 处理方法调用。约定的系统方法 session.deliver 是"内核把消费投递回推给插件"，
// 必须在实现里处理（见 §3.4）。
func (h *handler) Call(ctx context.Context, method string, params json.RawMessage) (any, error) {
	switch method {
	case sidecar.MethodSessionDeliver:
		return h.handleDeliver(ctx, params)
	case "stats":
		return map[string]any{"streams": h.streams.Load()}, nil
	default:
		// 未知方法必须明确报错：静默成功会让调用方以为生效了。
		return nil, fmt.Errorf("未知方法 %q", method)
	}
}

// Open 处理一条新打开的流：一条流 = 内核侧的一条客户端连接。
// 通常**阻塞处理到流结束**再返回；返回后该流即结束（内核会关闭对应的客户端连接）。
func (h *handler) Open(ctx context.Context, stream *sidecar.Stream, meta sidecar.Open) error {
	h.streams.Add(1)
	// meta.Remote / meta.Local 是两端地址，meta.Peek 是嗅探阶段读到的前缀字节，可用于更细的分支判断。
	return h.serve(stream)
}

// stdLogger 把 pkg/sidecar 的最小日志接口接到标准库日志。
type stdLogger struct{ l *log.Logger }

func (s stdLogger) Info(msg string, args ...any) {
	s.l.Printf("%s %s", msg, strings.TrimRight(fmt.Sprintln(args...), "\n"))
}
func (s stdLogger) Warn(msg string, args ...any) {
	s.l.Printf("WARN %s %s", msg, strings.TrimRight(fmt.Sprintln(args...), "\n"))
}
```

### 3.3 Plan de données : lire et écrire un `Stream`

`sidecar.Stream` implémente `io.ReadWriteCloser` ; traitez-le simplement comme « une connexion » :

```go
func (h *handler) serve(stream *sidecar.Stream) error {
	buf := make([]byte, 4096)
	for {
		n, err := stream.Read(buf)
		if n > 0 {
			// 你的协议解析在这里；示例先原样回显。
			if _, werr := stream.Write(buf[:n]); werr != nil {
				return werr
			}
		}
		if err != nil {
			return nil // 对端关闭
		}
	}
}
```

Points clés :

- Chaque connexion client = un flux ; le plugin peut commencer à émettre/recevoir immédiatement dans `Handler.Open`.
- Les gros messages sont automatiquement découpés en blocs par la bibliothèque (chaque trame ≤ 64 KiB), et **l'empreinte mémoire est indépendante de la taille du message**.
- Contre-pression : le tampon de réception d'un flux a une borne supérieure ; quand le tampon est plein, la **goroutine de distribution** de cette connexion est bloquée (tous les flux attendent ensemble) —
  c'est l'arbitrage entre « mémoire prévisible » et « limitation de débit par flux » ; voir §7 pour les détails.

### 3.4 Utiliser le pont sémantique du noyau (`session.*`)

Pour que le plugin réutilise la sémantique de files / routage / permissions / accusés du noyau (au lieu d'en construire une), utilisez les **appels inverses**.
Sur un flux, utilisez-les dans l'ordre `session.open` → autres `session.*` → (`session.close`) :

```go
import (
	"errors"

	"github.com/houzch/swiftmq/pkg/plugin"
	"github.com/houzch/swiftmq/pkg/sidecar"
)

func (h *handler) runDemo(ctx context.Context, stream *sidecar.Stream) error {
	br, ok := sidecar.BridgeFromContext(ctx) // 当前流的内核桥（内核在建流时注入 ctx）
	if !ok {
		return errors.New("ctx 中没有内核桥")
	}
	streamID := stream.ID()

	// 1) 认证：连接的内核操作面在认证前没有身份，会话一定打不开。
	//    response 就是你自己协议里的凭据（这里以 SASL PLAIN 为例）。
	plain := append([]byte("\x00guest\x00"), []byte("guest")...)
	var ident sidecar.AuthIdentityDTO
	if err := br.Call(ctx, sidecar.MethodCoreAuthenticate, sidecar.CoreAuthenticateParams{
		Stream: streamID, Mechanism: "PLAIN", Response: plain,
	}, &ident); err != nil {
		return err
	}

	// 2) 打开会话（内核会做与内置协议插件相同的权限校验）
	if err := br.Call(ctx, sidecar.MethodSessionOpen,
		sidecar.SessionOpenParams{Stream: streamID, VHost: "/"}, nil); err != nil {
		return err
	}

	// 3) 声明一个临时队列
	var q sidecar.QueueInfoResult
	if err := br.Call(ctx, sidecar.MethodSessionDeclareQueue, sidecar.QueueDeclareParams{
		Stream: streamID, Exclusive: true, AutoDelete: true,
	}, &q); err != nil {
		return err
	}

	// 4) 发布一条消息（持久化等待在内核应答前已完成：调用返回即已按 fsync 档位落盘）
	if err := br.Call(ctx, sidecar.MethodSessionPublish, sidecar.PublishParams{
		Stream: streamID, RoutingKey: q.Name,
		Message: sidecar.MessageDTO{Body: []byte("hello")},
	}, nil); err != nil {
		return err
	}

	// 5) 注册消费者；投递随后以正向调用 session.deliver 到达 Handler.Call
	var c sidecar.ConsumeResult
	return br.Call(ctx, sidecar.MethodSessionConsume, sidecar.ConsumeParams{
		Stream: streamID, Queue: q.Name, Prefetch: 32,
	}, &c)
}

// 处理内核回推的投递并结算
func (h *handler) handleDeliver(ctx context.Context, params json.RawMessage) (any, error) {
	var p sidecar.DeliverParams
	if err := json.Unmarshal(params, &p); err != nil {
		return nil, err
	}
	br, ok := sidecar.BridgeFromContext(ctx)
	if !ok {
		return nil, errors.New("ctx 中没有内核桥")
	}
	// 结算不需要流号：投递编号在整条连接上全局唯一。
	// 动作三选一：Ack（消费完成）/ Requeue（重新入队）/ Reject（丢弃，可能进死信）。
	return nil, br.Call(ctx, sidecar.MethodSessionSettle, sidecar.SettleParams{
		DeliveryID: p.DeliveryID, Action: sidecar.SettleActionAck,
	}, nil)
}
```

**Aperçu des méthodes d'appel inverse** (constantes de `pkg/sidecar/bridge.go` → noms filaires) :

| Groupe | Nom filaire (constante) | Description |
| --- | --- | --- |
| Authentification | `core.authenticate`（`MethodCoreAuthenticate`） | **À faire en premier** : remettre au noyau les identifiants extraits de votre protocole pour vérification ; paramètres `{stream, mechanism, response}`, renvoie `{user}` |
| Session | `session.open`（`MethodSessionOpen`） | Ouvrir une session pour un vhost sur le flux ; possible **seulement après l'authentification** |
| | `session.close`（`MethodSessionClose`） | Libérer la session sur le flux (annuler les consommateurs, supprimer les files exclusives) |
| Exchange | `session.declare_exchange` / `session.delete_exchange` | Créer / supprimer (déclaration passive d'un objet inexistant → `KindNotFound`) |
| | `session.bind_exchange` / `session.unbind_exchange` | Liaisons exchange-vers-exchange |
| File | `session.declare_queue` / `session.delete_queue` | Créer / supprimer ; le serveur en génère un quand `name` est vide |
| | `session.bind_queue` / `session.unbind_queue` | Liaisons file-vers-exchange |
| | `session.purge_queue` | Vider les messages prêts (hors non acquittés) |
| Publication | `session.publish` | Renvoie `{routed, rejected}` ; la persistance est terminée avant la réponse |
| Récupération | `session.get` | Récupérer activement un message ; `found=false` signifie que la file est vide |
| Consommation | `session.consume` / `session.cancel` | Enregistrer / annuler des consommateurs |
| Règlement | `session.settle` | Régler une livraison (`ack` / `requeue` / `reject`) |
| **Direct** | `session.deliver` | **Noyau → plugin** : renvoyer une livraison (à traiter dans votre `Handler.Call`) |

**Quatre conventions à respecter impérativement** :

1. **`core.authenticate` d'abord** : la surface d'opérations du noyau pour la connexion n'a pas d'identité avant l'authentification ;
   à ce moment-là `session.open` est refusé (`ACCESS_REFUSED - access to vhost '/' refused for user ''`).
   Le plugin est responsable de l'extraction des identifiants depuis son propre protocole ; la logique d'authentification et la table des utilisateurs restent dans le noyau, le plugin ne touche jamais au magasin de mots de passe.
2. **`session.open` ensuite** : appeler d'autres méthodes sans session ouverte fait renvoyer par le noyau `KindPreconditionFailed` ("le flux N n'a pas encore ouvert de session").
3. **Régler chaque livraison exactement une fois** : choisir entre `Ack` / `Requeue` / `Reject`.
   `Ack` et `Reject` rejettent tous deux le message, mais **seul `Reject` passe par la lettre morte**.
4. **Les livraisons non réglées ne sont pas perdues** : lorsque le flux se termine (déconnexion du client / retour de `Handler.Open`) ou que la connexion du plugin tombe,
   le noyau traite toutes les livraisons non réglées **comme « remise en file »**, évitant que les messages ne stagnent.

**Restauration des erreurs** : l'erreur `*plugin.Error` du noyau arrive via le pont sous forme de `*sidecar.RPCError` (champs `Kind` / `Text`),
et peut être restaurée en un `plugin.Error` catégorisé plutôt que de perdre la catégorie dans une chaîne :

```go
if err := br.Call(ctx, sidecar.MethodSessionPublish, params, nil); err != nil {
	var rpc *sidecar.RPCError
	if errors.As(err, &rpc) {
		return plugin.Errorf(plugin.ErrorKind(rpc.Kind), "%s", rpc.Text)
	}
	return err
}
```

Correspondance des `Kind` courants avec les protocoles intégrés (pour vous aider à décider comment renvoyer l'erreur au client) :

| `plugin.ErrorKind` | Sémantique | Correspondance AMQP 0-9-1 |
| --- | --- | --- |
| `KindNotFound` | L'objet n'existe pas | 404 NOT_FOUND (ferme le Channel) |
| `KindPreconditionFailed` | Paramètres incohérents avec un objet existant / session non ouverte | 406 PRECONDITION_FAILED (ferme le Channel) |
| `KindAccessRefused` | Permissions insuffisantes / nom réservé | 403 ACCESS_REFUSED (ferme le Channel) |
| `KindResourceLocked` | Ressource exclusive occupée | 405 RESOURCE_LOCKED (ferme le Channel) |
| `KindInvalidPath` | vhost inexistant | 402 INVALID_PATH (ferme la connexion) |
| `KindNotImplemented` | Capacité non implémentée | 540 NOT_IMPLEMENTED (ferme la connexion) |
| `KindInternal` | Erreur interne du noyau | 541 INTERNAL_ERROR (ferme la connexion) |

**Limite de fidélité des types de message** : la table de propriétés (`MessageDTO.Properties.Headers`) transite par JSON,
donc **l'information de type numérique distinguant `int32` / `double` comme dans une field-table AMQP n'est pas disponible**.
Quand une fidélité stricte des types est requise, portez vous-même cette information dans le corps du message (octets bruts).

### 3.5 État et journalisation côté plugin

- L'**état d'un plugin externe est déterminé par le fait que la connexion soit vivante ou non**, le plugin n'a rien à déclarer lui-même (le `StateReporter` des plugins intégrés ne s'applique pas aux processus externes).
- Journalisation : `sidecar.ServerOptions.Logger` écrit sur stdout/stderr du processus plugin ;
  **lorsqu'il est lancé par le noyau via `spawn`, ces sorties sont relayées par le noyau dans le journal du noyau** (avec l'étiquette `plugin`), ce qui facilite la collecte centralisée.
- En déploiement autonome (hors spawn), collectez les journaux du plugin à votre façon.

### 3.6 Se tester sans le noyau

`Handler` est une interface Go ordinaire ; dans un test unitaire, il suffit de l'instancier directement et d'appeler `Hello` / `Call` / `Open` pour couvrir la logique métier, sans démarrer de réseau :

```go
func TestHelloRejectsWrongName(t *testing.T) {
	h := &handler{}
	if _, err := h.Hello(context.Background(), sidecar.Hello{Plugin: "other", APIVersion: "v1"}); err == nil {
		t.Fatal("插件名不一致时应当拒绝")
	}
}
```

La vérification de bout en bout est décrite au §5.

---

## 4. Développer dans d'autres langages (spécification du protocole filaire)

`pkg/sidecar` est un contrat public sans dépendance ; le protocole filaire lui-même est très simple et peut être implémenté dans n'importe quel langage.
Pour vous interfacer, vous devez implémenter les conventions « au niveau des octets » ci-dessous (sources : `pkg/sidecar/frame.go`, `proto.go`).

> **Des guides par langage avec projet d'exemple complet sont fournis** (les exemples ont tous été vérifiés en conditions réelles : poignée de main → authentification → pont sémantique → livraison/règlement → flux d'octets) :
>
> | Langage | Guide | Projet d'exemple (espace de travail `swiftmq-plugin/`) |
> | --- | --- | --- |
> | Python | [plugin-development-python.md](plugin-development-python.md) | `python/sidecar_plugin.py` (bibliothèque standard uniquement) |
> | Node.js | [plugin-development-nodejs.md](plugin-development-nodejs.md) | `nodejs/index.js` (bibliothèque standard uniquement) |
> | PHP | [plugin-development-php.md](plugin-development-php.md) | `php/sidecar_plugin.php` (bibliothèque standard uniquement) |
> | Java | [plugin-development-java.md](plugin-development-java.md) | `java/SidecarPlugin.java` (fichier unique, JDK uniquement) |
>
> Pour l'implémentation de référence complète en Go, voir le projet de test autonome `swiftmq-test/test/integration/echosidecar/` (il utilise directement
> `pkg/sidecar.Server`, inutile donc de vous soucier des détails du niveau octet ci-dessous).

**Format de trame** (identique pour toutes les trames) :

```
+--------+--------+------------------+
| len    | kind   | payload          |
| u32 BE | u8     | (len-1) 字节      |
+--------+--------+------------------+
len = 1 + len(payload)，即长度字段**包含** kind 字节；单帧上限 16 MiB。
```

**Types de trame `kind`** :

| kind | Nom | Sens | Charge utile |
| --- | --- | --- | --- |
| 1 | Hello | noyau → plugin | JSON `Hello` |
| 2 | HelloAck | plugin → noyau | JSON `HelloAck` |
| 3 | Ping | noyau → plugin | vide |
| 4 | Pong | plugin → noyau | vide |
| 5 | Call | bidirectionnel | JSON `Call` |
| 6 | Reply | bidirectionnel | JSON `Reply` |
| 7 | Open | noyau → plugin | JSON `Open` |
| 8 | OpenAck | plugin → noyau | JSON `OpenAck` |
| 9 | Data | bidirectionnel | `u32 BE stream` + octets bruts |
| 10 | Close | bidirectionnel | JSON `Close` |

**Structures JSON du plan de contrôle** (les noms de champs correspondent à `proto.go`).

> Ce passage est un **exemple de message du protocole filaire** (plusieurs messages sont donnés dans l'ordre dans un même bloc, d'où les séparateurs `//` pour l'explication),
> **ce n'est pas une configuration que l'on peut écrire directement dans `swiftmqd.json`**.

```jsonc
// Hello（内核 → 插件）
{ "plugin": "my-sidecar", "protocol_version": "v1", "kernel_version": "1.1.01", "api_version": "v1" }
// HelloAck（插件 → 内核）；deny 非空表示拒绝服务
{ "name": "my-sidecar", "version": "0.1.0", "api_version": "v1",
  "capabilities": ["net.listen"], "protocols": ["myproto"], "methods": ["stats"], "deny": "" }
// Call / Reply：方向字段 reverse 区分"内核→插件"（false）与"插件→内核"（true）
{ "id": 1, "method": "session.open", "reverse": false, "params": { } }
{ "id": 1, "reverse": false, "ok": true, "error": "", "data": { } }
// Open（内核 → 插件）／OpenAck（插件 → 内核）／Close（双向）
{ "stream": 7, "remote": "1.2.3.4:5000", "local": "0.0.0.0:19002", "peek": "" }
{ "stream": 7, "ok": true, "error": "" }
{ "stream": 7, "reason": "closed by peer" }
```

**Sémantique à respecter impérativement** :

- **Poignée de main** : le noyau envoie `Hello` en premier, et le plugin doit répondre par une trame `HelloAck`.
  Le noyau vérifie que `HelloAck.name == le nom du plugin dans la configuration` et que `HelloAck.api_version == l'APIVersion du noyau` ;
  un `deny` non vide est considéré comme un refus de connexion (le plugin est isolé).
- **Heartbeat** : par défaut, le noyau envoie un `Ping` toutes les 2 s, et le plugin doit répondre `Pong` dans les 8 s ; côté plugin, si aucune trame n'est reçue pendant 24 s, la connexion peut être fermée de son propre chef.
- **Deux espaces d'ID** : le champ `reverse` de `Call` distingue le sens, et chaque sens s'incrémente à partir de 1 indépendamment ;
  `Reply` **doit donc renvoyer `reverse`**, sinon la réponse sera délivrée au mauvais demandeur.
- **Pas de base64 sur le plan de données** : les gros blocs tels que le corps des messages vont directement dans la charge utile de la trame `Data` (`stream` + octets bruts), découpés en blocs si nécessaire.

> Si vous utilisez Go, prenez directement `pkg/sidecar` ; aucun des détails ci-dessus n'est à implémenter vous-même.

---

## 5. L'intégrer pour qu'il tourne avec le reste : intégration, service aux clients, empaquetage ★

Cette section répond à « une fois le développement terminé, comment l'intégrer dans SwiftMQ et comment servir les clients ».

### 5.1 Déclarer le plugin dans la configuration

Un plugin externe est **entièrement géré par la configuration**, le noyau n'a besoin d'aucune modification de code pour lui. Ajoutez une entrée à la section `plugins` de `swiftmqd.json`
(**la configuration réelle est du JSON standard, sans commentaires possibles**) :

```json
{
  "listeners": {
    "myproto": [{ "addr": ":19002" }]
  },
  "plugins": {
    "my-sidecar": {
      "builtin": false,
      "enabled": true,
      "required": false,
      "sidecar": {
        "address": "tcp://127.0.0.1:19001",
        "spawn": ["/usr/local/bin/my-sidecar", "-addr", "tcp://127.0.0.1:19001"],
        "restart": "always",
        "protocols": [
          {
            "name": "myproto",
            "prefix": "MP",
            "listeners": [{ "name": "myproto", "addr": ":19002" }]
          }
        ]
      }
    }
  }
}
```

Explication point par point (liste des champs dans le tableau ci-dessous) :

- Le nom de clé de `plugins.<nom du plugin>` **doit correspondre au `HelloAck.name` déclaré par le plugin**, sinon la poignée de main est refusée.
- `builtin: false` : déclare explicitement un plugin externe (sinon il est affiché comme intégré par le plan de gestion).
- `enabled` : le désactiver = ne pas lancer de processus, ne pas créer d'écouteur.
- Avec `required: true`, un échec au démarrage bloque le démarrage du noyau — ne l'activez pas pour un plugin externe.
- `address` est **l'adresse à laquelle le noyau se connecte** (le noyau est le client) ; quand `spawn` n'est pas vide, le noyau lance le processus à votre place.
- `protocols[].prefix` **doit être non vide** (règles de sniffing au §5.3).
- `listeners` sont les **ports externes de ce protocole, ouverts par le noyau** (les clients se connectent au noyau).

Liste des champs :

| Champ | Obligatoire | Description |
| --- | --- | --- |
| `sidecar.address` | ✅ | Adresse du processus plugin : `tcp://host:port` ou `unix:///path` |
| `sidecar.spawn` | ✕ | Ligne de commande que le noyau lance à votre place (le premier élément est l'exécutable) ; **vide = le noyau se contente de se connecter sans lancer**, le processus est géré par vous |
| `sidecar.restart` | ✕ | `always` (par défaut, rétablissement automatique après coupure/crash) ou `never` (marque seulement `down`, en attendant l'intervention de l'exploitant) |
| `sidecar.protocols[].name` | ✅ | Nom du protocole (unique globalement, participe à la priorité de sniffing) |
| `sidecar.protocols[].prefix` | ✕ | Préfixe de sniffing (ASCII) ; **vide = ne participe pas au sniffing** |
| `sidecar.protocols[].listeners[]` | ✕ | Écoute externe de ce protocole (`name` + `addr`), créée par le noyau |
| `sidecar.handshake_timeout_seconds` | ✕ | Remplace le délai de poignée de main (5 s par défaut) |
| `sidecar.heartbeat_seconds` | ✕ | Remplace l'intervalle de heartbeat (2 s par défaut) |

> **Nom du plugin et nom du protocole** : les deux **peuvent différer** (par exemple le plugin `my-sidecar` fournit le protocole `myproto`).
> La désactivation à chaud commence par retrouver tous les protocoles enregistrés sous le nom du plugin, puis ferme les ports de ces protocoles ; il n'est donc pas nécessaire de leur donner délibérément le même nom.

### 5.2 Trois modes d'intégration

| Mode | Configuration | Cas d'usage |
| --- | --- | --- |
| **Même hôte + lancement par le noyau (spawn)** | `spawn: [...]`, `address` pointe vers l'adresse qu'il écoute | Déploiement sur la même machine, conteneur unique ; le plus simple, le noyau gère le lancement et la récupération |
| **Même hôte + gestion autonome (dial)** | `spawn: []`, `address` pointe vers un processus déjà en cours | Gérer le cycle de vie du plugin avec systemd / supervisor |
| **Inter-hôtes / inter-conteneurs (dial, tcp obligatoire)** | `spawn: []`, `address: "tcp://<nom du service>:19001"` | Plugin et noyau déployés dans des conteneurs / machines distincts |

Choix de l'adresse :

- **Sur la même machine, un socket unix est recommandé** (`unix:///tmp/my-sidecar.sock`) : il n'occupe pas de port TCP et n'est pas affecté par l'occupation des ports de l'hôte.
  Attention : le chemin du socket doit être accessible en écriture au processus du noyau (dans un conteneur, l'utilisateur `swiftmq` non root).
- **En inter-conteneurs, TCP est obligatoire**, et le processus plugin doit écouter sur `0.0.0.0`, avec `address` utilisant **le nom de service du réseau de conteneurs**.

> Ne vous trompez pas de sens : **l'adresse écoutée par le plugin** = `address` ; **le port exposé aux clients** = `protocols[].listeners`.

### 5.3 Servir les clients : être reconnu grâce à `prefix`

Lors de la distribution des connexions, la couche d'accès **ne regarde que le résultat du sniffing** : pour chaque protocole activé, elle interroge `Sniff(peek)` dans l'ordre d'enregistrement (peek fait au plus 8 octets),
et celui qui correspond prend en charge la connexion. Donc :

1. **`prefix` doit être non vide** (ASCII, ≤ 8 octets). Il faut que les premiers octets envoyés par le client correspondent pour que la connexion soit confiée à votre plugin.
   Exemple : `"prefix": "PY"` → les premiers octets du client doivent être `PY` (vous pouvez considérer le préfixe comme l'en-tête magique de votre protocole).
2. **Un `prefix` vide signifie ne pas participer au sniffing** : ces connexions **ne seront pas** confiées au plugin (mesuré : les connexions sur le port d'écoute sont immédiatement coupées).
   Un `prefix` vide ne convient donc qu'au scénario « un autre protocole relaiera pour vous sur le même port » ; **ne l'utilisez pas** pour faire un port dédié.
3. `listeners[].addr` décide « sur quel port servir à l'extérieur », et `prefix` décide « si cette connexion est la vôtre » —
   les deux doivent être employés ensemble : **un port dédié doit lui aussi avoir un `prefix` non vide** (c'est aussi pourquoi, dans la configuration d'exemple du noyau,
   `echo-sidecar` écrit à la fois `prefix: "ECHO"` et `listeners: [":1885"]`).
4. Le sniffing correspond dans l'ordre d'enregistrement des protocoles, **le premier qui correspond l'emporte** : quand plusieurs plugins coexistent, les préfixes doivent être distinctifs (par exemple, commencer tous par le même octet les masquera mutuellement).

### 5.4 Remplacer les adresses d'écoute et TLS

- L'adresse d'écoute externe peut être donnée à **deux endroits** : `sidecar.protocols[].listeners[].addr` (par défaut) et
  `listeners.<nom du protocole>` (remplacement global par nom de protocole). Quand les deux existent, c'est `listeners.<nom du protocole>` qui l'emporte.
- En cas de besoin de TLS, fournissez le certificat dans `listeners.<nom du protocole>[i].tls` (champs identiques aux protocoles intégrés).
  Voici un extrait `listeners` (**JSON standard, sans commentaires possibles**) : le 1er élément est en clair, le 2e passe par TLS.

```json
"listeners": {
  "myproto": [
    { "addr": ":19002" },
    { "addr": ":19003", "tls": { "cert_file": "/etc/swiftmq/tls/cert.pem",
                                 "key_file":  "/etc/swiftmq/tls/key.pem" } }
  ]
}
```

> TLS est terminé par **le noyau** côté écoute ; le processus plugin reçoit un flux en clair — il n'a pas à gérer TLS.

### 5.5 Empaquetage : faire tourner le plugin avec le noyau

**Méthode A — l'intégrer dans la même image** (recommandé pour les plugins « publiés avec le noyau ») : ajoutez une ligne à l'étape runtime de `swiftmq/Dockerfile` :

```dockerfile
COPY --from=<构建你的插件的 stage> /out/my-sidecar /usr/local/bin/my-sidecar
# 或直接拷预编译产物：
# COPY bin/my-sidecar /usr/local/bin/my-sidecar
```

Ensuite, dans la configuration, mettez `spawn: ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"]`,
et `address` avec la même valeur. Le noyau le lance à son démarrage.

**Méthode B — monter le binaire** (sans modifier l'image, adapté au débogage conjoint) :

```yaml
services:
  swiftmq:
    image: houzch/swiftmq:1.1.02
    command: ["-config", "/etc/swiftmq/swiftmqd.json", "-log-level", "info"]
    volumes:
      - ./configs/swiftmqd.json:/etc/swiftmq/swiftmqd.json:ro
      - ./bin/my-sidecar:/usr/local/bin/my-sidecar:ro   # 插件二进制
    ports:
      - "5672:5672"        # AMQP（内置）
      - "15672:15672"      # 管理面
      - "19002:19002"      # ← 你的协议对外端口（由内核监听）
```

La configuration utilise un socket unix (pour éviter d'occuper un port supplémentaire). Voici l'extrait `sidecar` dans `plugins.my-sidecar`
(**JSON standard, sans commentaires possibles** ; `prefix` doit toujours être non vide, voir §5.3) :

```json
"sidecar": {
  "address": "unix:///tmp/my-sidecar.sock",
  "spawn": ["/usr/local/bin/my-sidecar", "-addr", "unix:///tmp/my-sidecar.sock"],
  "protocols": [{ "name": "myproto", "prefix": "MP", "listeners": [{ "name": "myproto", "addr": ":19002" }] }]
}
```

**Méthode C — conteneur séparé** (plugin publié séparément / mise à l'échelle indépendante) :

```yaml
services:
  swiftmq:
    image: houzch/swiftmq:1.1.02
    volumes: ["./configs/swiftmqd.json:/etc/swiftmq/swiftmqd.json:ro"]
    ports: ["5672:5672", "15672:15672", "19002:19002"]
    depends_on: [my-sidecar]

  my-sidecar:
    image: my-sidecar:0.1.0
    command: ["-addr", "tcp://0.0.0.0:19001"]   # 对外（对内）监听 0.0.0.0
```

Dans la configuration, `spawn: []` (le noyau se connecte sans lancer), `address: "tcp://my-sidecar:19001"` (nom de service compose).

### 5.6 Démarrage et vérification

```bash
# 1) 内核日志里应能看到握手与接入
docker compose logs swiftmq | grep -E "外部插件已接入|外部插件进程"

# 2) 经 CLI 看插件状态（state=enabled 且 RuntimeNote 为空）
./bin/swiftmqctl plugins list
./bin/swiftmqctl plugins show my-sidecar

# 3) 经管理 API 看（等价入口）
curl -u guest:guest http://127.0.0.1:15672/api/plugins/my-sidecar

# 4) 实测对外服务：你的协议端口由内核监听，直接连它
#    若你的协议是文本行协议，可以这样冒烟：
printf 'hello\n' | nc 127.0.0.1 19002
```

Dans `plugins show`, surveillez particulièrement `state` et `RuntimeNote` :
`failed` porte la raison de l'échec (poignée de main refusée / port impossible à ouvrir…) ; `down` porte la raison de la coupure (processus planté / connexion coupée).

### 5.7 Exploitation en cours d'exécution

| Opération | Commande / interface | Effet |
| --- | --- | --- |
| Désactivation à chaud | `swiftmqctl plugins disable my-sidecar` ou `PUT /api/plugins/my-sidecar/disable` | **Ferme les écoutes externes de ce plugin** (désactivation au niveau capacité) ; le noyau et les autres plugins ne sont pas affectés |
| Activation à chaud | `swiftmqctl plugins enable my-sidecar` | Rouvre ses écoutes ; si un démarrage précédent a échoué, il réessaie une fois |
| Voir l'état | `swiftmqctl plugins list/show` | État + raison d'échec/de coupure |
| Sortie du noyau | — | Coupe la connexion avec le plugin, récupère les sessions du pont, **termine les processus enfants lancés par le noyau via `spawn`** |

> La désactivation à chaud ne ferme que la « capacité » (les ports d'écoute) et **ne tue pas** le processus plugin lancé via `spawn` ; la récupération du processus a lieu à la sortie du noyau.

### 5.8 Fournir une entrée dans la console d'administration (facultatif)

Quand le plugin embarque sa propre interface, ajoutez un `console_url` (l'adresse de l'UI d'administration ; les autres champs sont au §5.1) dans la section `plugins.<nom du plugin>`.
Ci-dessous, seule l'entrée `plugins.my-sidecar` est représentée (**JSON standard, sans commentaires possibles** ; le contenu de la section `sidecar` est identique au §5.1) :

```json
"plugins": {
  "my-sidecar": {
    "builtin": false,
    "enabled": true,
    "console_url": "http://127.0.0.1:19003/",
    "sidecar": { }
  }
}
```

- La page **Gestion des plugins** de la console d'administration (données issues du champ `console_url` de `GET /api/plugins`) affiche pour ces plugins
  un bouton « Ouvrir l'interface d'administration », qui **s'ouvre dans un nouvel onglet**.
- Quand `console_url` n'est pas déclaré, le bouton est indisponible et une infobulle indique « ce plugin ne fournit pas d'interface d'administration ».
- Ce n'est que **des métadonnées écrites dans la configuration par le déployeur** : cela ne fait pas partie de l'API de plugin (`pkg/plugin`), ne participe pas au démarrage-arrêt du plugin,
  et l'interface elle-même est hébergée par le plugin (elle peut être dans le processus plugin ou dans n'importe quel service autonome).

---

## 6. Cycle de vie et matrice de tolérance aux pannes

| Cas | Comportement du noyau | Impact côté plugin |
| --- | --- | --- |
| Processus plugin non démarré / poignée de main refusée | Retente la connexion pendant 8 s ; en cas d'échec persistant, marque `failed` et isole (ne bloque pas le démarrage du noyau) | Aucun |
| Sortie d'un processus lancé par `spawn` | Consigne dans le journal ; marque `down` ; selon la politique `restart`, reconnexion avec backoff / relance | Le nouveau processus refait la poignée de main |
| Crash du processus plugin (en cours d'exécution) | Le noyau n'est pas affecté ; `down` + reconnexion avec backoff | `pkg/sidecar.Server` fermera cette connexion |
| Noyau tué par `kill -9` | — | Côté plugin, récupération de la connexion via le délai d'inactivité (aucune trame pendant 24 s par défaut), sans laisser de zombie |
| Sortie normale du noyau | Appelle `Stop` : coupe les connexions, récupère les livraisons non réglées (en remise en file), `Kill` les processus enfants | Reçoit SIGKILL |
| Livraisons pendant une coupure de la connexion plugin | Les livraisons non réglées sont toujours **remises en file**, sans perte | — |
| Déconnexion du client / retour de `Open` | Ferme le flux correspondant, libère la session et les consommateurs de ce flux | `Stream.Read` renvoie EOF |

---

## 7. Lignes rouges et limites connues

**Lignes rouges**

1. Un plugin ne peut dépendre que de `pkg/sidecar` (et éventuellement `pkg/plugin`) ; il **ne doit pas** dépendre de l'`internal/**` du noyau.
2. Le nom du plugin doit correspondre à la configuration et `APIVersion` doit correspondre au noyau, sinon la connexion est impossible (c'est ce qui empêche « de tourner silencieusement sans effet »).
3. Avec `session.*` : **`core.authenticate` d'abord, puis `session.open`**, et régler chaque livraison **exactement une fois**.
4. `protocols[].prefix` doit être non vide, sinon les connexions ne seront pas confiées au plugin (voir §5.3).
5. Un refus dans `Hello` doit **explicitement renvoyer une erreur** (ne pas rester silencieux) — sinon le noyau ne voit qu'« une connexion fermée » et ne peut pas localiser la cause.

**Limites connues**

- **Le sniffing est côté noyau** : un plugin externe ne peut pas définir de fonction de sniffing personnalisée, il ne peut compter que sur `prefix` (ASCII, ≤ 8 octets) ;
  un `prefix` vide signifie « impossible d'obtenir une connexion » (voir §5.3).
- **Le plan de données passe par un proxy local** : pas de passage de fd (Windows n'a pas `SCM_RIGHTS`), soit une copie mémoire de plus qu'en intra-processus ;
  chaque appel inverse ajoute aussi un RPC local.
- **Les types de la table de propriétés se dégradent** : `Properties.Headers` transite par JSON, la distinction de types comme `int32` / `double` est perdue (voir §3.4).
- **La contre-pression d'un flux affecte toute la connexion** : quand le tampon de réception d'un flux est plein, la goroutine de distribution de cette connexion est bloquée ; la limitation de débit par flux relève d'une optimisation ultérieure.
- **Un échec d'authentification ne transmet que du texte** : l'échec d'authentification du noyau est une `*plugin.AuthError` (classification différente de `plugin.ErrorKind`),
  et seul le texte parvient au plugin via le pont ; le plugin doit le mapper en code d'erreur de protocole selon sa propre convention.
- **Seule la capacité `net.listen` prend réellement effet** : `store.read/write`, `http.route`, `cluster.metadata.write`,
  `auth.verify` sont des **emplacements réservés** ; les déclarer ne participe qu'à l'audit (voir la vue de gouvernance au §5.6), et il n'existe actuellement aucun point d'extension correspondant.

---

## 8. FAQ de dépannage

| Symptôme | Cause et traitement |
| --- | --- |
| État `failed`, la raison contient « nom de plugin incohérent » | Le nom de plugin configuré ≠ `HelloAck.name` ; alignez-les |
| État `failed`, la raison contient « version d'API incompatible » | `HelloAck.api_version` ≠ `APIVersion` du noyau ; alignez-les |
| État `failed`, la raison contient « poignée de main refusée » | Le `Hello` du plugin a renvoyé une erreur (`deny`) ; consultez la sortie du plugin relayée dans le journal du noyau |
| État `failed`, la raison contient « échec de connexion au plugin externe » | Le processus n'est pas démarré / `address` est erroné / le chemin du socket n'est pas accessible en écriture (attention aux permissions de l'utilisateur `swiftmq` dans un conteneur) |
| État `down` | Le processus plugin a planté ou la connexion est tombée ; `restart=always` reconnecte automatiquement, `never` nécessite une relance manuelle |
| Port non ouvert / client ne peut pas se connecter | `protocols[].listeners` n'est pas configuré ou son adresse est écrasée par `listeners.<nom du protocole>` ; vérifiez les deux endroits |
| Le client se connecte à un autre port puis est immédiatement déconnecté | Ce port ne correspond pas à votre protocole (`prefix` vide ou préfixe non concordant) ; donnez au protocole un `prefix` non vide (voir §5.3) |
| Erreur `ACCESS_REFUSED - ... for user ''` | **Pas d'authentification** avant le pont sémantique ; appelez `core.authenticate` avant `session.open` |
| L'appel inverse renvoie « le flux N n'a pas encore ouvert de session » | Faites `core.authenticate` d'abord, puis `session.open`, avant de pouvoir appeler d'autres `session.*` |
| Aucune livraison de consommation reçue | Les livraisons arrivent dans votre `Call` sous forme d'**appel direct** `session.deliver` ; vérifiez que cette méthode est bien traitée |
| Plugin hors du conteneur, noyau dans le conteneur, connexion impossible | Utilisez `address: tcp://host.docker.internal:<port>` (ou mettez aussi le plugin dans le conteneur et utilisez le nom de service) ; le plugin doit écouter sur `0.0.0.0` |

---

## 9. Références (index des sources)

| Ce que vous voulez voir | Fichier |
| --- | --- |
| Protocole filaire et implémentations des deux côtés (**lecture obligatoire pour le développement**) | [`pkg/sidecar/`](../../../pkg/sidecar/) : `frame.go` (trames), `proto.go` (messages), `server.go` (côté plugin), `client.go` (côté noyau), `bridge.go` (contrat `session.*`), `stream.go` (flux) |
| Hôte sidecar côté noyau (intégration/reconnexion/proxy/état) | [`internal/plugin/sidecar/sidecar.go`](../../../internal/plugin/sidecar/sidecar.go) |
| Pont sémantique côté noyau (`session.*` → `plugin.Session`) | [`internal/plugin/sidecar/bridge.go`](../../../internal/plugin/sidecar/bridge.go) |
| Types de la surface d'opérations de session du noyau (`Message` / `Delivery` / `ErrorKind`) | [`pkg/plugin/session.go`](../../../pkg/plugin/session.go) |
| Écoute, sniffing, démarrage-arrêt à chaud par plugin | [`internal/transport/server.go`](../../../internal/transport/server.go) |
| Cycle de vie et gouvernance des plugins (isolation/état/audit) | [`internal/plugin/manager.go`](../../../internal/plugin/manager.go), [`registry.go`](../../../internal/plugin/registry.go) |
| Éléments de configuration et exemples (y compris la section sidecar) | [`internal/config/config.go`](../../../internal/config/config.go), [`configs/swiftmqd.json`](../../../configs/swiftmqd.json) |
| Assemblage du processus (comment le sidecar est intégré au noyau) | [`cmd/swiftmqd/main.go`](../../../cmd/swiftmqd/main.go) |
| Implémentation de référence Go (utilise `pkg/sidecar.Server`, avec le pont `session.*` et `core.authenticate`) | Projet de test autonome `swiftmq-test/test/integration/echosidecar/` |
| **Guides par langage + projets d'exemple** | Dans ce répertoire `plugin-development-python.md` / `-nodejs.md` / `-php.md` / `-java.md` ; les exemples sont dans l'**espace de travail** `swiftmq-plugin/{python,nodejs,php,java}/` |
