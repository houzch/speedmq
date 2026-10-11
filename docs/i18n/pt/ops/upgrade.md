# Plano de atualização e migração do SpeedMQ

> Versão aplicável: `1.0.0` (`broker.Version`, ver `speedmq_build_info` em `/metrics`).
> Todas as conclusões "medidas" neste documento vêm de execuções reais nesta máquina; tudo o que não foi medido é marcado explicitamente como **【não verificado】**.
> Ambiente desta máquina: Windows + PowerShell 5.1, Go 1.27.1 windows/386, `data_dir` temporário + portas não padrão.

---

## 1. Migração (de RabbitMQ para SpeedMQ)

O posicionamento deste projeto é a **compatibilidade em nível de protocolo AMQP 0-9-1**, portanto a "migração" consiste principalmente em **mudar o endereço de conexão**:

- Zero alteração no código de negócio, basta mudar `host/port/vhost` (documento de design G3 "migração sem custo").
- A cadeia de ferramentas de gerenciamento (`rabbitmqadmin`, UI de gerenciamento, scripts de monitoramento) só precisa apontar para a porta do plano de gerenciamento; o formato da interface está alinhado ao RabbitMQ (convenções como `amq.default`, `%2F`, `{error, reason}` são copiadas).
- As portas padrão são iguais às do RabbitMQ: AMQP `5672`, plano de gerenciamento `15672`; MQTT é `1883`, e o RPC entre nós é `25672`.

**Diferenças semânticas que você deve verificar antes de migrar** (todas intencionais neste repositório, conforme o README / documento de design):

| Item | Comportamento do SpeedMQ | Impacto na migração |
| --- | --- | --- |
| Filas transitórias (não duráveis e não exclusivas) | **Recusa a declaração** (541), `auto_delete` não isenta | Clientes antigos que dependem desse tipo de fila vão falhar; é preciso mudar para durable ou exclusive |
| vhost padrão `/` | **Não pode ser excluído** (400); o RabbitMQ permite | Scripts de automação que excluem o vhost padrão vão falhar (esta é a única restrição de segurança ativa) |
| Dados de filas clássicas | **Não são replicados**, os dados ficam apenas no nó Owner | Se precisar de redundância entre nós, mude para filas quórum `x-queue-type=quorum` |
| Filas quórum | Suportam aumento de réplicas, **não suportam redução** | Planeje de uma vez |
| Plugins | Sem ecossistema de plugins Erlang; AMQP 1.0 / STOMP não implementados | Cenários que usam esses protocolos ainda não são migráveis |

**Migração de dados**: os formatos de armazenamento do SpeedMQ e do RabbitMQ são incompatíveis, e **não há ferramenta de transferência de dados online/offline**.
A forma de migração é "criar um SpeedMQ vazio → rodar em paralelo para validar → trocar o tráfego gradualmente". **【não verificado】** Este documento não contém nenhum ensaio real de transferência de dados do RabbitMQ.

---

## 2. Princípios gerais de atualização

1. **Faça backup primeiro** (ver `backup-restore.md`) — é a rede de segurança caso a atualização falhe.
2. **Pare o processo antes de substituir** (o diretório de dados tem restrição de escritor único, ver §4.2).
3. **Depois de atualizar é obrigatório validar**: o processo sobe, o `/api/overview` é legível, o `/metrics` pode ser coletado e a contagem de mensagens das filas é igual à de antes do backup.
4. Atualização de cluster **nó a nó, em rolling upgrade**, mexendo em um nó por vez (ver §5).

---

## 3. Layout do diretório de dados (base factual para atualização/migração)

O layout do `data_dir` obtido por **medição** em uma instância autônoma nesta máquina:

```
data/
├── meta/
│   ├── state.json        # Instantâneo de metadados no modo autônomo (vhost/exchanges/filas/bindings/usuários/permissões/políticas)
│   ├── users.seeded      # Marcador de bootstrap: os users da configuração já foram semeados
│   ├── vhosts.seeded     # Marcador de bootstrap: os vhosts da configuração já foram semeados
│   ├── raft.state        # 【Modo cluster】Mandato/voto do Raft
│   ├── raft.log          # 【Modo cluster】Log Raft
│   └── snapshot.json     # 【Modo cluster】Snapshot do Raft + tabela de membros
├── msg_stores/vhosts/<safe(vhost)>/queues/<safe(queue)>/
│   ├── 000001.seg        # Arquivo de segmento (corpo da mensagem + atributos), formato do registro: <len u32><crc32 u32><payload>
│   └── index/000001.idx  # Índice da fila: seq-id → (número do segmento, deslocamento no segmento, tamanho, estado)
└── quorum/<safe(vhost)>/<safe(queue)>/   # 【Cluster】Cada fila quórum tem um grupo Raft próprio (log/snapshot)
```

**Atenção (dois pontos que contrariam a intuição, ambos com base no código/na medição)**:

- No modo cluster, os arquivos de persistência do Raft ficam **diretamente sob `meta/`** (`raft.state` / `raft.log` / `snapshot.json`),
  e **não existe o subdiretório `meta/raft/`**. Base: as constantes de nome de arquivo em `internal/raft/log.go` + o
  `Dir: filepath.Join(b.cfg.DataDir, "meta")` em `internal/broker/cluster.go`. **【verificado (2026-10-10, Docker Desktop local, cluster de 3 nós)】** — o conteúdo do backup do volume do nó do cluster mostra diretamente `meta/raft.log`, `meta/raft.state`, `meta/*.seeded` (não foi visto o subdiretório `meta/raft/`).
- Os nomes de diretório **não são os nomes originais de vhost / fila**, mas sim a codificação de `store.SafeDirName`: prefixo `q_`, e bytes fora de `[A-Za-z0-9._-]` escapados como `%XX`.
  Medido: vhost `/` → diretório `q_%2F`, fila `persist.q` → diretório `q_persist.q`.
  Isso é feito para evitar path traversal e nomes de dispositivo reservados do Windows (`con`/`nul` etc.).

---

## 4. Compatibilidade de dados

### 4.1 Se os dados antigos podem ser lidos diretamente — podem

- **Formato de índice compatível para frente**: o M8-1 adicionou o campo "número do segmento" ao registro de índice (25 bytes); **o formato antigo (21 bytes, sem número de segmento) ainda pode ser lido tal como está**,
  e na leitura equivale a "só existe um segmento (seg=1)"; **a atualização não precisa de script de migração**.
  Base: as constantes `indexEntrySize` / `legacyIndexEntrySize` e a lógica de `recover()` em `internal/store/store.go`; README M8-1.
- **Semântica de falha inalterada**: cada registro carrega prefixo de tamanho + CRC32, e a recuperação **descarta registros parcialmente gravados/corrompidos no fim** e trunca.
  Medido (ver `backup-restore.md` §6): após parar o processo e reiniciar, as 5 mensagens persistentes da fila durable **foram todas recuperadas**,
  e apareceu no log `已从磁盘恢复队列消息 ... messages=5`.

**【verificado (2026-10-10, local)】** A leitura entre grandes versões foi medida: os dados de fila durable gravados com a tag `v1.1.0` são restaurados tal como estão sob `1.1.05` (comandos e saída em §5).

### 4.2 Critério de `vhosts` / `users` na configuração (a armadilha mais comum na atualização)

- Os dois **só valem no primeiro bootstrap**: na primeira inicialização, os vhosts/users da configuração são gravados nos metadados e são deixados os arquivos marcadores
  `meta/vhosts.seeded` / `meta/users.seeded`; **a partir daí, os metadados são a fonte de verdade**.
- Portanto, **ao atualizar/trocar configuração, não espere adicionar ou remover contas ou vhosts alterando o arquivo de configuração** — alterar não fará efeito;
  use a API de gerenciamento ou o `speedmqctl`.
- Por outro lado, a atualização **não** sobrescreve contas existentes com a configuração: uma senha alterada em tempo de execução não volta ao valor antigo da configuração após reiniciar,
  e uma conta excluída em tempo de execução também não ressuscita. Base: a lógica de marcador de semeadura em `cluster.go`; README M8-4 / M8-7.

**【verificado (2026-10-10, local)】** Alterações em tempo de execução realmente não são sobrescritas pela configuração: antes da atualização, via API de gerenciamento, a senha de `guest` foi alterada para `rotated`; após reiniciar com `1.1.05` + o mesmo `data_dir`, `guest/guest` (a senha antiga na configuração) retornou **401** e `guest/rotated` retornou **200**.

### 4.3 Rotação de segmentos e recuperação de disco

- As mensagens são divididas em segmentos por tamanho (padrão 8 MiB); **quando todas as mensagens do segmento são confirmadas (ack) e o segmento é selado, o segmento inteiro é excluído**, e o índice é compactado e reescrito na sequência.
- A atualização não muda esse comportamento; os arquivos de segmento único deixados pela instância antiga continuam funcionando normalmente na nova lógica de rotação de segmentos.

---

## 5. Atualização do binário (bare metal)

> **【verificado (2026-10-10, local)】** Ensaio real entre grandes versões: o binário antigo compilado a partir da tag `v1.1.0` escreveu exchanges/filas/bindings durable + 5 mensagens persistentes + vhosts/contas/permissões/políticas e alterou a senha em tempo de execução; após parar o processo, foi iniciado com o binário atual `1.1.05` + **o mesmo `data_dir` + as mesmas portas** — o processo subiu normalmente, os objetos e a contagem de mensagens em `/api/queues` coincidiram, um cliente AMQP real recuperou todas as 5 mensagens persistentes e o `/metrics` reportou `speedmq_build_info{version="1.1.05"}`.
> Os passos abaixo continuam sendo um fluxo genérico e podem ser aplicados a qualquer versão adjacente; para **downgrade** ver §8.

### 5.1 Passos

```powershell
$base = "C:\speedmq"
$data = "$base\data"

# 1) Pare o processo (a saída graciosa faz a descarga final em disco; ver §4 "consistência")
#    Se estiver rodando em primeiro plano: Ctrl+C; se como serviço: Stop-Service / Stop-Process
Stop-Process -Name speedmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) Faça backup do diretório de dados (garanta que seja após o processo parar)
Copy-Item -Recurse -Force $data "$base\backup-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) Substitua o binário (coloque o speedmqd.exe / speedmqctl.exe da nova versão no caminho original)
#    Copy-Item .\new\speedmqd.exe $base\speedmqd.exe -Force

# 4) Inicie
& "$base\speedmqd.exe" -config "$base\configs\speedmqd.json" -log-level info

# 5) Valide: processo vivo + API de gerenciamento legível
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

### 5.2 Checklist de validação após a atualização

- No log de inicialização aparecem `SpeedMQ 启动中 ... version=<nova versão>` e `管理面已启动`;
- O `object_totals` / `queue_totals` do `/api/overview` é igual ao de antes do backup (comparar com `backup-restore.md` §5);
- O `messages` / `messages_ready` de cada fila durable em `/api/queues` é igual ao de antes do backup;
- O `/metrics` pode ser coletado e mostra `speedmq_plugin_up{name="amqp091"} 1`, `{name="mqtt"} 1`.

---

## 6. Atualização da imagem (contêiner)

A imagem tem cerca de 13 MB (binário com ligação estática + alpine), **roda como não-root (uid 10001)** e o diretório de dados é montado em `/var/lib/speedmq`.

```powershell
# 1) Baixe/construa a nova imagem (use o número da nova versão na tag, para evitar confusão entre old/new)
docker build -t speedmq:1.0.0 .

# 2) Pare o contêiner antigo (o compose mantém o volume nomeado speedmq-data)
docker compose down

# 3) Suba a nova versão (mude o image para a nova tag no arquivo compose)
docker compose up -d

# 4) Status e logs
docker compose ps
docker compose logs -f --tail 100
```

> **Tarefas de uso único dentro do contêiner** (por exemplo, rodar `speedmqctl` dentro do contêiner): o `run` de `docker compose ...` precisa obrigatoriamente de `-T` em ambiente não interativo,
> caso contrário falha por não conseguir solicitar TTY:
> ```powershell
> docker compose run -T --rm broker speedmqctl -user guest -pass guest status
> ```

A persistência de dados depende do **volume nomeado** `speedmq-data` do compose; recriar o contêiner não perde dados (a gravação real em disco acontece desde o M4).
Se precisar fazer backup do conteúdo do volume antes de atualizar, equivale a fazer backup de `/var/lib/speedmq` (ver `backup-restore.md` §3.2).

> **【verificado (2026-10-10, Docker Desktop local)】** Foi feito um ensaio real de atualização de imagem: construir localmente a imagem antiga `speedmq:1.1.0-old` (git tag `v1.1.0`) e a imagem nova `speedmq:1.1.05-new` (código-fonte atual), iniciar a imagem antiga com compose de nó único → criar a fila durable `persist.q` e publicar 5 mensagens persistentes (`delivery_mode=2`) → `docker compose down` (**sem `-v`**, o volume nomeado `speedmq-data` é mantido) → trocar a tag da imagem no compose para a nova imagem e então `docker compose up -d`.
> Resultado: o novo contêiner subiu normalmente, o log de inicialização mostrou `已从磁盘恢复队列消息 ... queue=persist.q messages=5`, o `/metrics` reportou `speedmq_build_info{version="1.1.05"}`, o `queue_totals.messages=5` do `/api/overview` e o `messages=5` da fila `persist.q` ficaram iguais antes e depois da atualização, e um cliente AMQP real (`rabbitmq/amqp091-go`) recuperou, uma a uma, todas as 5 mensagens persistentes (`deliveryMode=2`).
> Nota: `v1.1.0` é a imagem da marca `speedmq` antes da mudança de nome (binário `speedmqd`, métrica `speedmq_build_info`, diretório de dados padrão `/var/lib/speedmq`), com nomenclatura diferente da versão atual; no ensaio foi usada a configuração `data_dir=/var/lib/speedmq` como referência e foi predefinido o proprietário para o volume nomeado (esse caminho gravável não existe na imagem antiga). Isso não afeta a conclusão de que "recriar o contêiner com uma nova tag não perde os dados do volume nomeado".

---

## 7. Implantação gradual e rollback

### 7.1 Autônomo

- **Implantação gradual**: o SpeedMQ autônomo não tem a capacidade embutida de "duas versões antiga e nova no mesmo processo". A forma viável de gradual é o **shadow sidecar**:
  a instância da nova versão primeiro observa o mesmo fluxo de tráfego de origem usando **consumo somente leitura/filas shadow** e, após confirmar que está tudo certo, troca-se o lado que escreve.
- **Rollback**:
  1. pare o processo da nova versão;
  2. volte ao binário antigo;
  3. se a nova versão já gravou dados, **é obrigatório restaurar o `data_dir` a partir do backup pré-atualização** (ver abaixo).
  **Não** há garantia de "a nova versão já escreveu e a antiga lê direto" — para downgrade entre versões, ver §8.

### 7.2 Cluster (rolling upgrade)

A plataforma não oferece "rolling upgrade com um clique"; é preciso operar manualmente nó a nó na ordem abaixo:

1. **Atualize um nó por vez**: pare o nó → faça backup do `data_dir` dele → troque o binário → inicie → espere ele reentrar e alcançar o log
   (veja `role`, `commit_index`/`last_applied` em `speedmqctl cluster_status` / `GET /api/cluster`).
2. **Ordem sugerida**: atualize primeiro os **learners / membros não votantes** (sem impacto na maioria), depois os **followers** e por último o **leader**
   (atualizar o leader dispara uma eleição, com um breve período sem escrita).
3. **Impacto da parada sobre a maioria** (crítico):
   - Cluster de 3 nós: pare **no máximo 1** membro votante por vez; parar 2 faz perder a maioria e, com `pause_minority`, **todo o cluster pausa o serviço**.
   - Cluster de 2 nós: parar 1 já perde a maioria, **não há capacidade de rolling upgrade** (recomenda-se ao menos 3 nós).
   - Portanto, durante o rolling upgrade é **terminantemente proibido parar vários membros votantes de uma vez**.
4. **Não misture mudança de membros com atualização**: a mudança de membros **não tem joint consensus**, e só é permitida uma configuração não confirmada por vez;
   durante a atualização, evite `add_member` / `remove_member` ao mesmo tempo.
5. Depois de concluir a atualização, confira se o `object_totals` do `GET /api/cluster` é igual ao de antes.

> **【verificado (2026-10-10, Docker Desktop local, cluster de 3 nós)】** Foi feito um ensaio real de **rolling upgrade entre versões** de um cluster: a linha de base foi a `speedmq:1.1.0-old` construída localmente (git tag `v1.1.0`), atualizada nó a nó (n1 → n2 → n3, um por vez: parar primeiro, depois fazer backup do volume nomeado, depois trocar a imagem e iniciar) até a atual `1.1.05` (`speedmq:1.1.05-new` construída localmente, com o compose trocando a imagem via override por nó). Durante todo o processo o cluster sempre manteve um leader e a maioria — ao parar o leader atual, um novo leader foi eleito em cerca de 0,6 s, `pause_minority` não acionou pausa e não houve interrupção externa durante a parada; após a atualização, o `object_totals` do `GET /api/cluster` coincidiu com o de antes da atualização (`queues:1`), as 25 mensagens persistentes da fila quórum `qroll` de 3 réplicas mantiveram a mesma contagem e puderam ser lidas de volta uma a uma, e após publicar mais 5 mensagens novas, todas as 30 puderam ser recuperadas.
> A ordem acima ainda se sustenta como restrição geral do Raft, e este ensaio cobriu o cenário de 3 nós. **【verificado (2026-10-10, Docker Desktop local): um cluster de 2 nós perde a maioria ao parar 1 nó】** Um cluster mínimo separado de 2 nós (n1/n2, cada um com `peers` contendo apenas esses dois, `partition_policy=pause_minority`) foi medido: com os dois nós online, o `GET /api/cluster` era `has_quorum=true / paused=false` (n1 leader, n2 follower); após `docker stop` do n2, o n1 passou a `has_quorum=false / paused=true` em cerca de 2 s — ou seja, parar 1 membro votante perde a maioria e `pause_minority` pausa o serviço, **confirmando que um cluster de 2 nós não tem capacidade de rolling upgrade**; após `docker start` restaurar o n2, o n1 voltou a `has_quorum=true / paused=false` em cerca de 2 s (serviço recuperado automaticamente).

> **【verificado (2026-10-11, Docker Desktop local, cluster de 3 nós)】O rolling upgrade sob partição de rede foi testado na prática** (cross-version `v1.1.0` → `1.1.05`; a partição usa uma topologia de rede dupla: Raft-RPC entre os nós passa apenas pela `cluster-net` com IPs estáticos, cliente/plano de gerenciamento passa pela `client-net`, partições comutadas com `docker network disconnect/connect`):
> - **Follower particionado**: o lado da maioria manteve quorum continuamente durante a partição, publicou/consumiu normalmente e completou, enquanto a partição persistia, o ciclo "parar→trocar imagem→iniciar→alcançar" de um nó da maioria; o cluster com versões mistas (1.1.0 + 1.1.05) permaneceu operável. Após a recuperação da partição, o nó isolado (versão antiga) reentrou e alcançou; mensagens reconciliadas por conjunto de IDs únicos 35/35 sem perda.
> - **Leader particionado**: a maioria elegeu um novo leader em aproximadamente **630 ms**; o leader antigo isolado manteve `role=leader` mas com `has_quorum=false / paused=true` (`pause_minority` em vigor, não eleva o mandato por conta própria); durante a partição, outro nó da maioria foi atualizado com sucesso; após a recuperação, o leader antigo reentrou, sem split-brain, sem bifurcação de dados.
> - **Comportamento esperado sem PreVote (registrado com honestidade)**: o follower isolado iniciou imediatamente eleições repetidas, mandato cerca de a cada 2 s +2 (os dois grupos Raft de metadata e fila se alternavam elevando); ao recuperar, **arrebatou** o leadership com seu mandato mais alto (na prática, eleito em 1,4 s) — perturbação de troca de leader, mas sem problema de corretude.
> - **Duas armadilhas operacionais**: ① se `peers` usar hostnames e o container estiver conectado simultaneamente a uma segunda rede acessível, o tráfego Raft faz um desvio criando uma "partição falsa" (o nó isolado ainda mostrava `has_quorum=true` por mais de um minuto) — RPC entre nós deve passar por apenas uma rede (peers apontam diretamente para o IP estático dessa rede); ② ao recuperar, `docker network connect` deve incluir `--ip <IP original>`, caso contrário o nó recebe um novo IP enquanto a maioria continua discando o endereço antigo, após o que o nó isolado, com log atrasado e mandato inflado, continua concorrendo (na prática, arrastou o mandato do cluster de 320 para 488, metadata commit de 13 para 97) e pode ocorrer um transiente em que o leader mostra `quorum=true` mas brevemente `paused=true`.
> - Durante a partição, a API de gerenciamento do lado da minoria é legível (útil para observação); publicação AMQP é recusada (`pause_minority` em vigor).

---

## 8. Partes não suportadas / não verificadas (listadas explicitamente)

- **Downgrade entre grandes versões: não suportado, não verificado**. Se a nova versão já gravou dados em novo formato/nova semântica, **não** há garantia de "voltar ao binário antigo e ler normalmente";
  o rollback depende apenas do backup pré-atualização.
- **Formato de configuração inalterado**: continua sendo JSON + variáveis de ambiente `SPEEDMQ_*`. **Configuração YAML ainda não suportada** (requer trazer uma dependência de parser; M8-17 a avaliar),
  e a atualização não traz YAML.
- **Atualização a quente de plugins/protocolos online**: os plugins são compilados junto com o núcleo (forma A) ou iniciados por `spawn` conforme a configuração (forma B),
  e atualizar o núcleo = reiniciar o processo; **não** há mecanismo de substituição a quente do binário em execução.
- **Migração no local do mecanismo de armazenamento**: a rotação de segmentos/compactação de índice é comportamento de segundo plano em tempo de execução; **não** há comando dedicado de "migração/compactação de dados".
- **Atualização de cluster com disco cheio**: **não verificado**. (O rolling upgrade sob partição de rede foi testado na prática em 2026-10-11, veja a nota no final de §7.)
- Este documento **não inclui** nenhuma validação de transferência de dados entre o SpeedMQ e outro broker (RabbitMQ).
