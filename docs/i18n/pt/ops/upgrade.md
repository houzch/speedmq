# Plano de atualização e migração do SwiftMQ

> Versão aplicável: `1.0.0` (`broker.Version`, ver `swiftmq_build_info` em `/metrics`).
> Todas as conclusões "medidas" neste documento vêm de execuções reais nesta máquina; tudo o que não foi medido é marcado explicitamente como **【não verificado】**.
> Ambiente desta máquina: Windows + PowerShell 5.1, Go 1.27.1 windows/386, `data_dir` temporário + portas não padrão.

---

## 1. Migração (de RabbitMQ para SwiftMQ)

O posicionamento deste projeto é a **compatibilidade em nível de protocolo AMQP 0-9-1**, portanto a "migração" consiste principalmente em **mudar o endereço de conexão**:

- Zero alteração no código de negócio, basta mudar `host/port/vhost` (documento de design G3 "migração sem custo").
- A cadeia de ferramentas de gerenciamento (`rabbitmqadmin`, UI de gerenciamento, scripts de monitoramento) só precisa apontar para a porta do plano de gerenciamento; o formato da interface está alinhado ao RabbitMQ (convenções como `amq.default`, `%2F`, `{error, reason}` são copiadas).
- As portas padrão são iguais às do RabbitMQ: AMQP `5672`, plano de gerenciamento `15672`; MQTT é `1883`, e o RPC entre nós é `25672`.

**Diferenças semânticas que você deve verificar antes de migrar** (todas intencionais neste repositório, conforme o README / documento de design):

| Item | Comportamento do SwiftMQ | Impacto na migração |
| --- | --- | --- |
| Filas transitórias (não duráveis e não exclusivas) | **Recusa a declaração** (541), `auto_delete` não isenta | Clientes antigos que dependem desse tipo de fila vão falhar; é preciso mudar para durable ou exclusive |
| vhost padrão `/` | **Não pode ser excluído** (400); o RabbitMQ permite | Scripts de automação que excluem o vhost padrão vão falhar (esta é a única restrição de segurança ativa) |
| Dados de filas clássicas | **Não são replicados**, os dados ficam apenas no nó Owner | Se precisar de redundância entre nós, mude para filas quórum `x-queue-type=quorum` |
| Filas quórum | Suportam aumento de réplicas, **não suportam redução** | Planeje de uma vez |
| Plugins | Sem ecossistema de plugins Erlang; AMQP 1.0 / STOMP não implementados | Cenários que usam esses protocolos ainda não são migráveis |

**Migração de dados**: os formatos de armazenamento do SwiftMQ e do RabbitMQ são incompatíveis, e **não há ferramenta de transferência de dados online/offline**.
A forma de migração é "criar um SwiftMQ vazio → rodar em paralelo para validar → trocar o tráfego gradualmente". **【não verificado】** Este documento não contém nenhum ensaio real de transferência de dados do RabbitMQ.

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
  `Dir: filepath.Join(b.cfg.DataDir, "meta")` em `internal/broker/cluster.go`. **【layout de cluster não medido】** (nesta máquina só foi executada a instância autônoma).
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

### 4.2 Critério de `vhosts` / `users` na configuração (a armadilha mais comum na atualização)

- Os dois **só valem no primeiro bootstrap**: na primeira inicialização, os vhosts/users da configuração são gravados nos metadados e são deixados os arquivos marcadores
  `meta/vhosts.seeded` / `meta/users.seeded`; **a partir daí, os metadados são a fonte de verdade**.
- Portanto, **ao atualizar/trocar configuração, não espere adicionar ou remover contas ou vhosts alterando o arquivo de configuração** — alterar não fará efeito;
  use a API de gerenciamento ou o `swiftmqctl`.
- Por outro lado, a atualização **não** sobrescreve contas existentes com a configuração: uma senha alterada em tempo de execução não volta ao valor antigo da configuração após reiniciar,
  e uma conta excluída em tempo de execução também não ressuscita. Base: a lógica de marcador de semeadura em `cluster.go`; README M8-4 / M8-7.

### 4.3 Rotação de segmentos e recuperação de disco

- As mensagens são divididas em segmentos por tamanho (padrão 8 MiB); **quando todas as mensagens do segmento são confirmadas (ack) e o segmento é selado, o segmento inteiro é excluído**, e o índice é compactado e reescrito na sequência.
- A atualização não muda esse comportamento; os arquivos de segmento único deixados pela instância antiga continuam funcionando normalmente na nova lógica de rotação de segmentos.

---

## 5. Atualização do binário (bare metal)

> Esta máquina **não fez um ensaio real entre versões** (o repositório tem atualmente só uma versão, `1.0.0`, sem binário antigo para atualizar). Os passos abaixo são a **validação de repetição da mesma versão + fluxo genérico** para as capacidades que este repositório já possui; as partes entre versões são marcadas como **【não verificado】**.

### 5.1 Passos

```powershell
$base = "C:\swiftmq"
$data = "$base\data"

# 1) Pare o processo (a saída graciosa faz a descarga final em disco; ver §4 "consistência")
#    Se estiver rodando em primeiro plano: Ctrl+C; se como serviço: Stop-Service / Stop-Process
Stop-Process -Name swiftmqd -ErrorAction SilentlyContinue
Start-Sleep -Seconds 2

# 2) Faça backup do diretório de dados (garanta que seja após o processo parar)
Copy-Item -Recurse -Force $data "$base\backup-$(Get-Date -Format yyyyMMdd-HHmmss)"

# 3) Substitua o binário (coloque o swiftmqd.exe / swiftmqctl.exe da nova versão no caminho original)
#    Copy-Item .\new\swiftmqd.exe $base\swiftmqd.exe -Force

# 4) Inicie
& "$base\swiftmqd.exe" -config "$base\configs\swiftmqd.json" -log-level info

# 5) Valide: processo vivo + API de gerenciamento legível
$pair = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes('guest:guest'))
$H = @{ Authorization = "Basic $pair" }
Invoke-WebRequest -Uri 'http://127.0.0.1:15672/api/overview' -Headers $H -UseBasicParsing | Select-Object -ExpandProperty Content
```

### 5.2 Checklist de validação após a atualização

- No log de inicialização aparecem `SwiftMQ 启动中 ... version=<nova versão>` e `管理面已启动`;
- O `object_totals` / `queue_totals` do `/api/overview` é igual ao de antes do backup (comparar com `backup-restore.md` §5);
- O `messages` / `messages_ready` de cada fila durable em `/api/queues` é igual ao de antes do backup;
- O `/metrics` pode ser coletado e mostra `swiftmq_plugin_up{name="amqp091"} 1`, `{name="mqtt"} 1`.

---

## 6. Atualização da imagem (contêiner)

A imagem tem cerca de 13 MB (binário com ligação estática + alpine), **roda como não-root (uid 10001)** e o diretório de dados é montado em `/var/lib/swiftmq`.

```powershell
# 1) Baixe/construa a nova imagem (use o número da nova versão na tag, para evitar confusão entre old/new)
docker build -t swiftmq:1.0.0 .

# 2) Pare o contêiner antigo (o compose mantém o volume nomeado swiftmq-data)
docker compose down

# 3) Suba a nova versão (mude o image para a nova tag no arquivo compose)
docker compose up -d

# 4) Status e logs
docker compose ps
docker compose logs -f --tail 100
```

> **Tarefas de uso único dentro do contêiner** (por exemplo, rodar `swiftmqctl` dentro do contêiner): o `run` de `docker compose ...` precisa obrigatoriamente de `-T` em ambiente não interativo,
> caso contrário falha por não conseguir solicitar TTY:
> ```powershell
> docker compose run -T --rm broker swiftmqctl -user guest -pass guest status
> ```

A persistência de dados depende do **volume nomeado** `swiftmq-data` do compose; recriar o contêiner não perde dados (a gravação real em disco acontece desde o M4).
Se precisar fazer backup do conteúdo do volume antes de atualizar, equivale a fazer backup de `/var/lib/swiftmq` (ver `backup-restore.md` §3.2). **【atualização de imagem não medida】** (Docker não foi executado nesta máquina).

---

## 7. Implantação gradual e rollback

### 7.1 Autônomo

- **Implantação gradual**: o SwiftMQ autônomo não tem a capacidade embutida de "duas versões antiga e nova no mesmo processo". A forma viável de gradual é o **shadow sidecar**:
  a instância da nova versão primeiro observa o mesmo fluxo de tráfego de origem usando **consumo somente leitura/filas shadow** e, após confirmar que está tudo certo, troca-se o lado que escreve.
- **Rollback**:
  1. pare o processo da nova versão;
  2. volte ao binário antigo;
  3. se a nova versão já gravou dados, **é obrigatório restaurar o `data_dir` a partir do backup pré-atualização** (ver abaixo).
  **Não** há garantia de "a nova versão já escreveu e a antiga lê direto" — para downgrade entre versões, ver §8.

### 7.2 Cluster (rolling upgrade)

A plataforma não oferece "rolling upgrade com um clique"; é preciso operar manualmente nó a nó na ordem abaixo:

1. **Atualize um nó por vez**: pare o nó → faça backup do `data_dir` dele → troque o binário → inicie → espere ele reentrar e alcançar o log
   (veja `role`, `commit_index`/`last_applied` em `swiftmqctl cluster_status` / `GET /api/cluster`).
2. **Ordem sugerida**: atualize primeiro os **learners / membros não votantes** (sem impacto na maioria), depois os **followers** e por último o **leader**
   (atualizar o leader dispara uma eleição, com um breve período sem escrita).
3. **Impacto da parada sobre a maioria** (crítico):
   - Cluster de 3 nós: pare **no máximo 1** membro votante por vez; parar 2 faz perder a maioria e, com `pause_minority`, **todo o cluster pausa o serviço**.
   - Cluster de 2 nós: parar 1 já perde a maioria, **não há capacidade de rolling upgrade** (recomenda-se ao menos 3 nós).
   - Portanto, durante o rolling upgrade é **terminantemente proibido parar vários membros votantes de uma vez**.
4. **Não misture mudança de membros com atualização**: a mudança de membros **não tem joint consensus**, e só é permitida uma configuração não confirmada por vez;
   durante a atualização, evite `add_member` / `remove_member` ao mesmo tempo.
5. Depois de concluir a atualização, confira se o `object_totals` do `GET /api/cluster` é igual ao de antes.

> **【não verificado】** Esta máquina não fez ensaio real de rolling upgrade de cluster (nem o caminho de cluster nem o contêiner foram executados); a ordem acima vem das restrições
> gerais de middleware de mensagens e do Raft, bem como dos fatos de implementação de `pause_minority` / mudança de membros neste repositório, e não é uma conclusão medida nesta máquina.

---

## 8. Partes não suportadas / não verificadas (listadas explicitamente)

- **Downgrade entre grandes versões: não suportado, não verificado**. Se a nova versão já gravou dados em novo formato/nova semântica, **não** há garantia de "voltar ao binário antigo e ler normalmente";
  o rollback depende apenas do backup pré-atualização.
- **Formato de configuração inalterado**: continua sendo JSON + variáveis de ambiente `SWIFTMQ_*`. **Configuração YAML ainda não suportada** (requer trazer uma dependência de parser; M8-17 a avaliar),
  e a atualização não traz YAML.
- **Atualização a quente de plugins/protocolos online**: os plugins são compilados junto com o núcleo (forma A) ou iniciados por `spawn` conforme a configuração (forma B),
  e atualizar o núcleo = reiniciar o processo; **não** há mecanismo de substituição a quente do binário em execução.
- **Migração no local do mecanismo de armazenamento**: a rotação de segmentos/compactação de índice é comportamento de segundo plano em tempo de execução; **não** há comando dedicado de "migração/compactação de dados".
- **Atualização de cluster em rede real**: este repositório só fez caos em escala reduzida (kill em nível de processo) e **não** fez ensaio de atualização sob partição de rede ou disco cheio.
- Este documento **não inclui** nenhuma validação de transferência de dados entre o SwiftMQ e outro broker (RabbitMQ).
