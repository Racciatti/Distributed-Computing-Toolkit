# Relatório Técnico — Trabalho da disciplina de Computação Distribuída e Paralela

**Disciplina:** Computação Distribuída e Paralela (trabalho da disciplina) — UNESP
**Data:** 30 de junho de 2026

---

## 1. Arquitetura Geral

### Visão geral

O sistema é composto por 3 a 4 processos Python independentes que se comunicam exclusivamente via **sockets TCP**. Cada processo representa um nó do sistema distribuído e executa em seu próprio espaço de memória, com comunicação assíncrona baseada em troca de mensagens. Um arquivo texto (`shared_data.txt`) armazenado em disco atua como abstração de memória distribuída compartilhada: é um dicionário JSON que os nós leem, escrevem e deletam via operações coordenadas por protocolos distribuídos.

### Arquivo compartilhado como memória distribuída

`shared_data.txt` é um arquivo JSON plano gerenciado pela classe `SharedMemory` (`src/shared_memory.py`). Leituras e escritas locais são protegidas por `threading.Lock` dentro do processo, mas a consistência entre processos distintos é garantida pelos protocolos de coordenação implementados (2PC, exclusão mútua). O arquivo representa o estado global do sistema — qualquer operação de modificação deve passar pelos mecanismos de coordenação para garantir atomicidade e isolamento.

### Estrutura de pastas e responsabilidade dos módulos

```
distributed-computing/
├── src/
│   ├── messaging.py       # Transporte TCP: Message, LamportClock, send/broadcast/listener
│   ├── node.py            # Classe base Node: relógio, listener, send/broadcast, logging
│   ├── election.py        # Eleição de coordenador: Bully (ElectionNode) e Chang-Roberts (ChangRobertsNode)
│   ├── mutex.py           # Exclusão mútua: Ricart-Agrawala (MutexNode) e Maekawa (MaekawaMutexNode)
│   ├── transactions.py    # Transações 2PC: TransactionCoordinator e TransactionParticipant
│   ├── deadlock.py        # Detecção de deadlock: WaitForGraph e DeadlockDetector
│   ├── shared_memory.py   # Abstração de leitura/escrita no arquivo compartilhado
│   ├── ordering.py        # Ordenação causal (CausalNode/VectorClock) e total (TotalOrderNode)
│   ├── lock_manager.py    # Gerenciador de travas: LockTable, LockManagerNode, LockClientNode
│   └── wal.py             # Write-ahead log: WriteAheadLog e RecoverableParticipant
├── tests/
├── logs/                  # Logs por nó (node_X.log, wal_X.log)
├── shared_data.txt        # Memória distribuída compartilhada
└── REPORT.md
```

### Como os processos são iniciados e se comunicam

Cada módulo expõe um ponto de entrada via `python -m src.<módulo>`. Por exemplo:

```bash
# Eleição Bully — 3 terminais
python -m src.election 0 0,1,2
python -m src.election 1 0,1,2
python -m src.election 2 0,1,2

# Transações 2PC — 3 terminais
python -m src.transactions coordinator 0 0,1,2 1,2
python -m src.transactions participant 1 0,1,2
python -m src.transactions participant 2 0,1,2
```

Ao iniciar, cada nó chama `Node.start()`, que vincula um socket TCP na porta `5000 + node_id` e lança uma thread de escuta em segundo plano. Mensagens são transmitidas com um prefixo de comprimento de 4 bytes (big-endian) antes do JSON codificado em UTF-8. Para cada conexão aceita, o listener lança uma thread daemon adicional que desserializa a mensagem e invoca o callback `handle_message`.

---

## 2. Algoritmos Implementados

### Relógio de Lamport — `src/messaging.py` (`LamportClock`)

Estrutura inteira protegida por `threading.Lock`. Antes de cada envio, `increment()` incrementa o contador em 1; ao receber uma mensagem, `update(received)` aplica `max(local, recebido) + 1`. O valor corrente é embutido no campo `lamport_clock` de cada `Message`.

### Relógio Vetorial — `src/ordering.py` (`VectorClock`)

Mantém um dicionário `{node_id: contador}` inicializado a zero. `increment()` incrementa a entrada própria; `update(received)` aplica o máximo ponto a ponto e incrementa a própria entrada. A entrega causal é controlada por `can_deliver(sender_id, msg_vc)`: entregável somente quando `msg_vc[sender_id] == local[sender_id] + 1` e `msg_vc[k] <= local[k]` para todo outro nó `k`.

### Eleição Bully — `src/election.py` (`ElectionNode`)

Elege o nó com maior `node_id`. `start_election()`:
1. Se não há nós com ID maior, declara-se coordenador imediatamente.
2. Caso contrário, envia `ELECTION` a todos com ID maior e aguarda até `ELECTION_TIMEOUT = 2.0` s por `ALIVE`.
3. Se receber `ALIVE`, recua e aguarda `COORDINATOR` do nó superior.
4. Se o timeout expirar sem `ALIVE`, declara-se coordenador e faz broadcast de `COORDINATOR`.

### Eleição Chang-Roberts — `src/election.py` (`ChangRobertsNode`)

Anel lógico ordenado por `node_id`. Ao receber `CR_ELECTION`:
- `candidate_id > node_id`: encaminha ao vizinho direito.
- `candidate_id < node_id`: descarta.
- `candidate_id == node_id`: token completou o anel — faz broadcast de `CR_COORDINATOR`.

### Exclusão Mútua Ricart-Agrawala — `src/mutex.py` (`MutexNode`)

`acquire()` faz broadcast de `MUTEX_REQUEST` e aguarda N-1 respostas `MUTEX_REPLY`. Ao receber requisição, responde imediatamente se não está solicitando a SC ou se o requisitante tem prioridade maior (menor timestamp; empate desfeito por menor `node_id`). Caso contrário, adia em `_deferred`. `release()` envia `MUTEX_REPLY` para todos os adiados.

### Exclusão Mútua Maekawa — `src/mutex.py` (`MaekawaMutexNode`)

`acquire()` envia `MK_REQUEST` a membros do quórum e aguarda `MK_GRANT` de todos. Quando um membro detecta requisição de maior prioridade que o grantee atual, envia `MK_INQUIRE`; o grantee devolve via `MK_RELINQUISH` se ainda não entrou na SC. O mecanismo INQUIRE/RELINQUISH resolve o deadlock clássico do Maekawa em requisições simultâneas.

### Two-Phase Commit (2PC) — `src/transactions.py`

**Fase 1**: Coordenador gera `tx_id` UUID, envia `PREPARE` e aguarda votos por até 3 s. Participante valida operações e responde `VOTE_YES` (armazena ops em `_prepared`) ou `VOTE_NO`.

**Fase 2**: Todos `YES` → `COMMIT`; qualquer `NO` ou timeout → `ABORT`. Participante aplica ou descarta operações em `SharedMemory` e responde com `ACK`.

### Detecção de Deadlock — `src/deadlock.py`

`WaitForGraph` mantém lista de adjacência. Após cada `WAIT_FOR`, o detector executa DFS de três cores sobre o grafo. Ao detectar ciclo, a vítima é o nó com maior `tx_id`; suas arestas são removidas e `DEADLOCK_DETECTED` é enviado a todos.

### Ordenação Causal — `src/ordering.py` (`CausalNode`)

Mantém `VectorClock` e lista `_pending`. Ao receber `CAUSAL_MSG`, verifica `can_deliver`. Se entregável, atualiza relógio vetorial e tenta esvaziar `_pending` em loop (`_collect_deliverable`). Mensagens não entregáveis são enfileiradas.

### Ordenação Total — `src/ordering.py` (`TotalOrderNode`)

`broadcast_total()` envia `SEQUENCER_REQUEST` ao sequenciador. O sequenciador atribui número de sequência monotônico e faz broadcast de `SEQUENCER_DELIVER`. Cada nó insere em `_hold_back[seq]` e entrega em ordem a partir de `_next_seq`.

### Gerenciador de Travas — `src/lock_manager.py`

`LockTable` mantém por recurso `{"shared": set[tx_id], "exclusive": tx_id | None}`. `LockManagerNode` responde `LOCK_GRANTED` ou `LOCK_DENIED`. `LockClientNode` bloqueia em `threading.Event` até receber resposta. Upgrade concedido somente quando o requisitante é o único detentor compartilhado.

### Write-Ahead Log e Recuperação — `src/wal.py`

`WriteAheadLog` grava entradas JSON com `os.fsync` em `logs/wal_{node_id}.log`. Sequência: PREPARE → grava WAL → armazena ops → vota YES; COMMIT → aplica ops → grava WAL commit → ACK; ABORT → descarta → grava WAL abort → ACK. `recover_on_startup` identifica transações com `prepare` sem decisão final e as aborta conservadoramente.

---

## 3. Estruturas de Dados Principais

### `Message` (dataclass — `src/messaging.py`)

```python
@dataclass
class Message:
    type: str           # Tag de tipo (ex.: "ELECTION", "MUTEX_REQUEST")
    sender_id: int      # ID numérico do nó remetente
    payload: dict       # Dados arbitrários específicos de cada protocolo
    lamport_clock: int  # Valor do relógio de Lamport no momento do envio
```

Serialização via `json.dumps(asdict(self))`; transporte com prefixo de 4 bytes big-endian.

### `WaitForGraph` (lista de adjacência — `src/deadlock.py`)

```python
_graph: dict[str, list[str]]  # tx_id → lista de tx_ids que esse tx espera
```

`add_edge` (idempotente), `remove_edges_for`, `has_cycle` (DFS três cores), `find_cycles`. Thread-safe internamente.

### `LockTable` (`src/lock_manager.py`)

```python
_table: dict[str, dict]
# _table[resource] = {"shared": set[str], "exclusive": str | None}
```

### WAL (`src/wal.py`)

Arquivo `logs/wal_{node_id}.log`, uma entrada JSON por linha:

```
{"tx_id": "abc123", "phase": "prepare", "operations": [...], "timestamp": 1.0}
{"tx_id": "abc123", "phase": "commit",  "timestamp": 1.1}
```

### `_hold_back` em `TotalOrderNode`

```python
_hold_back: dict[int, Message]  # seq_number → mensagem
_next_seq: int                  # próximo número de sequência a entregar
```

### `_pending` em `CausalNode`

```python
_pending: list[Message]  # mensagens aguardando condição de entrega causal
```

---

## 4. Formato de Mensagens

| Tipo | Módulo | Remetente | Destinatário | Payload principal |
|---|---|---|---|---|
| `ELECTION` | `election.py` | Nó iniciando eleição | Todos com ID maior | `{}` |
| `ALIVE` | `election.py` | Nó com ID maior | Remetente do ELECTION | `{}` |
| `COORDINATOR` | `election.py` | Vencedor (Bully) | Todos (broadcast) | `{"coordinator_id": int}` |
| `CR_ELECTION` | `election.py` | Qualquer nó | Vizinho direito no anel | `{"candidate_id": int}` |
| `CR_COORDINATOR` | `election.py` | Vencedor (Chang-Roberts) | Todos (broadcast) | `{"coordinator_id": int}` |
| `MUTEX_REQUEST` | `mutex.py` | Nó solicitando SC | Todos os pares (broadcast) | `{"request_clock": int}` |
| `MUTEX_REPLY` | `mutex.py` | Qualquer par | Solicitante | `{}` |
| `MK_REQUEST` | `mutex.py` | Nó solicitando SC | Membros do quórum | `{"request_clock": int}` |
| `MK_GRANT` | `mutex.py` | Membro do quórum | Solicitante | `{}` |
| `MK_RELEASE` | `mutex.py` | Nó saindo da SC | Membros do quórum | `{}` |
| `MK_INQUIRE` | `mutex.py` | Membro do quórum | Grantee atual | `{}` |
| `MK_RELINQUISH` | `mutex.py` | Grantee atual | Membro do quórum | `{}` |
| `PREPARE` | `transactions.py` | Coordenador | Participantes | `{"tx_id": str, "operations": list}` |
| `VOTE_YES` | `transactions.py` | Participante | Coordenador | `{"tx_id": str}` |
| `VOTE_NO` | `transactions.py` | Participante | Coordenador | `{"tx_id": str}` |
| `COMMIT` | `transactions.py` | Coordenador | Participantes | `{"tx_id": str}` |
| `ABORT` | `transactions.py` | Coordenador | Participantes | `{"tx_id": str}` |
| `ACK` | `transactions.py` | Participante | Coordenador | `{"tx_id": str}` |
| `WAIT_FOR` | `deadlock.py` | Nó worker | Detector | `{"waiter": str, "holder": str}` |
| `RELEASE` | `deadlock.py` | Nó worker | Detector | `{"tx_id": str}` |
| `DEADLOCK_DETECTED` | `deadlock.py` | Detector | Todos (broadcast) | `{"cycles": list, "victim": str}` |
| `LOCK_REQUEST` | `lock_manager.py` | Cliente | Gerenciador | `{"tx_id": str, "resource": str, "lock_type": str}` |
| `LOCK_GRANTED` | `lock_manager.py` | Gerenciador | Cliente | `{"tx_id": str, "resource": str}` |
| `LOCK_DENIED` | `lock_manager.py` | Gerenciador | Cliente | `{"tx_id": str, "resource": str, "reason": str}` |
| `LOCK_RELEASE` | `lock_manager.py` | Cliente | Gerenciador | `{"tx_id": str}` |
| `LOCK_UPGRADE` | `lock_manager.py` | Cliente | Gerenciador | `{"tx_id": str, "resource": str}` |
| `SEQUENCER_REQUEST` | `ordering.py` | Qualquer nó | Sequenciador | `{"original_type": str, "original_payload": dict}` |
| `SEQUENCER_DELIVER` | `ordering.py` | Sequenciador | Todos (broadcast) | `{"seq": int, "original_type": str, "original_payload": dict}` |
| `CAUSAL_MSG` | `ordering.py` | Qualquer nó | Todos (broadcast) | `{"vc": dict, "text": str, ...}` |

---

## 5. Fluxogramas

### Algoritmo Bully (Eleição)

```
Nó N detecta coordenador morto
            |
            v
   N tem nó com ID maior?
       /           \
     NÃO            SIM
      |              |
      v              v
  Declara-se    Envia ELECTION
  coordenador   a todos com ID > N
      |              |
      v              v
  Broadcast       Aguarda ALIVE
  COORDINATOR     (timeout 2s)
                 /           \
           ALIVE recebido    timeout
                |                |
                v                v
         Aguarda            Declara-se
         COORDINATOR        coordenador
         do superior            |
                                v
                           Broadcast
                           COORDINATOR
```

### Ricart-Agrawala (acquire/release)

```
Nó A quer entrar na SC
            |
            v
  Incrementa relógio Lamport
  Registra request_clock
  Broadcast MUTEX_REQUEST
            |
            v
  Aguarda N-1 MUTEX_REPLY
            |
  [Para cada MUTEX_REQUEST recebido de B]
            |
      A está em SC ou
   prioridade(A) > prioridade(B)?
         /           \
        SIM           NÃO
         |             |
    Adia REPLY       Envia MUTEX_REPLY
    para B           imediatamente
            |
            v
  Todos N-1 REPLY recebidos
            |
            v
  Entra na SC → release() → envia REPLY aos adiados
```

### Two-Phase Commit (2PC)

```
Coordenador inicia transação
            |
            v
       Gera tx_id único
            |
  --- FASE 1: VOTAÇÃO ---
            v
  Envia PREPARE a cada participante
            |
            v
  Aguarda VOTE_YES/NO (timeout 3s)
           / \
     Todos YES  Algum NO ou timeout
          |            |
          v            v
       COMMIT        ABORT
  --- FASE 2: DECISÃO ---
          |            |
          v            v
  Envia COMMIT    Envia ABORT
  a cada part.   a cada part.
          \            /
           v          v
  Participante: COMMIT → aplica ops → ACK
                ABORT  → descarta    → ACK
                    |
                    v
        Coordenador recebe todos ACKs → concluído
```

### Detecção de Deadlock

```
Worker envia WAIT_FOR {waiter=T1, holder=T2}
                  |
                  v
      Detector adiciona aresta T1→T2
                  |
                  v
      Executa DFS no grafo
                  |
          Ciclo encontrado?
             /         \
            SIM          NÃO
             |            |
             v            v
  Coleta nós do ciclo   Aguarda próxima
  Vítima = max(tx_ids)   mensagem WAIT_FOR
             |
             v
  Remove arestas da vítima
             |
             v
  Broadcast DEADLOCK_DETECTED {cycles, victim}
```

---

## 6. Análise de Complexidade de Mensagens

| Algoritmo | Mensagens por operação | Observação |
|---|---|---|
| **Bully** | O(n²) no pior caso | Cascata de eleições quando todos os nós iniciam simultaneamente |
| **Chang-Roberts** | O(n²) pior caso, O(n log n) esperado | Tokens fracos eliminados cedo em média |
| **Ricart-Agrawala** | 2(n-1) por entrada na SC | (n-1) REQUEST + (n-1) REPLY; sem token nem coordenador |
| **Maekawa** | O(3√n) com INQUIRE/RELINQUISH | Quórum de tamanho ≈ √n; INQUIRE adiciona até √n mensagens extras |
| **2PC** | 4n por transação | n PREPARE + n VOTE + n COMMIT/ABORT + n ACK |
| **Ordenação Causal** | 0 overhead de mensagens | Vetor de relógio embutido no payload; sem mensagens extras |
| **Ordenação Total (sequenciador)** | 2(n-1) por broadcast | 1 SEQUENCER_REQUEST + (n-1) SEQUENCER_DELIVER |

---

## 7. Comparações Teóricas

### Bully × Chang-Roberts

| Critério | Bully | Chang-Roberts |
|---|---|---|
| **Topologia** | Grafo completo | Anel lógico unidirecional |
| **Critério de vitória** | Maior node_id | Maior node_id (token que retorna à origem) |
| **Complexidade (pior caso)** | O(n²) | O(n²) |
| **Complexidade (caso médio)** | O(n²) | O(n log n) |
| **Tolerância a falhas** | Robusto: paraleliza ELECTION | Vulnerável: falha do vizinho direito perde o token |
| **Convergência** | Garantida com rede confiável | Garantida somente com anel íntegro |
| **Simplicidade** | Mais simples de implementar | Mais elegante em topologia anel |

O Bully é mais adequado para clusters em rede local sem restrição de topologia. Chang-Roberts é mais eficiente em média, porém exige que o anel permaneça conectado — uma falha de nó intermediário interrompe a eleição sem retentativa.

### Ricart-Agrawala × Maekawa

| Critério | Ricart-Agrawala | Maekawa |
|---|---|---|
| **Mensagens por SC** | 2(n-1) | O(3√n) |
| **Escalabilidade** | Degrada linearmente com n | Melhor para clusters grandes |
| **Coordenador central** | Não | Não |
| **Deadlock intrínseco** | Não — regra de prioridade evita | Sim sem INQUIRE — resolvido na implementação |
| **Complexidade de implementação** | Moderada | Alta (INQUIRE/RELINQUISH necessários) |

Ricart-Agrawala é preferível para clusters pequenos. Maekawa torna-se vantajoso quando n é grande e 2(n-1) mensagens por SC é proibitivo. Ambos foram completamente implementados.

### FIFO × Causal × Total

| Critério | FIFO | Causal | Total |
|---|---|---|---|
| **Garantia** | Ordem por remetente | Ordem de precursores causais | Mesma ordem global em todos os nós |
| **Overhead** | Nenhum (TCP garante) | Vetor de relógio no payload | SEQUENCER_REQUEST + SEQUENCER_DELIVER |
| **Hold-back** | Não | Sim (`_pending`) | Sim (`_hold_back`) |
| **Ponto único de falha** | Não | Não | Sim (sequenciador) |
| **Caso de uso** | Chat básico | Bancos de dados replicados | Replicação de máquina de estados |

---

## 8. Estratégias de Recuperação e Tolerância a Falhas

### Write-Ahead Log

O `WriteAheadLog` implementa o princípio _log first, apply second_ com `os.fsync` após cada append:

- **PREPARE**: WAL gravado antes de armazenar operações. Garante que a preparação sobreviva a uma queda antes do voto.
- **COMMIT**: Operações aplicadas ao `SharedMemory` primeiro; WAL commit gravado imediatamente depois.
- **ABORT**: Operações descartadas, WAL abort gravado.
- **Recuperação**: lê o WAL completo, identifica `prepare` sem decisão final, e grava `abort` para cada um.

### Timeout no Coordenador 2PC

O coordenador aguarda votos por `_VOTE_TIMEOUT = 3.0` s. Votos não recebidos resultam em ABORT automático.

### Detecção e Resolução de Deadlock

A vítima é o nó com maior `tx_id` (comparação lexicográfica — determinístico). Suas arestas são removidas do grafo e `DEADLOCK_DETECTED` é enviado a todos os pares.

### Limitações de Tolerância a Falhas

- **Sem replicação**: `shared_data.txt` existe apenas localmente.
- **Sem consenso distribuído**: Não há Raft ou Paxos; split-brain é possível em partição de rede.
- **2PC é bloqueante**: Falha do coordenador após PREPARE mas antes da decisão bloqueia participantes indefinidamente.

---

## 9. Limitações Encontradas

### Chang-Roberts: token perdido sem retentativa

Se o vizinho direito estiver inativo, o `OSError` é capturado e logado, mas o token é perdido silenciosamente. A eleição fica travada indefinidamente sem mecanismo de retentativa.

### Maekawa: deadlock clássico resolvido via INQUIRE/RELINQUISH

O algoritmo original sofre de deadlock quando múltiplos nós enviam `MK_REQUEST` simultaneamente com o mesmo timestamp. A implementação **resolve esse problema** com INQUIRE/RELINQUISH: quando um membro do quórum detecta requisição de maior prioridade que o grantee atual, envia `MK_INQUIRE`; o grantee devolve o grant via `MK_RELINQUISH`, é re-enfileirado e será servido após o nó de maior prioridade concluir sua SC.

### 2PC não é tolerante à falha do coordenador

Protocolo bloqueante por natureza. Se o coordenador falhar após a fase 1 mas antes de transmitir a decisão, os participantes ficam indefinidamente em estado preparado. O 3PC resolveria esse problema, mas não foi implementado.

### WAL: abort conservador pode causar rollback desnecessário

Se o participante falhar após aplicar operações mas antes de gravar `commit` no WAL, a recuperação aborta a transação. As operações já aplicadas ao arquivo permanecem no disco em estado inconsistente até o coordenador retentar.

### Sem persistência de estado de eleição entre reinicializações

`ElectionNode` e `ChangRobertsNode` mantêm `coordinator_id` apenas em memória. Ao reiniciar, o nó não conhece o coordenador até que uma nova eleição seja disparada.

### Ordenação total via sequenciador: ponto único de falha

Se o sequenciador falhar, `broadcast_total()` ficará bloqueado indefinidamente. Uma solução seria combinar o papel de sequenciador com o coordenador eleito, mas isso não foi implementado.
