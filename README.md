# Rede Nydus: StarCraft II Data Engineering Pipeline

## Resumo do Projeto
Este projeto é um pipeline de Engenharia de Dados focado no consumo das APIs oficiais da Blizzard (StarCraft II). O objetivo principal é extrair dados de performance, acompanhar variações estruturais das ligas e de MMR (Matchmaking Rating), além de armazenar históricos de partidas para análises posteriores e aplicação de regras de negócio.

O repositório está estruturado segundo práticas de engenharia de dados e orquestrado pelo **Apache Airflow**.

---

## Princípios de Arquitetura

O projeto adota a arquitetura em camadas **Medallion Architecture** (Bronze, Silver, Gold) fundamentada no formato **ELT** (Extract, Load, Transform).

*   **Camada Bronze:** Responsável pela extração do dado cru (*RAW*), em formato `JSON`. Seu design foca em garantir o salvamento exato do payload da API de origem. Nenhuma validação de negócio, tipagem explícita de colunas ou união de tabelas complexas ocorre neste momento. Isso minimiza a perda de dados em casos de flutuações, campos imprevistos ou falhas de formatação por parte da Blizzard.

*   **Camada Silver:** Responsável pela transformação, normalização e carga dos dados brutos em um banco relacional estruturado (PostgreSQL, schema `silver`). Aplica limpeza de tipos, achatamento de arrays aninhados (*flatten*), deduplicação via `ON CONFLICT DO NOTHING` e controle de idempotência por meio de uma tabela de log de arquivos processados (`silver.processed_files`). Não aplica regras de negócio — apenas garante a integridade e consultabilidade dos dados.

*   **Camada Gold:** Responsável pela agregação analítica dos dados silver em tabelas prontas para consumo por ferramentas de BI (Metabase). Aplica regras de negócio e enriquecimento de dados.

---

## Infraestrutura (Docker Compose)

O stack local é composto por quatro serviços:

| Serviço | Imagem | Porta | Finalidade |
|---|---|---|---|
| `postgres_airflow` | postgres:15 | interno | Metadata do Airflow (DAG runs, XCom, conexões) |
| `postgres_data` | postgres:15 | 5434 | Dados do pipeline (schemas silver, gold) + metadata do Metabase |
| `airflow` | apache/airflow:2.7.2 | 8081 | Orquestrador de DAGs |
| `metabase` | metabase/metabase:latest | 3001 | Visualização e análise dos dados |

As credenciais são gerenciadas via arquivo `.env` (não versionado). Consulte `.env.example` para o template.

---

## Árvore do Projeto (Diretórios de Interesse)

```text
rede-nydus/
├── data-engineering-app/
│   ├── docker-compose.yml              # Stack completo: 2x postgres, airflow, metabase
│   ├── init-db/
│   │   └── 01_create_metabase_db.sql   # Cria banco 'metabase' na primeira inicialização
│   └── dags/
│       ├── dag_bronze_structure.py         # Coleta diária de estrutura das 4 ligas
│       ├── dag_bronze_mmr_tracker.py       # Snapshots de MMR a cada 10 minutos
│       ├── dag_bronze_match_history.py     # Coleta incremental de histórico de partidas (12h)
│       ├── dag_silver_snapshots.py         # Transforma bronze → silver (snapshots/MMR)
│       ├── dag_silver_match_history.py     # Transforma bronze → silver.match_history (polling 30min)
│       └── dag_gold_analytics.py           # Agrega silver → gold (analytics diário)
└── source/
    ├── bronze/                         # Data Lake: arquivos RAW JSON particionados por timestamp
    └── utils/                          # Controladores independentes para requisições e carga
        ├── get_token.py                # Gestão de OAUTH Access Token
        ├── get_league_data.py          # Requisições em nível global de Ligas/Tiers
        ├── get_ladder.py               # Processamento individual de Ladders
        ├── get_match_history.py        # Recuperação de informações históricas restritas
        ├── get_current_season.py       # Consulta a season SC2 atual via API da Blizzard
        ├── bronze_schemas.py           # Validação de payloads da API (schemas esperados)
        ├── silver_transforms.py        # Funções de transformação Bronze → Silver (Pandas)
        └── silver_loader.py            # Funções de carga no PostgreSQL + controle de idempotência
```

---

## Documentação Técnica

### 1. DAGs da Camada Bronze

#### `dag_bronze_structure.py`
Pipeline de execução diária (`@daily`). Coleta a estrutura estática das ligas (divisões, ladder IDs) e os dados legacy (nomes, clãs) para todas as 4 ligas monitoradas: Platinum (3), Diamond (4), Master (5) e Grandmaster (6). A season atual é obtida dinamicamente via API.

**Trilha de Execução (`Tasks`):**
1.  **`get_token`**: Autentica na Blizzard via OAuth2 e deposita o token no `XCom`.
2.  **`get_current_season`**: Consulta `GET /sc2/ladder/season/1` para obter o `season_id` atual dinamicamente — elimina o risco de dados da season errada.
3.  **`extract_leagues`**: Para cada uma das 4 ligas, acessa o endpoint de estrutura da API e salva `league_raw_{league_id}_{YYYY-MM-DD_HHMM}.json`. Valida o payload via `bronze_schemas.validate_league_response`.
4.  **`extract_legacy_ladders`**: Para cada ladder mapeado, consulta a API legada (`/legacy/`) e salva `legacy_ladders_raw_{league_id}_{YYYY-MM-DD_HHMM}.json`. Valida via `bronze_schemas.validate_legacy_ladder_response`.

---

#### `dag_bronze_mmr_tracker.py`
Pipeline de execução contínua (janelas de 10 minutos). Registra snapshots consecutivos de MMR para criar granularidade de flutuações intra-diárias de rating, sem consumir a cota da API com re-extração da estrutura.

**Trilha de Execução (`Tasks`):**
1.  **`get_token`**: Autentica na Blizzard via OAuth2.
2.  **`load_ladder_ids`**: Lê o arquivo `league_raw_{id}_*.json` mais recente do disco para cada liga.
3.  **`extract_modern_ladders`**: Para cada liga, acessa o endpoint principal da API de Dados (`/data/`) e salva `modern_ladders_raw_{league_id}_{YYYY-MM-DD_HHMM}.json`. Valida via `bronze_schemas.validate_modern_ladder_response`.

*Padrão de saída:* `modern_ladders_raw_{league_id}_{YYYY-MM-DD_HHMM}.json`

---

#### `dag_bronze_match_history.py`
Pipeline de execução a cada 12 horas (`0 */12 * * *`). Coleta o histórico de partidas de forma **incremental**: apenas jogadores com alteração no total de jogos desde a última execução são consultados na API (~43.000 jogadores únicos entre as 4 ligas).

**Estratégia incremental:**
- Na primeira execução: full load de todos os jogadores com pelo menos 1 jogo.
- Nas execuções seguintes: compara `wins + losses` atual com `silver.match_history_checkpoint`. Apenas jogadores com diferença são consultados, reduzindo o volume de requisições em ~75%.

**Trilha de Execução (`Tasks`):**
1.  **`get_token`**: Autentica na Blizzard via OAuth2.
2.  **`load_players`**: Lê os arquivos `legacy_ladders_raw_{id}_*.json` mais recentes, deduplica por `character_id`, compara com o checkpoint e salva `players_list_{exec_date}.json` apenas com jogadores que tiveram novos jogos.
3.  **`extract_matches`**: Para cada jogador elegível, consulta o endpoint de histórico. Usa token bucket (6 req/s, 15 workers) e salva checkpoint a cada 500 jogadores em `matches_all_history_{exec_date}.json`.
4.  **`update_checkpoint`**: Atualiza `silver.match_history_checkpoint` com o total de jogos de cada jogador processado.

---

### 2. DAGs da Camada Silver

A camada Silver transforma os arquivos JSON brutos da camada Bronze em tabelas relacionais normalizadas no PostgreSQL (schema `silver`). O controle de idempotência é garantido pela tabela `silver.processed_files`.

#### `dag_silver_snapshots.py`
Pipeline de execução contínua (10 minutos), sincronizada com o `dag_bronze_mmr_tracker`. Processa uma janela dos arquivos mais recentes por liga. O tamanho da janela é configurável via Airflow Variable `silver_snapshots_window_size` (padrão: 10). Emite alerta de `[LAG]` no log quando há mais de `2 × window_size` arquivos pendentes.

**Tasks (executadas em paralelo):**
- **`process_league`**: Transforma `league_raw_{id}_*.json` → `silver.league_divisions`
- **`process_modern_ladders`**: Transforma `modern_ladders_raw_{id}_*.json` → `silver.modern_ladder_teams`
- **`process_legacy_ladders`**: Transforma `legacy_ladders_raw_{id}_*.json` → `silver.legacy_ladder_members`

**Tabelas resultantes:**

| Tabela | Granularidade | Chave Primária |
|---|---|---|
| `silver.league_divisions` | 1 linha por divisão por snapshot | `(ladder_id, snapshot_ts)` |
| `silver.modern_ladder_teams` | 1 linha por jogador por ladder por snapshot | `(ladder_id, character_id, snapshot_ts)` |
| `silver.legacy_ladder_members` | 1 linha por membro por ladder por snapshot | `(ladder_id, character_id, snapshot_ts)` |

---

#### `dag_silver_match_history.py`
Pipeline de execução por polling (`*/30 * * * *`). Verifica a cada 30 minutos se há novos arquivos `matches_all_history_*.json` não processados e os transforma em `silver.match_history`. Desacoplado do bronze — não depende de trigger direto.

**Trilha de Execução (`Tasks`):**
1.  **`process_match_history`**: Lê o arquivo mais recente não processado, achata a estrutura aninhada (1 linha por partida) e converte timestamps Unix para `datetime`.
2.  **`load_to_postgres`**: Carrega na tabela `silver.match_history` com `ON CONFLICT DO NOTHING`.

**Tabela resultante:**

| Tabela | Granularidade | Chave Primária |
|---|---|---|
| `silver.match_history` | 1 linha por partida por jogador | `(profile_id, realm_id, region_id, match_date)` |

---

### 3. Controle de Idempotência e Checkpoint

#### `silver.processed_files`
Todas as tasks da silver consultam esta tabela antes de processar qualquer arquivo. Se o arquivo já consta, a task pula silenciosamente. Ao concluir com sucesso, registra o nome do arquivo.

#### `silver.match_history_checkpoint`
Registra o total de jogos (`wins + losses`) por `character_id` após cada execução bem-sucedida do `dag_bronze_match_history`. Permite a lógica incremental: somente jogadores com contador diferente do último valor registrado são consultados na API na próxima rodada.

```
silver.match_history_checkpoint
  └── character_id  INTEGER (PK)
  └── total_games   INTEGER
  └── updated_at    TIMESTAMP
```

---

### 4. DAG da Camada Gold

#### `dag_gold_analytics.py`
Pipeline de execução diária (`@daily`). Agrega os dados das tabelas silver em tabelas analíticas prontas para consumo no Metabase.

**Tabelas resultantes:**

| Tabela | Descrição | Chave Primária |
|---|---|---|
| `gold.player_stats_daily` | Partidas, vitórias, derrotas e win rate por jogador por dia | `(profile_id, match_date)` |
| `gold.mmr_checkpoints` | Snapshots de MMR por jogador por ladder ao longo do tempo | `(ladder_id, profile_id, snapshot_ts)` |
| `gold.player_mmr_daily` | MMR máximo e atual, BattleTag e clan tag por jogador por dia | `(profile_id, snapshot_date)` |

> `gold.fact_matches` (partidas enriquecidas com inferência de oponente e raça) está implementada mas desativada temporariamente — aguarda volume de dados suficiente para validação.

---

## Guias e Restrições de Desenvolvimento

1.  **Gestão de Rate Limits**: A cota da Blizzard é de 36.000 req/h. O `dag_bronze_match_history` usa token bucket de 6 req/s (~21.600 req/h), deixando margem para o `dag_bronze_mmr_tracker` e crescimento futuro de ligas.

2.  **Isolamento da Ingestão / Arquitetura ELT**: As bibliotecas em `source/utils/` sob a batuta da camada Bronze não efetuam manipulação com Pandas ou junções. Cada extração guarda sua própria assinatura estrutural. O join entre metadados legados e vigentes é delegado à camada Silver.

3.  **Segurança de Credenciais**: As chaves `BLIZZARD_CLIENT_ID` e `BLIZZARD_CLIENT_SECRET` são gerenciadas via arquivo `.env` (listado no `.gitignore`) e injetadas no container via `env_file`. Nunca devem ser hardcoded no código.

4.  **Alterações no schema Gold**: As DAGs são montadas via volume (`./dags:/opt/airflow/dags`), portanto edições em `dag_gold_analytics.py` são refletidas sem recriar o container.

---

## RoadMap de Engenharia

*   ~~Criação da **Camada Silver**~~ ✅ Tabelas `league_divisions`, `modern_ladder_teams`, `legacy_ladder_members` e `match_history` no schema `silver`.
*   ~~**Separação de bancos** (Airflow metadata vs dados)~~ ✅ `postgres_airflow` e `postgres_data` isolados.
*   ~~**Season dinâmica**~~ ✅ `get_current_season_id` consulta a API em cada execução.
*   ~~**Carga incremental de match history**~~ ✅ Checkpoint via `silver.match_history_checkpoint`.
*   ~~**Camada Gold inicial**~~ ✅ `player_stats_daily`, `mmr_checkpoints`, `player_mmr_daily`.
*   **`gold.fact_matches`**: Partidas enriquecidas com inferência de oponente — aguarda validação com dados reais.
*   **Migração para GCP**: Cloud SQL (dois instances), Airflow containerizado em GCE/Cloud Run, Bronze files no GCS.
*   **Monitoramento de jogadores super-ativos**: DAG de alta frequência para jogadores com >N jogos por janela (backlog).
