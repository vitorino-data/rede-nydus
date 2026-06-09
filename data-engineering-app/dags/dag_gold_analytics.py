from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'capitao_sc2',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# ==============================================================================
# DDL
# ==============================================================================

_DDL_GOLD_SCHEMA = "CREATE SCHEMA IF NOT EXISTS gold;"

_DDL_DIM_PLAYERS = """
CREATE TABLE IF NOT EXISTS gold.dim_players (
    character_id   INTEGER    NOT NULL,
    race           TEXT       NOT NULL,
    battle_tag     TEXT,
    display_name   TEXT,
    clan_tag       TEXT,
    legacy_names   JSONB      NOT NULL DEFAULT '[]',
    legacy_clans   JSONB      NOT NULL DEFAULT '[]',
    updated_at     TIMESTAMP  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (character_id, race)
);
"""

_DDL_FACT_MMR_TRACK = """
CREATE TABLE IF NOT EXISTS gold.fact_mmr_track (
    character_id  INTEGER    NOT NULL,
    race          TEXT       NOT NULL,
    snapshot_ts   TIMESTAMP  NOT NULL,
    ladder_id     INTEGER,
    rating        INTEGER,
    wins          INTEGER,
    losses        INTEGER,
    PRIMARY KEY (character_id, race, snapshot_ts)
);
CREATE INDEX IF NOT EXISTS idx_fact_mmr_track_lookup
    ON gold.fact_mmr_track (character_id, race, snapshot_ts DESC);
"""

_DDL_FACT_MATCHES = """
CREATE TABLE IF NOT EXISTS gold.fact_matches (
    match_id             TEXT       PRIMARY KEY,
    match_date           TIMESTAMP  NOT NULL,
    map                  TEXT,
    type                 TEXT,
    p1_character_id      INTEGER    NOT NULL,
    p1_decision          TEXT,
    p1_race_inferred     TEXT,
    p1_race_confidence   TEXT,
    p2_character_id      INTEGER,
    p2_decision          TEXT,
    p2_race_inferred     TEXT,
    p2_race_confidence   TEXT
);
CREATE INDEX IF NOT EXISTS idx_fact_matches_p1
    ON gold.fact_matches (p1_character_id, match_date);
CREATE INDEX IF NOT EXISTS idx_fact_matches_p2
    ON gold.fact_matches (p2_character_id, match_date);
"""

# ==============================================================================
# DML — dim_players
# Fonte: modern_ladder_teams (battle_tag, race) + legacy_ladder_members (display_name, clan_tag)
# Snapshot mais recente por (character_id, race).
# ON CONFLICT: atualiza campos atuais e faz append nos arrays legacy quando o valor muda.
# ==============================================================================

_DML_DIM_PLAYERS = """
WITH latest_modern AS (
    SELECT DISTINCT ON (character_id, primary_race)
        character_id,
        primary_race   AS race,
        battle_tag,
        character_name AS display_name_modern
    FROM silver.modern_ladder_teams
    WHERE primary_race IS NOT NULL
      AND character_id IS NOT NULL
    ORDER BY character_id, primary_race, snapshot_ts DESC
),
latest_legacy AS (
    SELECT DISTINCT ON (character_id)
        character_id,
        display_name,
        clan_tag
    FROM silver.legacy_ladder_members
    WHERE character_id IS NOT NULL
    ORDER BY character_id, snapshot_ts DESC
)
INSERT INTO gold.dim_players (character_id, race, battle_tag, display_name, clan_tag, updated_at)
SELECT
    m.character_id,
    m.race,
    m.battle_tag,
    COALESCE(l.display_name, m.display_name_modern) AS display_name,
    l.clan_tag,
    NOW()
FROM latest_modern m
LEFT JOIN latest_legacy l ON l.character_id = m.character_id
ON CONFLICT (character_id, race) DO UPDATE SET
    battle_tag   = EXCLUDED.battle_tag,
    legacy_names = CASE
        WHEN EXCLUDED.display_name IS NOT NULL
         AND gold.dim_players.display_name IS NOT NULL
         AND EXCLUDED.display_name IS DISTINCT FROM gold.dim_players.display_name
        THEN gold.dim_players.legacy_names || jsonb_build_array(
                jsonb_build_object('name', gold.dim_players.display_name, 'since', NOW()::date::text)
             )
        ELSE gold.dim_players.legacy_names
    END,
    legacy_clans = CASE
        WHEN EXCLUDED.clan_tag IS NOT NULL
         AND gold.dim_players.clan_tag IS NOT NULL
         AND EXCLUDED.clan_tag IS DISTINCT FROM gold.dim_players.clan_tag
        THEN gold.dim_players.legacy_clans || jsonb_build_array(
                jsonb_build_object('clan', gold.dim_players.clan_tag, 'since', NOW()::date::text)
             )
        ELSE gold.dim_players.legacy_clans
    END,
    display_name = EXCLUDED.display_name,
    clan_tag     = COALESCE(EXCLUDED.clan_tag, gold.dim_players.clan_tag),
    updated_at   = NOW();
"""

# ==============================================================================
# DML — fact_mmr_track
# Janela rolante de 3 dias. ON CONFLICT DO NOTHING — idempotente.
# ==============================================================================

_DML_FACT_MMR_TRACK = """
INSERT INTO gold.fact_mmr_track (character_id, race, snapshot_ts, ladder_id, rating, wins, losses)
SELECT
    character_id,
    primary_race AS race,
    snapshot_ts,
    ladder_id,
    rating,
    wins,
    losses
FROM silver.modern_ladder_teams
WHERE primary_race IS NOT NULL
  AND character_id IS NOT NULL
  AND snapshot_ts >= NOW() - INTERVAL '3 days'
ON CONFLICT (character_id, race, snapshot_ts) DO NOTHING;
"""

# ==============================================================================
# DML — fact_matches
# Reconstrói partidas 1v1 via self-join em silver.match_history.
# Janela rolante de 3 dias por snapshot_date.
#
# Inferência de raça (3 níveis de confiança):
#   HIGH    — jogador tem só uma raça em dim_players (certeza estrutural)
#   BOOSTED — jogador tem múltiplas raças, mas há match HIGH nos últimos 90 min
#             (session momentum: quem joga Zerg tende a continuar jogando Zerg)
#   LOW     — jogador flex sem evidência recente suficiente
#
# Correção para partidas sem adversário no dataset (p2=NULL):
#   Usa a raça com MAIOR MMR (raça principal) em vez da menor diferença para zero.
#
# ON CONFLICT DO UPDATE — reruns corrigem inferências existentes automaticamente.
# ==============================================================================

_DML_FACT_MATCHES = """
WITH
rolling_window AS (
    SELECT profile_id, match_date, map, type, decision
    FROM silver.match_history
    WHERE type = '1v1'
      AND snapshot_date >= CURRENT_DATE - INTERVAL '3 days'
),
matched_games AS (
    SELECT
        LEAST(m1.profile_id, m2.profile_id)    AS p1_id,
        GREATEST(m1.profile_id, m2.profile_id) AS p2_id,
        m1.match_date, m1.map, m1.type,
        CASE WHEN m1.profile_id < m2.profile_id THEN m1.decision ELSE m2.decision END AS p1_decision,
        CASE WHEN m1.profile_id < m2.profile_id THEN m2.decision ELSE m1.decision END AS p2_decision
    FROM rolling_window m1
    JOIN silver.match_history m2
      ON  m1.match_date  = m2.match_date
      AND m1.map         = m2.map
      AND m1.type        = m2.type
      AND m1.profile_id  < m2.profile_id
      AND m1.decision   != m2.decision
      AND m2.type = '1v1'
),
matched_profiles AS (
    SELECT p1_id AS profile_id, match_date, map FROM matched_games
    UNION ALL
    SELECT p2_id,               match_date, map FROM matched_games
),
unmatched_games AS (
    SELECT
        m.profile_id  AS p1_id,
        NULL::integer AS p2_id,
        m.match_date, m.map, m.type,
        m.decision    AS p1_decision,
        NULL::text    AS p2_decision
    FROM rolling_window m
    WHERE NOT EXISTS (
        SELECT 1 FROM matched_profiles mp
        WHERE mp.profile_id = m.profile_id
          AND mp.match_date = m.match_date
          AND mp.map        = m.map
    )
),
all_games AS (
    SELECT * FROM matched_games
    UNION ALL
    SELECT * FROM unmatched_games
),
latest_mmr AS (
    SELECT DISTINCT ON (character_id, race)
        character_id, race, rating
    FROM gold.fact_mmr_track
    ORDER BY character_id, race, snapshot_ts DESC
),
race_combinations AS (
    SELECT
        g.*,
        p1m.race   AS p1_race,
        p2m.race   AS p2_race,
        p1m.rating AS p1_rating,
        p2m.rating AS p2_rating,
        ROW_NUMBER() OVER (
            PARTITION BY g.p1_id, g.p2_id, g.match_date, g.map
            ORDER BY
                CASE
                    -- Sem adversário: usa raça com maior MMR (raça principal)
                    WHEN g.p2_id IS NULL THEN -COALESCE(p1m.rating, 0)
                    -- Com adversário: par de raças com menor diferença de MMR
                    ELSE ABS(COALESCE(p1m.rating, 0) - COALESCE(p2m.rating, 0))
                END ASC
        ) AS rn
    FROM all_games g
    LEFT JOIN latest_mmr p1m ON p1m.character_id = g.p1_id
    LEFT JOIN latest_mmr p2m ON p2m.character_id = g.p2_id
),
best_race AS (
    SELECT * FROM race_combinations WHERE rn = 1
),
player_race_count AS (
    SELECT character_id, COUNT(DISTINCT race) AS race_count
    FROM gold.dim_players
    GROUP BY character_id
),
initial_inference AS (
    SELECT
        br.*,
        CASE WHEN prc1.race_count = 1 THEN 'HIGH' ELSE 'LOW' END AS p1_confidence_raw,
        CASE
            WHEN br.p2_id IS NOT NULL AND prc2.race_count = 1 THEN 'HIGH'
            WHEN br.p2_id IS NOT NULL                         THEN 'LOW'
            ELSE NULL
        END AS p2_confidence_raw
    FROM best_race br
    LEFT JOIN player_race_count prc1 ON prc1.character_id = br.p1_id
    LEFT JOIN player_race_count prc2 ON prc2.character_id = br.p2_id
),
-- Visão unificada de classificações HIGH por jogador (aparece como p1 ou p2)
player_high_confidence AS (
    SELECT p1_id AS character_id, match_date, p1_race AS race
    FROM initial_inference
    WHERE p1_confidence_raw = 'HIGH' AND p1_race IS NOT NULL
    UNION ALL
    SELECT p2_id, match_date, p2_race
    FROM initial_inference
    WHERE p2_id IS NOT NULL AND p2_confidence_raw = 'HIGH' AND p2_race IS NOT NULL
),
-- Raça HIGH mais recente nos últimos 90 min para cada match LOW (posição p1)
p1_boost AS (
    SELECT DISTINCT ON (ii.p1_id, ii.match_date)
        ii.p1_id,
        ii.match_date,
        ph.race AS boosted_race
    FROM initial_inference ii
    JOIN player_high_confidence ph
      ON ph.character_id = ii.p1_id
      AND ph.match_date < ii.match_date
      AND ph.match_date >= ii.match_date - INTERVAL '90 minutes'
    WHERE ii.p1_confidence_raw = 'LOW'
    ORDER BY ii.p1_id, ii.match_date, ph.match_date DESC
),
-- Raça HIGH mais recente nos últimos 90 min para cada match LOW (posição p2)
p2_boost AS (
    SELECT DISTINCT ON (ii.p2_id, ii.match_date)
        ii.p2_id,
        ii.match_date,
        ph.race AS boosted_race
    FROM initial_inference ii
    JOIN player_high_confidence ph
      ON ph.character_id = ii.p2_id
      AND ph.match_date < ii.match_date
      AND ph.match_date >= ii.match_date - INTERVAL '90 minutes'
    WHERE ii.p2_id IS NOT NULL AND ii.p2_confidence_raw = 'LOW'
    ORDER BY ii.p2_id, ii.match_date, ph.match_date DESC
)
INSERT INTO gold.fact_matches (
    match_id,
    match_date, map, type,
    p1_character_id, p1_decision, p1_race_inferred, p1_race_confidence,
    p2_character_id, p2_decision, p2_race_inferred, p2_race_confidence
)
SELECT
    md5(ii.p1_id::text || COALESCE(ii.p2_id::text, '') || ii.match_date::text || ii.map::text) AS match_id,
    ii.match_date, ii.map, ii.type,
    ii.p1_id, ii.p1_decision,
    COALESCE(b1.boosted_race, ii.p1_race)    AS p1_race_inferred,
    CASE
        WHEN b1.boosted_race IS NOT NULL THEN 'BOOSTED'
        ELSE ii.p1_confidence_raw
    END                                      AS p1_race_confidence,
    ii.p2_id, ii.p2_decision,
    COALESCE(b2.boosted_race, ii.p2_race)    AS p2_race_inferred,
    CASE
        WHEN ii.p2_id IS NULL               THEN NULL
        WHEN b2.boosted_race IS NOT NULL    THEN 'BOOSTED'
        ELSE ii.p2_confidence_raw
    END                                      AS p2_race_confidence
FROM initial_inference ii
LEFT JOIN p1_boost b1 ON b1.p1_id    = ii.p1_id AND b1.match_date = ii.match_date
LEFT JOIN p2_boost b2 ON b2.p2_id    = ii.p2_id AND b2.match_date = ii.match_date
ON CONFLICT (match_id) DO UPDATE SET
    p1_race_inferred   = EXCLUDED.p1_race_inferred,
    p1_race_confidence = EXCLUDED.p1_race_confidence,
    p2_race_inferred   = EXCLUDED.p2_race_inferred,
    p2_race_confidence = EXCLUDED.p2_race_confidence;
"""

# ==============================================================================
# DAG
# ==============================================================================

with DAG(
    'dag_gold_analytics',
    default_args=default_args,
    description='Camada Gold: dim_players, fact_mmr_track, fact_matches',
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['gold', 'analytics', 'starcraft'],
) as dag:

    create_schema = PostgresOperator(
        task_id='create_gold_schema',
        postgres_conn_id='postgres_default',
        sql=_DDL_GOLD_SCHEMA,
    )

    create_dim_players = PostgresOperator(
        task_id='create_dim_players',
        postgres_conn_id='postgres_default',
        sql=_DDL_DIM_PLAYERS,
    )

    create_fact_mmr_track = PostgresOperator(
        task_id='create_fact_mmr_track',
        postgres_conn_id='postgres_default',
        sql=_DDL_FACT_MMR_TRACK,
    )

    create_fact_matches = PostgresOperator(
        task_id='create_fact_matches',
        postgres_conn_id='postgres_default',
        sql=_DDL_FACT_MATCHES,
    )

    load_dim_players = PostgresOperator(
        task_id='load_dim_players',
        postgres_conn_id='postgres_default',
        sql=_DML_DIM_PLAYERS,
    )

    load_fact_mmr_track = PostgresOperator(
        task_id='load_fact_mmr_track',
        postgres_conn_id='postgres_default',
        sql=_DML_FACT_MMR_TRACK,
    )

    load_fact_matches = PostgresOperator(
        task_id='load_fact_matches',
        postgres_conn_id='postgres_default',
        sql=_DML_FACT_MATCHES,
    )

    # DDLs em paralelo após criação do schema
    create_schema >> [create_dim_players, create_fact_mmr_track, create_fact_matches]

    # Carga em sequência: dim_players → fact_mmr_track → fact_matches
    # fact_matches depende do fact_mmr_track (inferência de raça) e do dim_players (confiança)
    [create_dim_players, create_fact_mmr_track, create_fact_matches] >> load_dim_players
    load_dim_players >> load_fact_mmr_track >> load_fact_matches
