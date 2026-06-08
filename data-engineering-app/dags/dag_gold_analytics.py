from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Argumentos padrão
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG
with DAG(
    'dag_gold_analytics',
    default_args=default_args,
    description='Pipeline de transformaçao Camada Gold (Analytics)',
    schedule_interval='@daily',
    catchup=False,
    tags=['gold', 'analytics', 'postgres'],
) as dag:

    # 1. Criação do Schema Gold (se não existir)
    create_schema = PostgresOperator(
        task_id='create_gold_schema',
        postgres_conn_id='postgres_default',
        sql="CREATE SCHEMA IF NOT EXISTS gold;"
    )

    # ------------------------------------------------------------------
    # Tabela 1: Estatísticas Diárias por Jogador (Player Stats Daily)
    # ------------------------------------------------------------------

    # Cria tabela se não existir
    create_player_stats_ddl = PostgresOperator(
        task_id='create_gold_player_stats_daily_ddl',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS gold.player_stats_daily (
                profile_id INT,
                match_date DATE,
                matches_played INT,
                wins INT,
                losses INT,
                win_rate FLOAT,
                updated_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (profile_id, match_date)
            );
        """
    )
    
    # Popula com dados agregados da camada Silver (Upsert)
    populate_player_stats_dml = PostgresOperator(
        task_id='populate_gold_player_stats_daily_dml',
        postgres_conn_id='postgres_default',
        sql="""
            INSERT INTO gold.player_stats_daily (profile_id, match_date, matches_played, wins, losses, win_rate, updated_at)
            SELECT
                profile_id,
                DATE(match_date) as match_date,
                COUNT(*) as matches_played,
                COUNT(*) FILTER (WHERE decision = 'WIN') as wins,
                COUNT(*) FILTER (WHERE decision = 'LOSS') as losses,
                CASE 
                    WHEN COUNT(*) > 0 THEN (COUNT(*) FILTER (WHERE decision = 'WIN')::FLOAT / COUNT(*)) 
                    ELSE 0 
                END as win_rate,
                NOW()
            FROM silver.match_history
            GROUP BY profile_id, DATE(match_date)
            ON CONFLICT (profile_id, match_date) 
            DO UPDATE SET
                matches_played = EXCLUDED.matches_played,
                wins = EXCLUDED.wins,
                losses = EXCLUDED.losses,
                win_rate = EXCLUDED.win_rate,
                updated_at = EXCLUDED.updated_at;
        """
    )

    # ------------------------------------------------------------------
    # Tabela 2: Histórico de MMR e Stats (Checkpoints)
    # ------------------------------------------------------------------

    # DDL: MMR Checkpoints
    create_mmr_checkpoints_ddl = PostgresOperator(
        task_id='create_gold_mmr_checkpoints_ddl',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS gold.mmr_checkpoints (
                ladder_id INT,
                profile_id INT,
                snapshot_ts TIMESTAMP,
                rating INT,
                wins INT,
                losses INT,
                primary_race TEXT,
                updated_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (ladder_id, profile_id, snapshot_ts)
            );
        """
    )

    # DML: Popula MMR Checkpoints (Upsert)
    populate_mmr_checkpoints_dml = PostgresOperator(
        task_id='populate_gold_mmr_checkpoints_dml',
        postgres_conn_id='postgres_default',
        sql="""
            INSERT INTO gold.mmr_checkpoints (ladder_id, profile_id, snapshot_ts, rating, wins, losses, primary_race, updated_at)
            SELECT
                ladder_id,
                character_id as profile_id,
                snapshot_ts,
                rating,
                wins,
                losses,
                primary_race,
                NOW()
            FROM silver.modern_ladder_teams
            ON CONFLICT (ladder_id, profile_id, snapshot_ts)
            DO UPDATE SET
                rating = EXCLUDED.rating,
                wins = EXCLUDED.wins,
                losses = EXCLUDED.losses,
                primary_race = EXCLUDED.primary_race,
                updated_at = EXCLUDED.updated_at;
        """
    )


    # ------------------------------------------------------------------
    # Tabela 3: Fato Partidas Enriquecidas (Fact Matches) - DESATIVADA TEMPORARIAMENTE
    # ------------------------------------------------------------------

    # DDL: Fact Matches
    # create_fact_matches_ddl = PostgresOperator(
    #     task_id='create_gold_fact_matches_ddl',
    #     postgres_conn_id='postgres_default',
    #     sql="""
    #         CREATE TABLE IF NOT EXISTS gold.fact_matches (
    #             match_id TEXT PRIMARY KEY,
    #             match_date TIMESTAMP NOT NULL,
    #             map_name TEXT,
    #             game_type TEXT,
    #             
    #             -- Player 1
    #             p1_profile_id INT,
    #             p1_result TEXT,
    #             p1_race_inferred TEXT,
    #             p1_mmr_inferred INT,
    #             
    #             -- Player 2 (Oponente)
    #             p2_profile_id INT,
    #             p2_result TEXT,
    #             p2_race_inferred TEXT,
    #             p2_mmr_inferred INT,
    #             
    #             updated_at TIMESTAMP DEFAULT NOW()
    #         );
    #     """
    # )

    # DML: Popula Fact Matches (Lógica Complexa de Inferência)
    # populate_fact_matches_dml = PostgresOperator(
    #     task_id='populate_gold_fact_matches_dml',
    #     postgres_conn_id='postgres_default',
    #     sql="""
    #         WITH raw_matches AS (
    #             -- Auto-join para encontrar oponente (P1 vs P2)
    #             SELECT
    #                 m1.profile_id as p1_id,
    #                 m1.decision as p1_result,
    #                 m2.profile_id as p2_id,
    #                 m2.decision as p2_result,
    #                 m1.match_date,
    #                 m1.map,
    #                 m1.type
    #             FROM silver.match_history m1
    #             JOIN silver.match_history m2 
    #                 ON m1.map = m2.map 
    #                 AND m1.type = m2.type 
    #                 -- Tolerância rígida: < 1 segundo de diferença para match exato
    #                 AND m1.match_date BETWEEN m2.match_date - INTERVAL '1 second' AND m2.match_date + INTERVAL '1 second'
    #                 AND m1.profile_id < m2.profile_id -- Evita duplicatas (A vs B e B vs A)
    #         ),
    #         enriched_matches AS (
    #             SELECT
    #                 rm.*,
    #                 -- Inferência P1: Snapshot mais próximo (usando LATERAL)
    #                 p1_snap.primary_race as p1_race,
    #                 p1_snap.rating as p1_mmr,
    #                 -- Inferência P2: Snapshot mais próximo (usando LATERAL)
    #                 p2_snap.primary_race as p2_race,
    #                 p2_snap.rating as p2_mmr
    #             FROM raw_matches rm
    #             LEFT JOIN LATERAL (
    #                 SELECT primary_race, rating 
    #                 FROM silver.modern_ladder_teams t
    #                 WHERE t.character_id = rm.p1_id
    #                 ORDER BY ABS(EXTRACT(EPOCH FROM (t.snapshot_ts - rm.match_date))) ASC
    #                 LIMIT 1
    #             ) p1_snap ON TRUE
    #             LEFT JOIN LATERAL (
    #                 SELECT primary_race, rating 
    #                 FROM silver.modern_ladder_teams t
    #                 WHERE t.character_id = rm.p2_id
    #                 ORDER BY ABS(EXTRACT(EPOCH FROM (t.snapshot_ts - rm.match_date))) ASC
    #                 LIMIT 1
    #             ) p2_snap ON TRUE
    #         )
    #         INSERT INTO gold.fact_matches (
    #             match_id, match_date, map_name, game_type,
    #             p1_profile_id, p1_result, p1_race_inferred, p1_mmr_inferred,
    #             p2_profile_id, p2_result, p2_race_inferred, p2_mmr_inferred,
    #             updated_at
    #         )
    #         SELECT
    #             md5(p1_id::text || p2_id::text || match_date::text) as match_id,
    #             match_date, map, type,
    #             p1_id, p1_result, p1_race, p1_mmr,
    #             p2_id, p2_result, p2_race, p2_mmr,
    #             NOW()
    #         FROM enriched_matches
    #         ON CONFLICT (match_id) DO NOTHING;
    #     """
    # )


    # ------------------------------------------------------------------
    # Tabela 4: Monitoramento de MMR Diário e Clã (MMR Tracker)
    # ------------------------------------------------------------------

    # Objetivo: Acompanhar evolução de MMR e verificar regras de campeonato (teto de MMR)
    # Fonte: silver.modern_ladder_teams (MMR) + silver.legacy_ladder_members (Clan Tag)

    create_mmr_stats_ddl = PostgresOperator(
        task_id='create_gold_player_mmr_daily_ddl',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS gold.player_mmr_daily (
                profile_id INT,
                snapshot_date DATE,
                battle_tag TEXT,
                clan_tag TEXT,
                primary_race TEXT,
                max_mmr INT,
                current_mmr INT,
                wins INT,
                losses INT,
                updated_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (profile_id, snapshot_date)
            );
        """
    )

    populate_mmr_stats_dml = PostgresOperator(
        task_id='populate_gold_player_mmr_daily_dml',
        postgres_conn_id='postgres_default',
        sql="""
            WITH joined_data AS (
                SELECT
                    m.character_id,
                    DATE(m.snapshot_ts) as snapshot_date,
                    m.rating,
                    m.wins,
                    m.losses,
                    m.snapshot_ts,
                    m.battle_tag,
                    m.primary_race,
                    l.clan_tag
                FROM silver.modern_ladder_teams m
                LEFT JOIN silver.legacy_ladder_members l
                   ON m.ladder_id = l.ladder_id
                   AND m.character_id = l.character_id
                   AND m.snapshot_ts = l.snapshot_ts
            ),
            daily_aggregated AS (
                SELECT
                    character_id,
                    snapshot_date,
                    MAX(rating) as max_mmr,
                    (ARRAY_AGG(rating ORDER BY snapshot_ts DESC))[1] as current_mmr,
                    (ARRAY_AGG(wins ORDER BY snapshot_ts DESC))[1] as current_wins,
                    (ARRAY_AGG(losses ORDER BY snapshot_ts DESC))[1] as current_losses,
                    (ARRAY_AGG(battle_tag ORDER BY snapshot_ts DESC))[1] as battle_tag,
                    (ARRAY_AGG(clan_tag ORDER BY snapshot_ts DESC))[1] as clan_tag,
                    (ARRAY_AGG(primary_race ORDER BY snapshot_ts DESC))[1] as primary_race
                FROM joined_data
                GROUP BY character_id, snapshot_date
            )
            INSERT INTO gold.player_mmr_daily (
                profile_id, snapshot_date, battle_tag, clan_tag, primary_race, 
                max_mmr, current_mmr, wins, losses, updated_at
            )
            SELECT
                character_id,
                snapshot_date,
                battle_tag,
                clan_tag,
                primary_race,
                max_mmr,
                current_mmr,
                current_wins,
                current_losses,
                NOW()
            FROM daily_aggregated
            ON CONFLICT (profile_id, snapshot_date)
            DO UPDATE SET
                max_mmr = GREATEST(gold.player_mmr_daily.max_mmr, EXCLUDED.max_mmr),
                current_mmr = EXCLUDED.current_mmr,
                wins = EXCLUDED.wins,
                losses = EXCLUDED.losses,
                clan_tag = COALESCE(EXCLUDED.clan_tag, gold.player_mmr_daily.clan_tag),
                updated_at = EXCLUDED.updated_at;
        """
    )

    # Definição de Dependências
    
    # CriaSchema >> DDLs >> DMLs
    create_schema >> create_player_stats_ddl >> populate_player_stats_dml
    create_schema >> create_mmr_checkpoints_ddl >> populate_mmr_checkpoints_dml
    # create_schema >> create_fact_matches_ddl >> populate_fact_matches_dml

