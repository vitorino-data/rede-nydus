import os

import pandas as pd
import psycopg2
import psycopg2.extras


def _get_connection():
    conn_str = os.getenv(
        'SILVER_DB_CONN',
        'postgresql://user:password@postgres_data:5432/mydatabase',
    )
    return psycopg2.connect(conn_str)


# ==============================================================================
# DDL — tabelas criadas automaticamente na primeira execução
# loaded_at é preenchido pelo banco com NOW() no momento do INSERT
# ==============================================================================

_DDL_LEAGUE_DIVISIONS = """
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.league_divisions (
    season_id       SMALLINT,
    queue_id        SMALLINT,
    league_id       SMALLINT,
    team_type       SMALLINT,
    tier_id         SMALLINT,
    tier_min_rating INTEGER,
    tier_max_rating INTEGER,
    division_id     INTEGER,
    ladder_id       INTEGER   NOT NULL,
    member_count    SMALLINT,
    snapshot_ts     TIMESTAMP NOT NULL,
    loaded_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ladder_id, snapshot_ts)
);
"""

_DDL_MODERN_LADDER_TEAMS = """
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.modern_ladder_teams (
    league_id              SMALLINT,
    ladder_id              INTEGER   NOT NULL,
    team_id                TEXT,
    rating                 INTEGER,
    wins                   INTEGER,
    losses                 INTEGER,
    ties                   INTEGER,
    points                 INTEGER,
    longest_win_streak     INTEGER,
    current_win_streak     INTEGER,
    current_rank           INTEGER,
    highest_rank           INTEGER,
    previous_rank          INTEGER,
    join_time_stamp        BIGINT,
    last_played_time_stamp BIGINT,
    character_id           INTEGER   NOT NULL,
    character_realm        SMALLINT,
    character_name         TEXT,
    battle_tag             TEXT,
    primary_race           TEXT,
    primary_race_count     INTEGER,
    snapshot_ts            TIMESTAMP NOT NULL,
    loaded_at              TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ladder_id, character_id, snapshot_ts)
);
"""

_DDL_LEGACY_LADDER_MEMBERS = """
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.legacy_ladder_members (
    league_id     SMALLINT,
    ladder_id     INTEGER   NOT NULL,
    character_id  INTEGER   NOT NULL,
    realm         SMALLINT,
    region        SMALLINT,
    display_name  TEXT,
    clan_name     TEXT,
    clan_tag      TEXT,
    profile_path  TEXT,
    join_timestamp BIGINT,
    points        INTEGER,
    wins          INTEGER,
    losses        INTEGER,
    highest_rank  INTEGER,
    previous_rank INTEGER,
    favorite_race TEXT,
    snapshot_ts   TIMESTAMP NOT NULL,
    loaded_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ladder_id, character_id, snapshot_ts)
);
"""


_DDL_MATCH_HISTORY = """
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.match_history (
    profile_id    INTEGER   NOT NULL,
    realm_id      SMALLINT  NOT NULL,
    region_id     SMALLINT  NOT NULL,
    match_date    TIMESTAMP NOT NULL,
    map           TEXT,
    type          TEXT,
    decision      TEXT,
    speed         TEXT,
    snapshot_date DATE      NOT NULL,
    loaded_at     TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (profile_id, realm_id, region_id, match_date)
);
"""

_DDL_PROCESSED_FILES = """
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.processed_files (
    filename     TEXT      PRIMARY KEY,
    processed_at TIMESTAMP NOT NULL DEFAULT NOW()
);
"""

_DDL_MATCH_HISTORY_CHECKPOINT = """
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.match_history_checkpoint (
    character_id  INTEGER   NOT NULL,
    total_games   INTEGER   NOT NULL,
    updated_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (character_id)
);
"""


def ensure_silver_tables(cursor) -> None:
    """Cria o schema e as tabelas silver se ainda não existirem (idempotente)."""
    cursor.execute(_DDL_LEAGUE_DIVISIONS)
    cursor.execute(_DDL_MODERN_LADDER_TEAMS)
    cursor.execute(_DDL_LEGACY_LADDER_MEMBERS)
    cursor.execute(_DDL_MATCH_HISTORY)
    cursor.execute(_DDL_PROCESSED_FILES)
    cursor.execute(_DDL_MATCH_HISTORY_CHECKPOINT)


def is_file_processed(filename: str) -> bool:
    """Retorna True se o arquivo já foi registrado em silver.processed_files."""
    with _get_connection() as conn:
        with conn.cursor() as cur:
            ensure_silver_tables(cur)
            cur.execute(
                "SELECT 1 FROM silver.processed_files WHERE filename = %s",
                (filename,)
            )
            return cur.fetchone() is not None


def mark_file_processed(filename: str) -> None:
    """Registra o arquivo como processado em silver.processed_files."""
    with _get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO silver.processed_files (filename) VALUES (%s) ON CONFLICT DO NOTHING",
                (filename,)
            )


# ==============================================================================
# Helpers
# ==============================================================================

def _to_python(v):
    """Converte valores pandas (NA, Timestamp, numpy scalars) para tipos Python nativos."""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    # numpy.int64, numpy.float64, numpy.bool_, etc. -> tipo Python nativo
    if hasattr(v, 'item'):
        return v.item()
    return v


def _df_to_rows(df: pd.DataFrame, cols: list) -> list:
    """Converte DataFrame em lista de tuplas para psycopg2.extras.execute_values."""
    return [
        tuple(_to_python(v) for v in row)
        for row in df[cols].itertuples(index=False)
    ]


# ==============================================================================
# Funções de carga por tabela
# ==============================================================================

def load_league_divisions(df: pd.DataFrame) -> int:
    """
    Insere os dados de league_divisions na tabela silver.
    Retorna o número de linhas efetivamente inseridas.
    ON CONFLICT DO NOTHING — reruns são seguros.
    """
    cols = list(df.columns)  # loaded_at não está no df; o banco preenche com DEFAULT
    rows = _df_to_rows(df, cols)

    sql = f"""
        INSERT INTO silver.league_divisions ({', '.join(cols)})
        VALUES %s
        ON CONFLICT (ladder_id, snapshot_ts) DO NOTHING
    """
    with _get_connection() as conn:
        with conn.cursor() as cur:
            ensure_silver_tables(cur)
            psycopg2.extras.execute_values(cur, sql, rows)
            inserted = cur.rowcount
    print(f"[league_divisions] {inserted} linhas inseridas / {len(rows)} processadas.")
    return inserted


def load_modern_ladder_teams(df: pd.DataFrame) -> int:
    """
    Insere os dados de modern_ladder_teams na tabela silver.
    Filtra linhas sem character_id (não podem compor a PK).
    ON CONFLICT DO NOTHING — reruns são seguros.
    """
    df = df.dropna(subset=['character_id']).copy()
    cols = list(df.columns)
    rows = _df_to_rows(df, cols)

    sql = f"""
        INSERT INTO silver.modern_ladder_teams ({', '.join(cols)})
        VALUES %s
        ON CONFLICT (ladder_id, character_id, snapshot_ts) DO NOTHING
    """
    with _get_connection() as conn:
        with conn.cursor() as cur:
            ensure_silver_tables(cur)
            psycopg2.extras.execute_values(cur, sql, rows)
            inserted = cur.rowcount
    print(f"[modern_ladder_teams] {inserted} linhas inseridas / {len(rows)} processadas.")
    return inserted


def load_legacy_ladder_members(df: pd.DataFrame) -> int:
    """
    Insere os dados de legacy_ladder_members na tabela silver.
    Filtra linhas sem character_id (não podem compor a PK).
    ON CONFLICT DO NOTHING — reruns são seguros.
    """
    df = df.dropna(subset=['character_id']).copy()
    cols = list(df.columns)
    rows = _df_to_rows(df, cols)

    sql = f"""
        INSERT INTO silver.legacy_ladder_members ({', '.join(cols)})
        VALUES %s
        ON CONFLICT (ladder_id, character_id, snapshot_ts) DO NOTHING
    """
    with _get_connection() as conn:
        with conn.cursor() as cur:
            ensure_silver_tables(cur)
            psycopg2.extras.execute_values(cur, sql, rows)
            inserted = cur.rowcount
    print(f"[legacy_ladder_members] {inserted} linhas inseridas / {len(rows)} processadas.")
    return inserted


def get_checkpoint_game_counts() -> dict:
    """
    Retorna {character_id: total_games} para todos os jogadores registrados
    em silver.match_history_checkpoint.
    Retorna dict vazio na primeira execução (sem checkpoint ainda).
    """
    with _get_connection() as conn:
        with conn.cursor() as cur:
            ensure_silver_tables(cur)
            cur.execute("SELECT character_id, total_games FROM silver.match_history_checkpoint")
            return {row[0]: row[1] for row in cur.fetchall()}


def upsert_match_history_checkpoint(player_counts: dict) -> None:
    """
    Atualiza total_games no checkpoint para os jogadores fornecidos.
    player_counts: {character_id: total_games}
    """
    if not player_counts:
        return

    rows = list(player_counts.items())
    sql = """
        INSERT INTO silver.match_history_checkpoint (character_id, total_games)
        VALUES %s
        ON CONFLICT (character_id) DO UPDATE SET
            total_games = EXCLUDED.total_games,
            updated_at  = NOW()
    """
    with _get_connection() as conn:
        with conn.cursor() as cur:
            ensure_silver_tables(cur)
            psycopg2.extras.execute_values(cur, sql, rows)
    print(f"[match_history_checkpoint] {len(rows)} registros atualizados.")


def load_match_history(df: pd.DataFrame) -> int:
    """
    Insere o histórico de partidas na tabela silver.match_history.
    Filtra linhas sem match_date (não podem compor a PK).
    ON CONFLICT DO NOTHING — reruns são seguros.
    """
    df = df.dropna(subset=['profile_id', 'match_date']).copy()
    cols = list(df.columns)
    rows = _df_to_rows(df, cols)

    sql = f"""
        INSERT INTO silver.match_history ({', '.join(cols)})
        VALUES %s
        ON CONFLICT (profile_id, realm_id, region_id, match_date) DO NOTHING
    """
    with _get_connection() as conn:
        with conn.cursor() as cur:
            ensure_silver_tables(cur)
            psycopg2.extras.execute_values(cur, sql, rows)
            inserted = cur.rowcount
    print(f"[match_history] {inserted} linhas inseridas / {len(rows)} processadas.")
    return inserted
