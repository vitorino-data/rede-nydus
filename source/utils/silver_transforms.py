import os
import json
from datetime import datetime, timezone

import pandas as pd


def _parse_snapshot_ts(filepath: str) -> str:
    """
    Extrai o timestamp do snapshot a partir do nome do arquivo bronze.
    Suporta ambos os formatos:
      - modern_ladders_raw_4_2026-03-07_0440.json  (novo, multi-liga)
      - modern_ladders_raw_2026-03-07_0440.json    (legado, só diamond)
    """
    basename = os.path.basename(filepath).replace('.json', '')
    parts = basename.split('_')
    date_str = parts[-2]  # 2026-03-07
    time_str = parts[-1]  # 0440
    dt = datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H%M")
    return dt.strftime("%Y-%m-%d %H:%M:00")


def _parse_league_id(filepath: str) -> int:
    """
    Extrai o league_id do nome do arquivo bronze.
    Ex: modern_ladders_raw_4_2026-03-07_0440.json -> 4
    Arquivos legados (sem league_id no nome) retornam 4 (Diamond).
    """
    basename = os.path.basename(filepath).replace('.json', '')
    parts = basename.split('_')
    # O league_id fica na posição -3 no formato novo: ..._raw_<league_id>_<date>_<time>
    candidate = parts[-3]
    if candidate.isdigit():
        return int(candidate)
    return 4  # fallback para arquivos legados


def transform_league_to_silver(bronze_path: str) -> pd.DataFrame:
    """
    Bronze -> Silver: diamond_league_raw

    Achata a estrutura aninhada de tiers/divisões em uma tabela flat.
    Uma linha por divisão (ladder_id).
    """
    with open(bronze_path, 'r', encoding='utf-8') as f:
        raw = json.load(f)

    snapshot_ts = _parse_snapshot_ts(bronze_path)
    key = raw.get('key', {})
    rows = []

    for tier in raw.get('tier', []):
        for division in tier.get('division', []):
            rows.append({
                'season_id':       key.get('season_id'),
                'queue_id':        key.get('queue_id'),
                'league_id':       key.get('league_id'),
                'team_type':       key.get('team_type'),
                'tier_id':         tier.get('id'),
                'tier_min_rating': tier.get('min_rating'),
                'tier_max_rating': tier.get('max_rating'),
                'division_id':     division.get('id'),
                'ladder_id':       division.get('ladder_id'),
                'member_count':    division.get('member_count'),
                'snapshot_ts':     snapshot_ts,
            })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    int_cols = [
        'season_id', 'queue_id', 'league_id', 'team_type',
        'tier_id', 'tier_min_rating', 'tier_max_rating',
        'division_id', 'ladder_id', 'member_count',
    ]
    df[int_cols] = df[int_cols].astype('Int64')
    df['snapshot_ts'] = pd.to_datetime(df['snapshot_ts'])

    return df


def transform_modern_ladders_to_silver(bronze_path: str) -> pd.DataFrame:
    """
    Bronze -> Silver: modern_ladders_raw

    Achata os times e membros de todas as ladders em uma tabela flat.
    Uma linha por membro (jogador) por ladder.
    """
    with open(bronze_path, 'r', encoding='utf-8') as f:
        raw = json.load(f)

    snapshot_ts = _parse_snapshot_ts(bronze_path)
    league_id = _parse_league_id(bronze_path)
    rows = []

    for ladder_entry in raw:
        ladder_id = ladder_entry.get('ladder_id')
        teams = ladder_entry.get('data', {}).get('team', [])

        for team in teams:
            members = team.get('member', [])
            for member in members:
                legacy_link = member.get('legacy_link', {})
                character_link = member.get('character_link', {})
                race_counts = member.get('played_race_count', [])

                primary_race = None
                primary_race_count = None
                if race_counts:
                    top = race_counts[0]
                    primary_race = top.get('race', {}).get('en_US')
                    primary_race_count = top.get('count')

                rows.append({
                    'league_id':              league_id,
                    'ladder_id':              ladder_id,
                    # team_id é uint64 na API da Blizzard, guardamos como str para evitar overflow
                    'team_id':                str(team.get('id')),
                    'rating':                 team.get('rating'),
                    'wins':                   team.get('wins'),
                    'losses':                 team.get('losses'),
                    'ties':                   team.get('ties'),
                    'points':                 team.get('points'),
                    'longest_win_streak':     team.get('longest_win_streak'),
                    'current_win_streak':     team.get('current_win_streak'),
                    'current_rank':           team.get('current_rank'),
                    'highest_rank':           team.get('highest_rank'),
                    'previous_rank':          team.get('previous_rank'),
                    'join_time_stamp':        team.get('join_time_stamp'),
                    'last_played_time_stamp': team.get('last_played_time_stamp'),
                    'character_id':           legacy_link.get('id'),
                    'character_realm':        legacy_link.get('realm'),
                    'character_name':         legacy_link.get('name'),
                    'battle_tag':             character_link.get('battle_tag'),
                    'primary_race':           primary_race,
                    'primary_race_count':     primary_race_count,
                    'snapshot_ts':            snapshot_ts,
                })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    int_cols = [
        'league_id', 'ladder_id', 'rating', 'wins', 'losses', 'ties', 'points',
        'longest_win_streak', 'current_win_streak', 'current_rank',
        'highest_rank', 'previous_rank', 'join_time_stamp',
        'last_played_time_stamp', 'character_id', 'character_realm',
        'primary_race_count',
    ]
    df[int_cols] = df[int_cols].astype('Int64')

    # A API da Blizzard ocasionalmente retorna valores negativos como uint32
    # (ex: rating=-67 vira 4294967229). Valores fora do range do PostgreSQL INTEGER
    # são nulificados para evitar NumericValueOutOfRange no insert.
    INT32_MAX = 2_147_483_647
    INT32_MIN = -2_147_483_648
    int32_cols = [
        'league_id', 'ladder_id', 'rating', 'wins', 'losses', 'ties', 'points',
        'longest_win_streak', 'current_win_streak', 'current_rank',
        'highest_rank', 'previous_rank', 'character_id', 'character_realm',
        'primary_race_count',
    ]
    for col in int32_cols:
        if col in df.columns:
            df[col] = df[col].where(
                df[col].isna() | ((df[col] >= INT32_MIN) & (df[col] <= INT32_MAX)),
                other=pd.NA,
            )

    df['snapshot_ts'] = pd.to_datetime(df['snapshot_ts'])

    return df


def transform_legacy_ladders_to_silver(bronze_path: str) -> pd.DataFrame:
    """
    Bronze -> Silver: legacy_ladders_raw

    Achata os membros de todas as ladders em uma tabela flat.
    Uma linha por membro (jogador) por ladder.
    """
    with open(bronze_path, 'r', encoding='utf-8') as f:
        raw = json.load(f)

    snapshot_ts = _parse_snapshot_ts(bronze_path)
    league_id = _parse_league_id(bronze_path)
    rows = []

    for ladder_entry in raw:
        ladder_id = ladder_entry.get('ladder_id')
        members = ladder_entry.get('data', {}).get('ladderMembers', [])

        for member in members:
            character = member.get('character', {})
            rows.append({
                'league_id':    league_id,
                'ladder_id':    ladder_id,
                # character_id vem como string da API Legacy
                'character_id': character.get('id'),
                'realm':        character.get('realm'),
                'region':       character.get('region'),
                'display_name': character.get('displayName'),
                'clan_name':    character.get('clanName'),
                'clan_tag':     character.get('clanTag'),
                'profile_path': character.get('profilePath'),
                'join_timestamp': member.get('joinTimestamp'),
                'points':       member.get('points'),
                'wins':         member.get('wins'),
                'losses':       member.get('losses'),
                'highest_rank': member.get('highestRank'),
                'previous_rank': member.get('previousRank'),
                'favorite_race': member.get('favoriteRaceP1'),
                'snapshot_ts':  snapshot_ts,
            })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    int_cols = [
        'league_id', 'ladder_id', 'character_id', 'realm', 'region',
        'join_timestamp', 'points', 'wins', 'losses',
        'highest_rank', 'previous_rank',
    ]
    df[int_cols] = df[int_cols].astype('Int64')
    df['snapshot_ts'] = pd.to_datetime(df['snapshot_ts'])

    return df


def transform_match_history_to_silver(bronze_path: str) -> pd.DataFrame:
    """
    Bronze -> Silver: matches_all_history

    Flatten de um registro por jogador (com N partidas aninhadas) em uma
    tabela com uma linha por partida. A data unix é convertida para timestamp.
    """
    with open(bronze_path, 'r', encoding='utf-8') as f:
        raw = json.load(f)

    # snapshot_date extraído do nome do arquivo (ex: matches_all_history_2026-03-08_0200.json)
    basename = os.path.basename(bronze_path).replace('.json', '')
    parts = basename.split('_')
    snapshot_date = parts[-2]  # 2026-03-08

    rows = []
    for player_entry in raw:
        profile_id = player_entry.get('request_profile_id')
        realm_id   = player_entry.get('request_realm_id')
        region_id  = player_entry.get('request_region_id')

        for match in player_entry.get('matches', []):
            rows.append({
                'profile_id':    profile_id,
                'realm_id':      realm_id,
                'region_id':     region_id,
                'match_date':    datetime.fromtimestamp(match['date'], tz=timezone.utc).replace(tzinfo=None) if match.get('date') else None,
                'map':           match.get('map'),
                'type':          match.get('type'),
                'decision':      match.get('decision'),
                'speed':         match.get('speed'),
                'snapshot_date': snapshot_date,
            })

    df = pd.DataFrame(rows)

    if df.empty:
        return df

    int_cols = ['profile_id', 'realm_id', 'region_id']
    df[int_cols] = df[int_cols].astype('Int64')
    df['match_date'] = pd.to_datetime(df['match_date'])

    return df
