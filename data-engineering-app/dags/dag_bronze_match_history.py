import sys
import os
import time
import json
import glob
import threading
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Tenta carregar o arquivo .env se estiver rodando localmente (fora do docker)
load_dotenv()

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

sys.path.append('/opt/airflow/source')

from utils.get_token import get_battle_net_access_token
from utils.get_match_history import fetch_match_history_raw
from utils.bronze_schemas import validate_match_history_response
from utils.silver_loader import get_checkpoint_game_counts, upsert_match_history_checkpoint

CLIENT_ID = os.getenv('BLIZZARD_CLIENT_ID', 'COLOQUE_SEU_CLIENT_ID_AQUI')
CLIENT_SECRET = os.getenv('BLIZZARD_CLIENT_SECRET', 'COLOQUE_SEU_SECRET_AQUI')
BRONZE_PATH = '/opt/airflow/source/bronze'

# Ligas cobertas: 3=Platinum, 4=Diamond, 5=Master, 6=Grandmaster
LEAGUE_IDS = [3, 4, 5, 6]

# Salva progresso no disco a cada N jogadores para não perder dados
# em execuções longas que possam ser interrompidas
CHECKPOINT_EVERY = 500

# Rate limiter: 6 req/s → 21.600 req/h (~60% da cota de 36k)
# Deixa margem para o dag_bronze_mmr_tracker e para crescimento futuro de ligas
RATE_LIMIT_RPS = 6
MATCH_HISTORY_WORKERS = 15


class _RateLimiter:
    """
    Token bucket simples baseado em Semaphore.
    Uma thread dedicada repõe 1 token a cada (1/rps) segundos.
    Workers bloqueiam em acquire() até um token estar disponível,
    eliminando o sleep fixo por worker e aproveitando o paralelismo de I/O.
    """
    def __init__(self, calls_per_second: float):
        self._sem = threading.Semaphore(0)
        interval = 1.0 / calls_per_second
        def _refill():
            while True:
                self._sem.release()
                time.sleep(interval)
        threading.Thread(target=_refill, daemon=True).start()

    def acquire(self):
        self._sem.acquire()


def _get_token(**context):
    token = get_battle_net_access_token(CLIENT_ID, CLIENT_SECRET)
    if not token:
        raise ValueError("Falha ao obter o token da Blizzard!")
    return token


def _load_players(**context):
    """
    Camada Bronze - Lê os arquivos legacy_ladders_raw_{league_id}_{date}.json,
    compara o total de jogos atual (wins + losses) com silver.match_history_checkpoint
    e retorna apenas os jogadores que tiveram novos jogos desde a última execução.

    Primeira execução: checkpoint vazio → full load de todos os jogadores.
    Execuções seguintes: carrega apenas quem mudou → reduz requisições drasticamente.

    Jogadores com 0 jogos são ignorados (sem match history para buscar).
    Salva players_list_{exec_date}.json no disco e retorna o caminho via XCom.
    """
    exec_date = context['logical_date'].strftime('%Y-%m-%d_%H%M')
    os.makedirs(BRONZE_PATH, exist_ok=True)

    # character_id → {id, realm, region, current_games}
    unique_players = {}

    for league_id in LEAGUE_IDS:
        files = sorted(glob.glob(f"{BRONZE_PATH}/legacy_ladders_raw_{league_id}_*.json"))
        if not files:
            print(f"Nenhum arquivo legacy_ladders_raw_{league_id} encontrado, pulando liga {league_id}.")
            continue

        latest = files[-1]
        print(f"Liga {league_id}: lendo {os.path.basename(latest)}")

        with open(latest, 'r', encoding='utf-8') as f:
            ladders_data = json.load(f)

        for ladder_entry in ladders_data:
            members = ladder_entry.get('data', {}).get('ladderMembers', [])
            for member in members:
                char = member.get('character', {})
                if char and char.get('id') and char.get('realm') and char.get('region'):
                    char_id = char['id']
                    current_games = (member.get('wins') or 0) + (member.get('losses') or 0)

                    if char_id not in unique_players:
                        unique_players[char_id] = {
                            'id': char_id,
                            'realm': char['realm'],
                            'region': char['region'],
                            'current_games': current_games,
                        }
                    else:
                        # Jogador em múltiplas ligas: mantém o maior contador visto
                        unique_players[char_id]['current_games'] = max(
                            unique_players[char_id]['current_games'], current_games
                        )

    checkpoint = get_checkpoint_game_counts()

    changed_players = [
        p for p in unique_players.values()
        if p['current_games'] > 0
        and p['current_games'] != checkpoint.get(int(p['id']), -1)
    ]

    total = len(unique_players)
    changed = len(changed_players)
    skipped = total - changed
    first_run = len(checkpoint) == 0
    print(
        f"{'[PRIMEIRA EXECUÇÃO — full load] ' if first_run else ''}"
        f"Jogadores totais: {total} | "
        f"Com novos jogos: {changed} | "
        f"Sem alteração (pulados): {skipped}"
    )

    players_file = f"{BRONZE_PATH}/players_list_{exec_date}.json"
    with open(players_file, 'w', encoding='utf-8') as f:
        json.dump(changed_players, f)
    print(f"Lista salva em: {players_file}")

    return players_file


def _update_checkpoint(**context):
    """
    Atualiza silver.match_history_checkpoint com o total de jogos de cada
    jogador que teve match history coletado nesta execução.
    Executada somente após _extract_matches concluir com sucesso — garante que
    o checkpoint só avança quando a extração de fato aconteceu.
    """
    players_file = context['ti'].xcom_pull(task_ids='load_players')
    if not players_file:
        print("Nenhum arquivo de jogadores encontrado, pulando atualização do checkpoint.")
        return

    with open(players_file, 'r', encoding='utf-8') as f:
        players = json.load(f)

    if not players:
        print("Nenhum jogador para atualizar no checkpoint.")
        return

    player_counts = {int(p['id']): p['current_games'] for p in players}
    upsert_match_history_checkpoint(player_counts)
    print(f"Checkpoint atualizado para {len(player_counts)} jogadores.")


def _extract_matches(**context):
    """
    Camada Bronze - Extrai match history de todos os jogadores via API da Blizzard.

    - Processa as 4 ligas sem limites artificiais.
    - Salva checkpoint a cada CHECKPOINT_EVERY jogadores: garante que nenhum
      dado coletado seja perdido em caso de falha ou timeout da execução.
    - Saída: matches_all_history_{exec_date}.json
    """
    token = context['ti'].xcom_pull(task_ids='get_token')
    players_file = context['ti'].xcom_pull(task_ids='load_players')
    exec_date = context['logical_date'].strftime('%Y-%m-%d_%H%M')
    out_file = f"{BRONZE_PATH}/matches_all_history_{exec_date}.json"

    os.makedirs(BRONZE_PATH, exist_ok=True)

    if not players_file:
        print("Nenhum arquivo de jogadores encontrado. Verifique se dag_bronze_structure já rodou.")
        return

    with open(players_file, 'r', encoding='utf-8') as f:
        players = json.load(f)

    if not players:
        print("Nenhum jogador com novos jogos detectado. Execução encerrada sem chamadas à API.")
        return

    print(f"Iniciando extração de match history para {len(players)} jogadores...")

    all_matches_raw = []
    rate_limiter = _RateLimiter(RATE_LIMIT_RPS)

    def fetch_player_matches(player):
        try:
            rate_limiter.acquire()
            data = fetch_match_history_raw(
                token, player['region'], player['realm'], player['id']
            )
            if data and data.get('matches'):
                validate_match_history_response(data, player['id'])
                return data
        except Exception as e:
            print(f"Erro ao buscar matches do jogador {player['id']}: {e}")
        return None

    # Processa em lotes de CHECKPOINT_EVERY — salva progresso parcial após cada lote
    for batch_start in range(0, len(players), CHECKPOINT_EVERY):
        batch = players[batch_start: batch_start + CHECKPOINT_EVERY]
        batch_end = min(batch_start + CHECKPOINT_EVERY, len(players))
        print(f"[lote] Processando jogadores {batch_start + 1}–{batch_end} de {len(players)}...")

        with ThreadPoolExecutor(max_workers=MATCH_HISTORY_WORKERS) as executor:
            futures = {executor.submit(fetch_player_matches, p): p for p in batch}
            for future in as_completed(futures):
                result = future.result()
                if result:
                    all_matches_raw.append(result)

        # Checkpoint: persiste o acumulado até aqui para não perder progresso
        with open(out_file, 'w', encoding='utf-8') as f:
            json.dump(all_matches_raw, f, ensure_ascii=False)
        print(f"Checkpoint salvo: {len(all_matches_raw)} registros em {out_file}")

    print(f"Extração concluída: {len(all_matches_raw)} jogadores com partidas coletadas.")


default_args = {
    'owner': 'capitao_sc2',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

with DAG(
    'bronze_match_history_scraper',
    default_args=default_args,
    schedule_interval='0 */12 * * *',
    catchup=False,
    max_active_runs=1,
    tags=['starcraft', 'esports', 'data_lake', 'match_history'],
) as dag:

    get_token = PythonOperator(
        task_id='get_token',
        python_callable=_get_token,
    )

    load_players = PythonOperator(
        task_id='load_players',
        python_callable=_load_players,
    )

    extract_matches = PythonOperator(
        task_id='extract_matches',
        python_callable=_extract_matches,
    )

    update_checkpoint = PythonOperator(
        task_id='update_checkpoint',
        python_callable=_update_checkpoint,
    )

    get_token >> load_players >> extract_matches >> update_checkpoint
