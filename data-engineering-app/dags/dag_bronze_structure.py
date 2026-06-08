import sys
import os
import json
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

load_dotenv()

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

sys.path.append('/opt/airflow/source')

from utils.get_token import get_battle_net_access_token
from utils.get_league_data import get_league_data_raw
from utils.get_ladder import fetch_ladder_legacy_raw
from utils.get_current_season import get_current_season_id
from utils.bronze_schemas import validate_league_response, validate_legacy_ladder_response

CLIENT_ID = os.getenv('BLIZZARD_CLIENT_ID', 'COLOQUE_SEU_CLIENT_ID_AQUI')
CLIENT_SECRET = os.getenv('BLIZZARD_CLIENT_SECRET', 'COLOQUE_SEU_SECRET_AQUI')
BRONZE_PATH = '/opt/airflow/source/bronze'

# Ligas a coletar: 3=Platinum, 4=Diamond, 5=Master, 6=Grandmaster
LEAGUES = {
    3: 'platinum',
    4: 'diamond',
    5: 'master',
    6: 'grandmaster',
}


def _get_token(**context):
    token = get_battle_net_access_token(CLIENT_ID, CLIENT_SECRET)
    if not token:
        raise ValueError("Falha ao obter o token da Blizzard!")
    return token


def _get_current_season(**context):
    token = context['ti'].xcom_pull(task_ids='get_token')
    return get_current_season_id(token)


def _extract_leagues(**context):
    """
    Puxa os dados de estrutura de todas as ligas (divisões e ladder_ids).
    Salva um arquivo bronze por liga e retorna {league_id: [ladder_ids]} via XCom.
    Frequência: 1x/dia — estrutura de ligas não muda com frequência.
    """
    token = context['ti'].xcom_pull(task_ids='get_token')
    season_id = context['ti'].xcom_pull(task_ids='get_current_season')
    exec_date = context['logical_date'].strftime('%Y-%m-%d_%H%M')
    os.makedirs(BRONZE_PATH, exist_ok=True)

    all_ladder_ids = {}

    for league_id, league_name in LEAGUES.items():
        print(f"Baixando estrutura da liga {league_name} (id={league_id}), season {season_id}...")
        try:
            raw_data = get_league_data_raw(
                season_id=season_id, queue_id=201, team_type=0,
                league_id=league_id, token=token
            )
            validate_league_response(raw_data, league_id)
        except Exception as e:
            print(f"Erro ao buscar liga {league_name}: {e}")
            continue

        file_path = f"{BRONZE_PATH}/league_raw_{league_id}_{exec_date}.json"
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(raw_data, f)
        print(f"Salvo: {file_path}")

        ladder_ids = []
        for tier in raw_data.get('tier', []):
            for division in tier.get('division', []):
                ladder_ids.append(division['ladder_id'])
        all_ladder_ids[str(league_id)] = ladder_ids

    if not all_ladder_ids:
        raise ValueError("Nenhuma liga foi extraída com sucesso.")

    return all_ladder_ids


def _extract_legacy_ladders(**context):
    """
    Puxa o JSON legacy (nomes, clãs, divisões) para todos os ladders das ligas.
    Frequência: 1x/dia — dados de estrutura/membros não mudam a cada 10 minutos.
    """
    token = context['ti'].xcom_pull(task_ids='get_token')
    all_ladder_ids = context['ti'].xcom_pull(task_ids='extract_leagues')
    exec_date = context['logical_date'].strftime('%Y-%m-%d_%H%M')

    def fetch_legacy(ladder_id):
        try:
            data = fetch_ladder_legacy_raw(acesso=token, ladder_id=ladder_id)
            if data:
                validate_legacy_ladder_response(data, ladder_id)
                return {"ladder_id": ladder_id, "data": data}
        except Exception as e:
            print(f"Erro no ladder legacy {ladder_id}: {e}")
        return None

    for league_id_str, ladder_ids in all_ladder_ids.items():
        league_id = int(league_id_str)
        print(f"[liga {league_id}] Buscando {len(ladder_ids)} ladders legacy em paralelo...")

        legacy_raw_data = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(fetch_legacy, lid): lid for lid in ladder_ids}
            for future in as_completed(futures):
                result = future.result()
                if result:
                    legacy_raw_data.append(result)

        if legacy_raw_data:
            file_path = f"{BRONZE_PATH}/legacy_ladders_raw_{league_id}_{exec_date}.json"
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(legacy_raw_data, f)
            print(f"Salvo: {file_path} ({len(legacy_raw_data)} ladders)")
        else:
            print(f"Nenhuma ladder legacy processada para liga {league_id}.")


default_args = {
    'owner': 'capitao_sc2',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

with DAG(
    'bronze_structure',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['starcraft', 'esports', 'data_lake', 'structure'],
) as dag:

    get_token = PythonOperator(task_id='get_token', python_callable=_get_token)
    get_current_season = PythonOperator(task_id='get_current_season', python_callable=_get_current_season)
    extract_leagues = PythonOperator(task_id='extract_leagues', python_callable=_extract_leagues)
    extract_legacy_ladders = PythonOperator(task_id='extract_legacy_ladders', python_callable=_extract_legacy_ladders)

    get_token >> get_current_season >> extract_leagues >> extract_legacy_ladders
