import sys
import os
import time
import json
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed

# Tenta carregar o arquivo .env se estiver rodando localmente (fora do docker)
load_dotenv()

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Adiciona a pasta source ao path do Python interno do Airflow (graças ao volume que mapeamos!)
sys.path.append('/opt/airflow/source')

from utils.get_token import get_battle_net_access_token
from utils.get_ladder import fetch_ladder_modern_raw
from utils.bronze_schemas import validate_modern_ladder_response

# Credenciais vindas de Váriaveis de Ambiente (Para não expor no GitHub)
CLIENT_ID = os.getenv('BLIZZARD_CLIENT_ID', 'COLOQUE_SEU_CLIENT_ID_AQUI')
CLIENT_SECRET = os.getenv('BLIZZARD_CLIENT_SECRET', 'COLOQUE_SEU_SECRET_AQUI')

# Nossa pasta do Data Lake local (Será criada dentro de rede-nydus/source/bronze)
BRONZE_PATH = '/opt/airflow/source/bronze'

# Ligas a coletar: 3=Platinum, 4=Diamond, 5=Master, 6=Grandmaster
LEAGUE_IDS = [3, 4, 5, 6]


def _get_token(**context):
    """ Task 1: Busca o token da API e compartilha com as próximas tasks via XCom """
    token = get_battle_net_access_token(CLIENT_ID, CLIENT_SECRET)
    if not token:
        raise ValueError("Falha ao obter o token da Blizzard!")
    return token


def _load_ladder_ids_from_disk(**context):
    """
    Task 2: Lê os ladder_ids do disco a partir dos arquivos league_raw gerados
    pelo dag_bronze_structure (roda @daily). Evita chamadas desnecessárias à API.
    Retorna {league_id: [ladder_ids]} via XCom.
    """
    import glob

    all_ladder_ids = {}
    for lid in LEAGUE_IDS:
        files = sorted(glob.glob(f"{BRONZE_PATH}/league_raw_{lid}_*.json"))
        if not files:
            print(f"Nenhum arquivo league_raw_{lid} encontrado, pulando liga {lid}.")
            continue
        latest = files[-1]
        with open(latest, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        ladder_ids = [
            division['ladder_id']
            for tier in raw_data.get('tier', [])
            for division in tier.get('division', [])
        ]
        all_ladder_ids[str(lid)] = ladder_ids
        print(f"Liga {lid}: {len(ladder_ids)} ladder_ids lidos de {os.path.basename(latest)}")

    if not all_ladder_ids:
        raise ValueError("Nenhum arquivo league_raw encontrado no disco! Execute dag_bronze_structure primeiro.")

    return all_ladder_ids


def _extract_modern_ladders(**context):
    """
    Task 3: Puxa o snapshot MMR (API moderna) para cada liga e salva um arquivo
    bronze por liga com o league_id no nome: modern_ladders_raw_{league_id}_{exec_date}.json
    """
    token = context['ti'].xcom_pull(task_ids='get_token')
    all_ladder_ids = context['ti'].xcom_pull(task_ids='load_ladder_ids')
    exec_date = context['logical_date'].strftime('%Y-%m-%d_%H%M')

    def fetch_modern(ladder_id):
        try:
            time.sleep(0.5)
            data = fetch_ladder_modern_raw(acesso=token, ladder_id=ladder_id)
            if data:
                validate_modern_ladder_response(data, ladder_id)
                return {"ladder_id": ladder_id, "data": data}
        except Exception as e:
            print(f"Erro no ladder modern {ladder_id}: {e}")
        return None

    for league_id_str, ladder_ids in all_ladder_ids.items():
        league_id = int(league_id_str)
        print(f"[liga {league_id}] Buscando {len(ladder_ids)} ladders modern em paralelo...")

        modern_raw_data = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(fetch_modern, lid): lid for lid in ladder_ids}
            for future in as_completed(futures):
                result = future.result()
                if result:
                    modern_raw_data.append(result)

        if modern_raw_data:
            file_path = f"{BRONZE_PATH}/modern_ladders_raw_{league_id}_{exec_date}.json"
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(modern_raw_data, f)
            print(f"Salvo: {file_path} ({len(modern_raw_data)} ladders)")
        else:
            print(f"Nenhuma ladder modern processada para liga {league_id}.")


# ====================
# DEFINIÇÃO DO FLUXO DO AIRFLOW (DAG)
# ====================
default_args = {
    'owner': 'capitao_sc2',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

with DAG('bronze_mmr_snapshot_tracker',
         default_args=default_args,
         schedule_interval='*/10 * * * *', # Roda a CADA 10 MINUTOS
         catchup=False,
         max_active_runs=1,
         tags=['starcraft', 'esports', 'data_lake', 'mmr_tracker']) as dag:

    get_token = PythonOperator(
        task_id='get_token',
        python_callable=_get_token
    )

    load_ladder_ids = PythonOperator(
        task_id='load_ladder_ids',
        python_callable=_load_ladder_ids_from_disk
    )

    extract_modern_ladders = PythonOperator(
        task_id='extract_modern_ladders',
        python_callable=_extract_modern_ladders
    )

    # Lê estrutura do disco (gerada pelo dag_bronze_structure @daily) e extrai MMR
    get_token >> load_ladder_ids >> extract_modern_ladders