import sys
import os
import glob

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

sys.path.append('/opt/airflow/source')

from utils.silver_transforms import transform_match_history_to_silver
from utils.silver_loader import load_match_history, is_file_processed, mark_file_processed

BRONZE_PATH = '/opt/airflow/source/bronze'


def _process_match_history_to_silver(**context):
    """
    Encontra o arquivo matches_all_history_*.json mais recente no bronze.
    Se o arquivo já foi processado (silver.processed_files), encerra sem reprocessar.
    Retorna o caminho do arquivo via XCom para a task de carga (nunca o DataFrame).
    """
    files = sorted(glob.glob(f"{BRONZE_PATH}/matches_all_history_*.json"))
    if not files:
        print(
            f"Nenhum arquivo matches_all_history_*.json encontrado em {BRONZE_PATH}. "
            "Aguardando próxima execução do dag_bronze_match_history."
        )
        return None

    latest_file = files[-1]
    filename = os.path.basename(latest_file)

    if is_file_processed(filename):
        print(f"Arquivo já processado anteriormente, pulando: {filename}")
        return None

    print(f"Novo arquivo detectado para processamento: {filename}")
    return latest_file


def _load_match_history_to_postgres(**context):
    """
    Carrega o arquivo de match history na tabela silver.match_history.
    Marca o arquivo como processado em silver.processed_files ao concluir.
    """
    bronze_file = context['ti'].xcom_pull(task_ids='process_match_history')

    if bronze_file is None:
        print("Nenhum arquivo novo para carregar.")
        return

    df = transform_match_history_to_silver(bronze_file)
    inserted = load_match_history(df)
    mark_file_processed(os.path.basename(bronze_file))
    print(f"Carga concluída: {inserted} novas partidas inseridas.")


default_args = {
    'owner': 'capitao_sc2',
    'start_date': datetime(2023, 10, 1),
    'retries': 1,
}

# Roda a cada 30 minutos e verifica se há arquivo novo não processado.
# A idempotência via silver.processed_files garante que reruns são seguros —
# se o bronze ainda não terminou, esta DAG encerra sem fazer nada e tenta novamente.
with DAG(
    'silver_match_history',
    default_args=default_args,
    schedule_interval='*/30 * * * *',
    catchup=False,
    max_active_runs=1,
    tags=['starcraft', 'esports', 'silver', 'match_history'],
) as dag:

    process_match_history = PythonOperator(
        task_id='process_match_history',
        python_callable=_process_match_history_to_silver,
    )

    load_to_postgres = PythonOperator(
        task_id='load_to_postgres',
        python_callable=_load_match_history_to_postgres,
    )

    process_match_history >> load_to_postgres
