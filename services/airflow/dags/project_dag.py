from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

SCRIPTS_PATH = "/opt/airflow/dags/scripts"

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'data_simulation_pipeline',
    default_args=default_args,
    schedule='@daily',
    start_date=datetime(2025, 11, 29),
    catchup=False,
) as dag:

    postgres_vars = {
        'POSTGRES_DB': 'baza_postgres',
        'POSTGRES_USER': 'uzytkownik',
        'POSTGRES_PASSWORD': 'haslo123',
        'POSTGRES_HOST': 'postgres',
        'POSTGRES_PORT': '5432'
    }

    debezium_env = {
        'DEBEZIUM_HOST': 'debezium', 
        'DEBEZIUM_PORT': '8083'
    }

    # 1. create database, load data
    load_data = BashOperator(
        task_id='load_initial_data',
        bash_command=f'cd {SCRIPTS_PATH} && python generate_data_load_postgres.py',
        env=postgres_vars
    )
 
    # 2. configure connector
    configure_debezium = BashOperator(
        task_id='configure_debezium_connector',
        bash_command=f'cd {SCRIPTS_PATH} && python create_debezium_connector.py',
        env=debezium_env
    )

    # 3. simulate changes
    simulate_data = BashOperator(
        task_id='simulate_data_changes',
        bash_command=f'cd {SCRIPTS_PATH} && python simulate_change.py',
        env=postgres_vars,
        execution_timeout=timedelta(minutes=10)
    )

    load_data >> configure_debezium >> simulate_data