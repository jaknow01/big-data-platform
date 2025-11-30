from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
from scripts.brozne import ingest_bronze
from scripts.silver import transform_silver

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='medallion_architecture',
    default_args=default_args,
    description='Pipeline Bronze -> Silver using Spark Connect',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['spark', 'medallion', 'delta', 'connect']
)
def medallion_pipeline():

    # Zadanie 1: Warstwa Bronze (Kafka -> MinIO)
    @task(task_id="process_bronze_layer")
    def run_bronze_layer():
        if ingest_bronze:
            print("Uruchamianie ingest_bronze via Spark Connect...")
            ingest_bronze()
        else:
            raise Exception("Nie znaleziono funkcji ingest_bronze w scripts/bronze.py")

    # Zadanie 2: Warstwa Silver (MinIO -> MinIO + Transformacja)
    @task(task_id="process_silver_layer")
    def run_silver_layer():
        if transform_silver:
            print("Uruchamianie transform_silver via Spark Connect...")
            transform_silver()
        else:
            raise Exception("Nie znaleziono funkcji transform_silver w scripts/silver.py")

    run_bronze_layer() >> run_silver_layer()

medallion_pipeline()