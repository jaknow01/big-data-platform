# Big Data Platform

Platforma big data implementujaca architekture medalionowa (Bronze -> Silver -> Gold) z real-time Change Data Capture. Dane plyna z PostgreSQL przez Debezium i Kafke, sa przetwarzane przez Spark z Delta Lake, orkiestrowane przez Airflow, a modele ML rejestrowane w MLflow. Calosc dziala w Docker Compose.

## Architektura

```
┌─────────────┐     ┌───────────┐     ┌─────────┐
│  PostgreSQL  │────>│ Debezium  │────>│  Kafka   │
│  (source DB) │ CDC │ (connect) │     │ (broker) │
└─────────────┘     └───────────┘     └────┬─────┘
                                           │
                    ┌──────────────────────┘
                    │
                    v
            ┌──────────────┐     ┌─────────────────────────────┐
            │    Spark      │     │          MinIO (S3)          │
            │  (processing) │────>│  datalake/bronze/  (raw)     │
            │              │────>│  datalake/silver/  (cleaned)  │
            └──────────────┘     │  mlflow-bucket/   (artifacts) │
                                 └───────────┬─────────────────┘
                                             │
                    ┌────────────────────────┘
                    v
            ┌──────────────┐     ┌──────────────┐
            │   Airflow     │────>│    MLflow     │
            │ (orchestrator)│     │  (ML registry) │
            └──────────────┘     └──────────────┘
```

## Serwisy

Platforma sklada sie z dwoch warstw: **mini data platform** (CDC pipeline) i **big data platform** (orkiestracja i przetwarzanie).

### Mini Data Platform - CDC Pipeline

| Serwis | Obraz | Opis | Port |
|--------|-------|------|------|
| **postgres** | `postgres:15` | Zrodlowa baza danych z wlaczonym WAL logical replication | 5432 |
| **zookeeper** | `confluentinc/cp-zookeeper:5.5.3` | Koordynacja klastra Kafka | - |
| **kafka** | `confluentinc/cp-enterprise-kafka:5.5.3` | Broker wiadomosci - odbiera eventy CDC | 9092, 29092 |
| **debezium** | `debezium/connect:1.4` | Konektor CDC - monitoruje zmiany w PostgreSQL i publikuje je na Kafke | 8083 |
| **kafka_manager** | `hlebalbau/kafka-manager:stable` | Web UI do zarzadzania topicami Kafka | 9001 |

### Big Data Platform - Orkiestracja i przetwarzanie

| Serwis | Obraz | Opis | Port |
|--------|-------|------|------|
| **spark** | `apache/spark:3.4.1-python3` | Klaster Spark (master) | 7077, 8088 |
| **spark-processor** | `apache/spark:3.4.1-python3` | Spark Connect Server - obsluguje zdalne sesje PySpark z Delta Lake, S3A i Kafka connector | 4040, 15002 |
| **minio** | `minio/minio:latest` | S3-kompatybilny object storage - data lake | 9090 (API), 9091 (console) |
| **mlflow-server** | custom (`services/mlflow/`) | Serwer MLflow do sledzenia eksperymentow i rejestru modeli | 5000 |

### Airflow (CeleryExecutor)

| Serwis | Opis | Port |
|--------|------|------|
| **airflow-apiserver** | API i Web UI | 8080 |
| **airflow-scheduler** | Planowanie i wyzwalanie DAGow | - |
| **airflow-dag-processor** | Parsowanie plikow DAG | - |
| **airflow-worker** | Celery worker - wykonywanie taskow | - |
| **airflow-triggerer** | Obsluga deferrable operators | - |
| **airflow-postgres** | Baza metadanych Airflow (PostgreSQL 16) | - |
| **redis** | Broker Celery | - |
| **flower** | Monitoring Celery (profil `flower`) | 5555 |

### Serwisy pomocnicze

| Serwis | Opis |
|--------|------|
| **minio-init** | Tworzy buckety `datalake` i `mlflow-bucket` przy starcie |
| **airflow-init** | Inicjalizacja katalogow, migracja bazy, tworzenie uzytkownika |
| **kafka-consumer** | Debug tool - nasluchuje eventy CDC z Kafki i loguje je |
| **mlflow-postgres** | Baza metadanych MLflow (PostgreSQL 16) |

## Struktura katalogow

```
services/
├── airflow/
│   └── dags/                        # Definicje DAGow Airflow
│       ├── database_dag.py          # Ladowanie danych i symulacja CDC
│       ├── medallion_dag.py         # Pipeline Bronze -> Silver
│       ├── train_model_dag.py       # Trening modelu ML
│       └── scripts/
│           ├── bronze.py            # Ingestion: Kafka -> Delta (MinIO)
│           ├── silver.py            # Transformacja i czyszczenie danych
│           ├── great_exp.py         # Walidacja Great Expectations
│           ├── generate_data_load_postgres.py
│           ├── create_debezium_connector.py
│           ├── simulate_change.py
│           └── data/Housing.csv     # Dataset mieszkaniowy (545 rekordow)
├── kafka_consumer/
│   ├── build/Dockerfile
│   └── scripts/kafka_consumer_debug.py
├── mlflow/
│   └── build/Dockerfile
└── spark_processor/
    ├── build/Dockerfile
    └── scripts/spark_streaming_job.py
```

## DAGi Airflow

### `data_simulation_pipeline`
Inicjalizacja danych i symulacja zmian. Laduje `Housing.csv` do PostgreSQL, konfiguruje konektor Debezium, a nastepnie przez 10 minut symuluje inserty i update'y generujac strumien CDC.

```
load_initial_data >> configure_debezium_connector >> simulate_data_changes
```

### `medallion_architecture`
Glowny pipeline przetwarzania danych w architekturze medalionowej:

1. **Bronze** - batch read z Kafki (topic `dbserver1.public.housing`), zapis do Delta table na MinIO (`s3a://datalake/bronze/housing`)
2. **Silver** - odczyt z Bronze, parsowanie formatu Debezium, filtrowanie outlierow, konwersja yes/no na booleany, standaryzacja statusu umeblowania, deduplikacja, zapis do `s3a://datalake/silver/housing`
3. **Walidacja** - Great Expectations sprawdza zakresy wartosci, unikatowosc, typy, NOT NULL, liczbe wierszy (400-600)

```
process_bronze_layer >> process_silver_layer >> validate_expectations
```

### `train_model`
Trening modelu ML na danych z warstwy Silver:

1. Odczyt Delta table (`datalake/silver/housing`) do Pandas
2. Trening `LinearRegression` (sklearn) z predykcja ceny mieszkan
3. Logowanie metryk (R2 train/test) i rejestracja modelu w MLflow jako `Housing_data_price_prediction`

```
read_data_from_delta >> train_and_register_model
```

## Uruchomienie

### Wymagania
- Docker i Docker Compose
- Min. 4 GB RAM i 2 CPU dla Dockera
- Min. 10 GB wolnego miejsca na dysku

### Start

```bash
# Skopiuj plik konfiguracyjny
cp .env_template .env

# Uruchom platforme
docker compose up -d

# (opcjonalnie) Z monitoringiem Celery
docker compose --profile flower up -d
```

### Uruchomienie pipeline'u

1. Odpal DAG `data_simulation_pipeline` w Airflow UI (http://localhost:8080, login: `airflow`/`airflow`) - zaladuje dane i uruchomi CDC
2. Po zakonczeniu odpal `medallion_architecture` - przetworzy dane przez warstwy Bronze i Silver
3. Na koniec odpal `train_model` - wytrenuje model i zarejestruje go w MLflow (http://localhost:5000)

### Web UI

| Serwis | URL |
|--------|-----|
| Airflow | http://localhost:8080 |
| MLflow | http://localhost:5000 |
| MinIO Console | http://localhost:9091 |
| Kafka Manager | http://localhost:9001 |
| Spark UI | http://localhost:4040 |
| Flower (Celery) | http://localhost:5555 |

## Stack technologiczny

| Komponent | Wersja |
|-----------|--------|
| Apache Airflow | 3.1.3 |
| Apache Spark | 3.4.1 |
| Delta Lake | 2.4.0 |
| PostgreSQL | 15 (source), 16 (Airflow, MLflow) |
| Kafka | 5.5.3 (Confluent) |
| Debezium | 1.4 |
| MLflow | latest |
| MinIO | latest |
| Great Expectations | >= 0.16.15 |
| Redis | 7.2 |
