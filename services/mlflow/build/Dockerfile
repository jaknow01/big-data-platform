# ./services/mlflow/Dockerfile
FROM ghcr.io/mlflow/mlflow:latest

# DODANO: gunicorn (kluczowe, aby pozbyć się błędu Host Header)
RUN pip install psycopg2-binary boto3 gunicorn

EXPOSE 5000