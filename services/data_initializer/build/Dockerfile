FROM python:3.9-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app

COPY build/requirements.txt .
COPY scripts ./scripts
COPY data/ ./data/

RUN pip install --no-cache-dir -r requirements.txt

CMD ["python", "scripts/generate_data_load_postgres.py"]