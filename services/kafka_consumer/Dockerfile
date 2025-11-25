FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY 03_kafka_consumer_debug.py .

CMD ["python", "03_kafka_consumer_debug.py"]