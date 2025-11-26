FROM python:3.9-slim

WORKDIR /app

COPY build/requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY scripts .

CMD ["python", "kafka_consumer_debug.py"]