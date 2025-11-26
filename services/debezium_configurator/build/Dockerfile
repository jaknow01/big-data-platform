FROM python:3.11-slim

WORKDIR /app

# Instalacja zależności
COPY build/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY scripts .


# Kopiowanie skryptu

# Skrypt czeka na dostępność Debezium, a potem tworzy konektor
CMD ["python", "create_debezium_connector.py"]