import requests
import json
import time

url = "http://localhost:8083/connectors"

# Konfiguracja Debezium z formatem JSON
config = {
    "name": "postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "uzytkownik",
        "database.password": "haslo123",
        "database.dbname": "baza_postgres",
        "database.server.name": "dbserver1",
        "plugin.name": "pgoutput",
        "slot.name": "debezium_slot",
        "publication.autocreate.mode": "filtered",
        
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false",
        
        # Dodatkowe opcje
        "decimal.handling.mode": "double",
        "time.precision.mode": "connect",
        "tombstones.on.delete": "false",
    }
}

def check_debezium_health():
    """Sprawdza czy Debezium jest gotowy"""
    try:
        response = requests.get("http://localhost:8083/")
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

def register_connector():
    """Rejestruje konektor Debezium"""
    headers = {"Content-Type": "application/json"}
    print("Sending connector configuration to Debezium...")
    
    try:
        response = requests.post(url, headers=headers, data=json.dumps(config))
        
        if response.status_code == 201:
            print("✅ Connector registered successfully.")
            return True
        elif response.status_code == 409:
            print("⚠️ Connector already exists.")
            return True
        else:
            print(f"❌ Failed to register connector. Status code: {response.status_code}")
            print("Response:", response.text)
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Error connecting to Debezium: {e}")
        return False

def check_connector_status():
    """Sprawdza status konektora"""
    try:
        response = requests.get(f"{url}/postgres-connector/status")
        if response.status_code == 200:
            status = response.json()
            print(f"📊 Connector status: {status['connector']['state']}")
            for task in status['tasks']:
                print(f"   Task {task['id']}: {task['state']}")
        else:
            print(f"❌ Could not get connector status: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"❌ Error checking connector status: {e}")

def main():
    print("🔄 Waiting for Debezium to start...")
    
    # Czekaj na Debezium
    retries = 30
    while retries > 0 and not check_debezium_health():
        print(f"   Waiting... ({retries} retries left)")
        time.sleep(2)
        retries -= 1
    
    if retries == 0:
        print("❌ Debezium is not responding. Make sure docker-compose is running.")
        return
    
    print("✅ Debezium is ready!")
    
    # Rejestruj konektor
    if register_connector():
        time.sleep(3)  # Czekaj chwilę na inicjalizację
        check_connector_status()
    

if __name__ == "__main__":
    main()