import requests
import json

url = "http://localhost:8083/connectors"

config = {
    "name": "postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": "uzytkownik",
        "database.password": "haslo123",
        "database.dbname": "baza_postgres",
        "topic.prefix": "dbserver1",
        "database.server.name": "dbserver1",
        "plugin.name": "pgoutput",
        "slot.name": "debezium_slot",
        "publication.autocreate.mode": "filtered",
        "key.converter": "io.confluent.connect.avro.AvroConverter",
        "value.converter": "io.confluent.connect.avro.AvroConverter",
        "key.converter.schema.registry.url": "http://schema-registry:8081",
        "value.converter.schema.registry.url": "http://schema-registry:8081"
    }
}

def register():
    headers = {"Content-Type": "application/json"}

    print("Sending connector configuration to Debezium...")
    response = requests.post(url, headers=headers, data=json.dumps(config))

    if response.status_code == 201:
        print("✅ Connector registered successfully.")
    elif response.status_code == 409:
        print("⚠️ Connector already exists.")
    else:
        print(f"❌ Failed to register connector. Status code: {response.status_code}")
        print("Response:", response.text)

if __name__ == "__main__":
    register()