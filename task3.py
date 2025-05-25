from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
from datetime import datetime
import json

config_kafka = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'avro-consumer-group',
    'auto.offset.reset': 'earliest'
}

config_schema_registry = {
    'url': 'http://localhost:8081'
}

TOPIC_NAME = 'dbserver1.public.organizations_100'

schema_client = SchemaRegistryClient(config_schema_registry)
avro_client = AvroDeserializer(schema_client)

consumer = Consumer(config_kafka)
consumer.subscribe([TOPIC_NAME])

print(f"✅ Subscribed to topic: {TOPIC_NAME}")
print("🔁 Listening for messages... Press Ctrl+C to stop.\n")


def process_cdc_event(message_data):
    """Przetwarza event CDC i wyświetla w czytelnym formacie"""
    operation = message_data.get('op')
    before = message_data.get('before')
    after = message_data.get('after')
    timestamp = message_data.get('ts_ms', 0)
    
    # Konwersja timestamp
    if timestamp:
        dt = datetime.fromtimestamp(timestamp / 1000)
        timestamp_str = dt.strftime('%Y-%m-%d %H:%M:%S')
    else:
        timestamp_str = "Unknown"
    
    print(f"🕒 Timestamp: {timestamp_str}")
    
    if operation == 'c':
        print("➕ CREATE operation")
        if after:
            print(f"   New record: {json.dumps(after, indent=4)}")
            
    elif operation == 'u':
        print("✏️  UPDATE operation")
        if before:
            print(f"   Before: {json.dumps(before, indent=4)}")
        if after:
            print(f"   After:  {json.dumps(after, indent=4)}")
            
    elif operation == 'd':
        print("🗑️  DELETE operation")
        if before:
            print(f"   Deleted record: {json.dumps(before, indent=4)}")
            
    elif operation == 'r':
        print("📖 READ operation (initial snapshot)")
        if after:
            print(f"   Record: {json.dumps(after, indent=4)}")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("⚠️ Error:", msg.error())
            continue

        # deserializacja AVRO do słownika
        value = avro_client(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
        process_cdc_event(value)

        print("📨 Event received:")
        print(json.dumps(value, indent=2))

except KeyboardInterrupt:
    print("\n🛑 Stopping...")

finally:
    consumer.close()