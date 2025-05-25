from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
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

        print("📨 Event received:")
        print(json.dumps(value, indent=2))

except KeyboardInterrupt:
    print("\n🛑 Stopping...")

finally:
    consumer.close()