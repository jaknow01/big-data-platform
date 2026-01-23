from confluent_kafka import Consumer
from datetime import datetime
import json

# Konfiguracja Kafka Consumer dla JSON (bez Schema Registry)
config_kafka = {
    'bootstrap.servers': 'localhost:29092',
    'group.id': 'json-consumer-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True,
    'auto.commit.interval.ms': 1000
}

# Lista tematów do subskrypcji (można dostosować do swoich potrzeb)
TOPICS = [
    'dbserver1.public.housing'
]

def process_cdc_event(message_data):
    """Przetwarza event CDC i wyświetla w czytelnym formacie"""
    try:
        operation = message_data.get('op')
        before = message_data.get('before')
        after = message_data.get('after')
        timestamp = message_data.get('ts_ms', 0)
        source = message_data.get('source', {})
        
        # Konwersja timestamp
        if timestamp:
            dt = datetime.fromtimestamp(timestamp / 1000)
            timestamp_str = dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            timestamp_str = "Unknown"
        
        # Wyświetl informacje źródłowe
        table_name = source.get('table', 'unknown')
        schema_name = source.get('schema', 'unknown')
        
        print(f"📊 Table: {schema_name}.{table_name}")
        print(f"🕒 Timestamp: {timestamp_str}")
        
        # Analiza typu operacji
        if operation == 'c':
            print("➕ CREATE operation (INSERT)")
            if after:
                print(f"   New record: {json.dumps(after, indent=4, ensure_ascii=False)}")
        
        elif operation == 'u':
            print("✏️ UPDATE operation")
            if before:
                print(f"   Before: {json.dumps(before, indent=4, ensure_ascii=False)}")
            if after:
                print(f"   After: {json.dumps(after, indent=4, ensure_ascii=False)}")
        
        elif operation == 'd':
            print("🗑️ DELETE operation")
            if before:
                print(f"   Deleted record: {json.dumps(before, indent=4, ensure_ascii=False)}")
        
        elif operation == 'r':
            print("📖 READ operation (initial snapshot)")
            if after:
                print(f"   Record: {json.dumps(after, indent=4, ensure_ascii=False)}")
        
        else:
            print(f"❓ Unknown operation: {operation}")
            
    except Exception as e:
        print(f"⚠️ Error processing CDC event: {e}")
        print(f"   Raw data: {message_data}")

def deserialize_json_message(raw_value):
    """
    Deserializuje wiadomość JSON z Kafka
    
    Args:
        raw_value: Raw bytes z Kafka
        
    Returns:
        dict: Zdezserializowane dane JSON lub None w przypadku błędu
    """
    if raw_value is None:
        return None
    
    try:
        # Konwertuj bytes na string, a następnie parsuj JSON
        json_string = raw_value.decode('utf-8')
        return json.loads(json_string)
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        print(f"⚠️ Failed to deserialize JSON message: {e}")
        print(f"   Raw value: {raw_value}")
        return None

def main():
    """Główna funkcja konsumenta"""
    print("🚀 Starting JSON Kafka Consumer")
    print(f"📡 Subscribing to topics: {', '.join(TOPICS)}")
    print("🔁 Listening for messages... Press Ctrl+C to stop.\n")
    
    # Inicjalizuj konsumenta
    consumer = Consumer(config_kafka)
    consumer.subscribe(TOPICS)
    
    message_count = 0
    
    try:
        while True:
            # Poll dla nowych wiadomości
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                print(f"⚠️ Kafka error: {msg.error()}")
                continue
            
            message_count += 1
            
            # Wyświetl podstawowe informacje o wiadomości
            print(f"\n{'='*60}")
            print(f"📨 Message #{message_count}")
            print(f"   Topic: {msg.topic()}")
            print(f"   Partition: {msg.partition()}")
            print(f"   Offset: {msg.offset()}")
            
            # Deserializuj klucz (jeśli istnieje)
            key_data = None
            if msg.key():
                key_data = deserialize_json_message(msg.key())
                if key_data:
                    print(f"   Key: {json.dumps(key_data, ensure_ascii=False)}")
            
            # Deserializuj wartość
            value_data = deserialize_json_message(msg.value())
            
            if value_data:
                # Przetwórz event CDC
                process_cdc_event(value_data)
                
                # Wyświetl pełną wiadomość JSON (opcjonalnie)
                print(f"\n📋 Full JSON message:")
                print(json.dumps(value_data, indent=2, ensure_ascii=False))
            else:
                print("❌ Failed to process message")
            
            print(f"{'='*60}")
            
    except KeyboardInterrupt:
        print("\n🛑 Stopping consumer...")
        
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        
    finally:
        print("🔄 Closing consumer...")
        consumer.close()
        print(f"✅ Consumer closed. Processed {message_count} messages total.")

def test_specific_topic(topic_name):
    """
    Testuje konsumenta dla konkretnego tematu
    
    Args:
        topic_name: Nazwa tematu do testowania
    """
    print(f"🧪 Testing consumer for specific topic: {topic_name}")
    
    config = config_kafka.copy()
    config['group.id'] = f'json-test-consumer-{topic_name.replace(".", "-")}'
    
    consumer = Consumer(config)
    consumer.subscribe([topic_name])
    
    print(f"📡 Subscribed to: {topic_name}")
    print("⏳ Waiting for messages (timeout: 10 seconds)...\n")
    
    messages_received = 0
    
    try:
        # Czekaj na wiadomości przez 10 sekund
        for _ in range(10):
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                print(f"⚠️ Error: {msg.error()}")
                continue
            
            messages_received += 1
            print(f"✅ Message {messages_received} received from {msg.topic()}")
            
            # Deserializuj i wyświetl
            value_data = deserialize_json_message(msg.value())
            if value_data:
                process_cdc_event(value_data)
        
        if messages_received == 0:
            print("⚠️ No messages received. Check if:")
            print("   - Debezium connector is running")
            print("   - Database changes are being made")
            print("   - Topic name is correct")
            
    except KeyboardInterrupt:
        print("\n🛑 Test stopped by user")
        
    finally:
        consumer.close()
        print(f"🏁 Test completed. Received {messages_received} messages.")

if __name__ == "__main__":
    # Uruchom główny konsument
    main()
    
    # Lub przetestuj konkretny temat (odkomentuj poniżej)
    # test_specific_topic('dbserver1.public.customers')