from kafka import KafkaConsumer
import json

def consume_messages(topic, bootstrap_servers):
    print(f"Starting consumer for topic: {topic}")
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    
    try:
        for message in consumer:
            print(f"Received message: {message.value}")
    except KeyboardInterrupt:
        print("Consumer stopped")
    except Exception as e:
        print(f"Error consuming messages: {e}")

import sys

if __name__ == "__main__":
    if len(sys.argv) > 1:
        broker = sys.argv[1]
    else:
        broker = "localhost:9094"
    consume_messages("demo-alpha", broker)
