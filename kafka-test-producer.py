from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers=['localhost:9094'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer.send('demo-test', {'msg': 'Hello from host1!'})
producer.flush()
print("Message sent!")
