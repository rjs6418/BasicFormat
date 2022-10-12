from kafka import KafkaConsumer, KafkaProducer
import json

CONSUMERTOPIC = "CONSUMERTOPIC"
PRODUCERTOPIC = "PRODUCERTOPIC"
BROKERS = ["localhost:9091", "localhost:9092", "localhost:9093"]

consumer = KafkaConsumer(CONSUMERTOPIC, bootstrap_servers=BROKERS)
producer = KafkaProducer(bootstrap_servers=BROKERS)

for message in consumer:
    msg = json.loads(message.value.decode())
    
    producer.send(PRODUCERTOPIC, json.dumps(msg).encode("utf-8"))