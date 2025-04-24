import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'instance-test',
    bootstrap_servers='43.203.219.53:9092',
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

for msg in consumer:
    print(f"받은 메시지: {msg.value['msg']}")

consumer.close()