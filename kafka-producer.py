import json
import time
from kafka import KafkaProducer
from tqdm import tqdm

BUFFER_SIZE = 100

producer = KafkaProducer(
    bootstrap_servers="43.203.219.53:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for i in tqdm(range(10000)):
    msg = {"msg": i}
    producer.send("instance-test", msg)
    
    if (i + 1) % BUFFER_SIZE == 0:
        producer.flush()
        time.sleep(0.01)

producer.flush()
producer.close()