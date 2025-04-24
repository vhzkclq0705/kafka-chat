import json
from kafka import KafkaConsumer

def create_consumer(server_ip: str, topic: str) -> KafkaConsumer:
    return KafkaConsumer(
    topic,
    bootstrap_servers=f'{server_ip}:9092',
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

def main():
    print("Chat program - Message Consumer")
    
    server_ip = input("Server IP: ")
    topic = input("Topic name: ")
    
    consumer = create_consumer(server_ip, topic)
    
    print("Wating for messages...")

    try:
        for msg in consumer:
            value = msg.value
            if 'msg' in value:
                print(f"Friend: {value['msg']}")
            else:
                print(f"ERROR: {value['error']}")
    except Exception:
        print("Good bye!")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()