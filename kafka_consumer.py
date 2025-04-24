import json
from kafka import KafkaConsumer

def main():
    print("Chat program - Message Consumer")
    print("Wating for messages...")
    
    consumer = KafkaConsumer(
    'instance-test',
    bootstrap_servers='43.203.219.53:9092',
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

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