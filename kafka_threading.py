import json
import sys
import threading
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from prompt_toolkit import prompt
from prompt_toolkit.patch_stdout import patch_stdout

def create_producer(server_ip: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=f"{server_ip}:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def create_consumer(server_ip: str, topic: str) -> KafkaConsumer:
    return KafkaConsumer(
    topic,
    bootstrap_servers=f'{server_ip}:9092',
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
    )

def get_formatted_msg(msg: str) -> str:
    now = datetime.now().strftime("%H:%M")
    return f"[{now}] {msg}"

def end_chat(producer: KafkaProducer):
    producer.flush()
    producer.close()
    
    print("Good bye!")
    sys.exit()
    
def show_chat(consumer: KafkaConsumer):
    try:
        for msg in consumer:
            value = msg.value
            if 'msg' in value:
                print(f"{value['user']}: {value['msg']}")
            else:
                print(f"ERROR: {value['error']}")
    except Exception:
        print("Good bye!")
    finally:
        consumer.close()

def main():
    print("Chat program")
    
    server_ip = input("Server IP: ")
    topic = input("Topic name: ")
    nickname = input("Your nickname: ")
    
    producer = create_producer(server_ip)
    consumer = create_consumer(server_ip, topic)
    
    thread = threading.Thread(target=show_chat, args=(consumer,), daemon=True)
    thread.start()
    
    print("Enter your message. (type 'exit' to quit.)")
    
    try:
        while patch_stdout():
            msg = input()
            if msg.lower() == "exit":
                break
            
            msg = get_formatted_msg(msg)
            producer.send(topic, {"user": nickname, "msg": msg})
            producer.flush()
    except Exception as e:
        msg = get_formatted_msg(get_formatted_msg(f"An error caused.\n{str(e)}"))
        producer.send(topic, {"user": nickname, "error": msg})
    finally:
        end_chat(producer)

if __name__ == "__main__":
    main()