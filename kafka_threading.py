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
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        group_id=None,  # 각자 모든 메시지 수신
    )

def get_formatted_msg(msg: str) -> str:
    now = datetime.now().strftime("%H:%M")
    return f"[{now}] {msg}"

def end_chat(producer: KafkaProducer, topic: str, nickname: str):
    producer.flush()
    producer.close()
    sys.exit()
    
def show_chat(consumer: KafkaConsumer):
    try:
        for msg in consumer:
            value = msg.value
            if 'msg' in value:
                print(f"{value['user']}: {value['msg']}")
            else:
                print(f"[ERROR]: User-{value['user']}\n{value['error']}")
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
    
    producer.send(topic, {"user": "", "msg": f"👋 {nickname} 님이 입장하셨습니다."})
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
        producer.send(topic, {"user": "", "msg": f"👋 {nickname} 님이 퇴장하셨습니다."})
        end_chat(producer)

if __name__ == "__main__":
    main()