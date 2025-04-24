import json
import sys
from datetime import datetime
from kafka import KafkaProducer

def create_producer(server_ip: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=f"{server_ip}:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

def end_chat(producer: KafkaProducer):
    producer.flush()
    producer.close()
    
    print("Good bye!")
    sys.exit()

def get_formatted_msg(msg: str) -> str:
    now = datetime.now().strftime("%H:%M")
    return f"[{now}] {msg}"

def main():
    print("Chat program - Message Producer")
    
    server_ip = input("Server IP: ")
    producer = create_producer(server_ip)
    
    topic = input("Topic name: ")
    print("Enter your message. (type 'exit' to quit.)")
    
    try:
        while True:
            msg = input("You: ")
            if msg == "exit": break
            
            msg = get_formatted_msg(msg)
            producer.send(topic, {"msg": msg})
            producer.flush()
    except Exception as e:
        msg = get_formatted_msg(get_formatted_msg(f"An error caused.\n{str(e)}"))
        producer.send(topic, {"error": msg})
    finally:
        end_chat(producer)

if __name__ == "__main__":
    main()