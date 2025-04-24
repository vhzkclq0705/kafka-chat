# kafka-chat

**Kafka 메시지 발신, 수신 연습**

## 구조

### AWS EC2 인스턴스
- zookeeper-server, server 구동

### Local
- Kafka producer, consumer 구동

## How to use
```
$ source .venv/bin/activate
$ pdm install

# Producer
$ chatpro
Chat program - Message Producer
Server IP: 43.203.219.53
Topic name: instance-test
Enter your message. (type 'exit' to quit.)
You: hi

# Consumer
$ python kafka_consumer.py
Chat program - Message Consumer
Server IP: 43.203.219.53
Topic name: instance-test
Wating for messages...
Friend: hi [14:20]