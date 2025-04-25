# Textual + Kafka 기반 채팅 프로그램

import json
import sys
import threading
from datetime import datetime
from kafka import KafkaProducer, KafkaConsumer
from textual import on
from textual.app import App, ComposeResult
from textual.containers import Vertical, Container
from textual.screen import Screen
from textual.widgets import Static, Input, Button, RichLog, Header, Footer, Label

class ChatState:
    producer: KafkaProducer = None
    consumer: KafkaConsumer = None
    server_ip: str = ""
    topic: str = ""
    nickname: str = ""

chat_state = ChatState()

def get_formatted_msg(msg: str) -> str:
    now = datetime.now().strftime("%H:%M")
    return f"[{now}] {msg}"

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
        group_id=None
    )

def send_messages(text: str):
    formatted = get_formatted_msg(text)
    chat_state.producer.send(chat_state.topic, {"user": chat_state.nickname, "msg": formatted})
    chat_state.producer.flush()

def show_chat(log_widget: RichLog):
    for msg in chat_state.consumer:
        value = msg.value
        if 'msg' in value:
            log_widget.write(f"{value['user']}: {value['msg']}")
        elif 'error' in value:
            log_widget.write(f"[ERROR] {value['user']}\n{value['error']}")

def exit_chat():
    bye = get_formatted_msg(f"👋 {chat_state.nickname} 님이 퇴장하셨습니다.")
    chat_state.producer.send(chat_state.topic, {"user": "[SERVER]", "msg": bye})
    chat_state.producer.flush()
    chat_state.producer.close()
    chat_state.consumer.close()

class HomeScreen(Container):
    def compose(self) -> ComposeResult:
        yield Static("Server IP: ")
        yield Input(placeholder="43.203.219.53", id="ip")
        yield Static("Topic name: ")
        yield Input(placeholder="chat_room", id="topic")
        yield Static("Nickname: ")
        yield Input(placeholder="nickname", id="nickname")
        yield Button("Enter Chat", id="enter_btn")

    @on(Button.Pressed, "#enter_btn")
    def connect_kafka(self):
        chat_state.server_ip = self.query_one("#ip", Input).value
        chat_state.topic = self.query_one("#topic", Input).value
        chat_state.nickname = self.query_one("#nickname", Input).value

        chat_state.producer = create_producer(chat_state.server_ip)
        chat_state.consumer = create_consumer(chat_state.server_ip, chat_state.topic)

        self.app.push_screen(ChatScreen())

class ChatScreen(Screen):
    def compose(self) -> ComposeResult:
        yield Header()
        yield Button("나가기", id="exit_btn", classes="exit-button")
        yield RichLog(id="chat_log")
        yield Input(placeholder="메시지를 입력하세요", id="chat_input")
        yield Footer()

    def on_mount(self) -> None:
        log = self.query_one("#chat_log", RichLog)
        t = threading.Thread(target=show_chat, args=(log,), daemon=True)
        t.start()

        welcome = get_formatted_msg(f"👋 {chat_state.nickname} 님이 입장하셨습니다.")
        log.write(f"[SERVER]: {welcome}")
        chat_state.producer.send(chat_state.topic, {"user": "[SERVER]", "msg": welcome})
        chat_state.producer.flush()

    @on(Input.Submitted, "#chat_input")
    def send_message(self, event: Input.Submitted):
        text = event.value.strip()
        send_messages(text)
        self.query_one("#chat_input", Input).value = ""
    
    @on(Button.Pressed, "#exit_btn")
    def exit_chatroom(self):
        exit_chat()
        self.app.exit()
        
class ChatApp(App):
    CSS_PATH = "main.css"

    def compose(self) -> ComposeResult:
        yield HomeScreen()

if __name__ == "__main__":
    ChatApp().run()
