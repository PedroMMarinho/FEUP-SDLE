from enum import Enum
import json

class MessageType(Enum):
    LIST_UPDATE = 1
    REQUEST_FULL_LIST = 2
    FULL_LIST = 3
    HEARTBEAT = 4
    SERVER_INTRODUCTION = 5
    SERVER_INTRODUCTION_ACK = 6
    HINTED_HANDOFF = 7

class Message:
    def __init__(self, msg_type, payload):
        self.msg_type = msg_type
        self.payload = payload
    def __init__(self, json_str):
        data = json.loads(json_str)
        self.msg_type = MessageType(data["msg_type"])
        self.payload = data["payload"]

    def serialize(self):
        return json.dumps(self.to_dict()).encode('utf-8')

    def to_dict(self):
        return {
            "msg_type": self.msg_type.value,
            "payload": self.payload
        }
    @staticmethod
    def from_dict(data):
        msg_type = MessageType(data["msg_type"])
        payload = data["payload"]
        return Message(msg_type, payload)   
    
