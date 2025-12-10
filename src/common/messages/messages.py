from enum import Enum
import json

class MessageType(Enum):
    REQUEST_FULL_LIST = 1
    REQUEST_FULL_LIST_ACK = 2
    REQUEST_FULL_LIST_NACK = 3
    SENT_FULL_LIST = 4
    SENT_FULL_LIST_ACK = 5
    HEARTBEAT = 6
    HEARTBEAT_ACK = 7
    SERVER_INTRODUCTION = 8
    SERVER_INTRODUCTION_ACK = 9
    HINTED_HANDOFF = 10
    SERVER_CONFIG_UPDATE = 11
    SERVER_CONFIG_UPDATE_ACK = 12
    REPLICA = 13
    REPLICA_ACK = 14
    LOAD_BALANCE = 15
    LOAD_BALANCE_ACK = 16
    PROXY_INTRODUCTION = 17
    PROXY_INTRODUCTION_ACK = 18
    HASHRING_UPDATE = 19
    HASHRING_UPDATE_ACK = 20
    REMOVE_SERVER = 21
    REMOVE_SERVER_ACK = 22

class Message:
    def __init__(self, msg_type=None, payload=None, json_str=None):
        if json_str is not None:
            data = json.loads(json_str.decode('utf-8'))
            self.msg_type = MessageType(data["msg_type"])
            self.payload = data["payload"]
        else:
            self.msg_type = msg_type
            self.payload = payload

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
    
