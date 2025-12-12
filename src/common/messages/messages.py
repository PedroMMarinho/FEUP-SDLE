from enum import Enum
import json

class MessageType(Enum):
    REQUEST_FULL_LIST = 1
    REQUEST_FULL_LIST_ACK = 2
    REQUEST_FULL_LIST_NACK = 3
    SENT_FULL_LIST = 4
    SENT_FULL_LIST_ACK = 5
    GOSSIP_SERVER_LIST = 6
    GOSSIP_SERVER_REMOVAL = 7
    HINTED_HANDOFF = 8
    HINTED_HANDOFF_ACK = 9
    REPLICA = 10
    REPLICA_ACK = 11
    PROXY_INTRODUCTION = 12
    PROXY_INTRODUCTION_ACK = 13
    HASHRING_UPDATE = 14
    HASHRING_UPDATE_ACK = 15
    REMOVE_SERVER = 16
    REMOVE_SERVER_ACK = 17

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
    
