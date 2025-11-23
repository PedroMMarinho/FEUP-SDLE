import uuid


class LWWRegister:    
    def __init__(self, value, timestamp, peer_id=None):
        self.value = value
        self.timestamp = timestamp
        self.peer_id = peer_id if peer_id is not None else uuid.uuid4().hex

    def merge(self, other):
        if self.timestamp > other.timestamp:
            return self
        if self.timestamp < other.timestamp:
            return other
        return self if self.peer_id > other.peer_id else other

    def to_dict(self):
        return {
            "value": self.value,
            "timestamp": self.timestamp,
            "peer_id": self.peer_id
        }

    @staticmethod
    def from_dict(d):
        return LWWRegister(d["value"], d["timestamp"], d["peer_id"])

    def __repr__(self):
        return f"LWWRegister(value={self.value}, ts={self.timestamp}, peer={self.peer_id})"