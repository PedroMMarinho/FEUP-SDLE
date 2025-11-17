import uuid


class LWWRegister:
    _logical_counter = 0  # counter for logical timestamps
    
    def __init__(self, value, timestamp=None, peer_id=None):
        self.value = value
        if timestamp is not None:
            self.timestamp = timestamp
        else:
            LWWRegister._logical_counter += 1
            self.timestamp = LWWRegister._logical_counter
        self.peer_id = peer_id if peer_id is not None else uuid.uuid4().hex

    def merge(self, other):
        if self.timestamp > other.timestamp:
            return self
        if self.timestamp < other.timestamp:
            return other
        return self if self.peer_id > other.peer_id else other

    def __repr__(self):
        return f"LWWRegister(value={self.value}, ts={self.timestamp}, peer={self.peer_id})"