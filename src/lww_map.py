from .lww_register import LWWRegister


class LWWMap:
    
    def __init__(self):
        self.state = {}   # key -> LWWRegister

    def set(self, key, value, timestamp=None, peer_id=None):
        reg = LWWRegister(value, timestamp, peer_id)
        self.state[key] = reg

    def get(self, key):
        return self.state[key].value if key in self.state else None

    def delete(self, key, timestamp=None, peer_id=None):
        reg = LWWRegister(None, timestamp, peer_id)
        self.state[key] = reg

    def merge(self, other):
        result = LWWMap()
        keys = set(self.state.keys()).union(other.state.keys())

        for k in keys:
            if k in self.state and k in other.state:
                result.state[k] = self.state[k].merge(other.state[k])
            elif k in self.state:
                result.state[k] = self.state[k]
            else:
                result.state[k] = other.state[k]

        return result

    def items(self):
        for key, register in self.state.items():
            if register.value is not None:
                yield key, register.value

    def keys(self):
        return [key for key, register in self.state.items() if register.value is not None]

    def __repr__(self):
        return f"LWWMap({self.state})"