
class GCounter:
    def __init__(self):
        self.counts = {}

    def increment(self, client_id, amount=1):
        if amount < 0:
            raise ValueError("GCounters can only grow")
        current = self.counts.get(client_id, 0)
        self.counts[client_id] = current + amount

    def get_value(self):
        return sum(self.counts.values())

    def merge(self, other):
        all_keys = set(self.counts.keys()) | set(other.counts.keys())
        #print(f"Merging GCounter: self={self.counts}, other={other.counts}")
        #print(f"All keys for merge: {all_keys}")
        for key in all_keys:
            self.counts[key] = max(
                self.counts.get(key, 0),
                other.counts.get(key, 0)
            )