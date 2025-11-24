from src.common.crdt.pncounter.GCounter import GCounter

class PNCounter:
    def __init__(self):
        self.positive = GCounter()
        self.negative = GCounter()

    def change(self, client_id, amount):
        if amount > 0:
            self.positive.increment(client_id, amount)
        elif amount < 0:
            self.negative.increment(client_id, abs(amount))

    def increase(self, client_id, amount=1):
        self.positive.increment(client_id, amount)

    def decrease(self, client_id, amount=1):
        self.negative.increment(client_id, amount)

    def get_value(self):
        return self.positive.get_value() - self.negative.get_value()

    def merge(self, other):
        self.positive.merge(other.positive)
        self.negative.merge(other.negative)