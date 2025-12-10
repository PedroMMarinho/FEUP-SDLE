import uuid

class ORSet:
    def __init__(self):
        self.elements = set()
        self.tombstones = set()

    def add(self, element, tag=None):
        if tag is None:
            tag = str(uuid.uuid4()) 
        self.elements.add((element, tag))

    def remove(self, element):
        items_to_remove = {entry for entry in self.elements if entry[0] == element}
        self.elements -= items_to_remove
        self.tombstones |= items_to_remove

    def contains(self, element):
        return any(entry[0] == element and entry not in self.tombstones for entry in self.elements)

    def merge(self, other):
        combined_elements = self.elements | other.elements
        combined_tombstones = self.tombstones | other.tombstones
        
        self.elements = combined_elements - combined_tombstones
        self.tombstones = combined_tombstones