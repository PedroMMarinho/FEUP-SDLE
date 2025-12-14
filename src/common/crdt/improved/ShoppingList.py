from src.common.crdt.improved.PNCounter import PNCounter
from src.common.crdt.improved.ORSet import ORSet
import json

class ShoppingList:
    def __init__(self, list_uuid,name=None):
        self.uuid = list_uuid        
        self.clock = 0
        self.name = name            
        self.items = {} 

    def _tick(self):
        self.clock += 1
        return self.clock

    def _get_tag(self):
        return f"{self.id}:{self.clock}"

    def add_item(self, name, needed_amount=1, acquired_amount=0):
        self._tick()
        tag = self._get_tag()

        if name not in self.items:
            self.items[name] = {
                "needed": PNCounter(),
                "acquired": PNCounter(),
                "existence": ORSet() 
            }
            self.items[name]["needed"].change(self.id, needed_amount)
            self.items[name]["acquired"].change(self.id, acquired_amount)

        self.items[name]["existence"].add(name, tag)

    def remove_item(self, name):
        self._tick()
        if name in self.items:
            self.items[name]["existence"].remove(name)

    def update_needed(self, name, amount):
        self._tick()
        if name in self.items:
            self.items[name]["needed"].change(self.id, amount)

    def update_acquired(self, name, amount):
        self._tick()
        if name in self.items:
            self.items[name]["acquired"].change(self.id, amount)

    def get_visible_items(self):
        visible_list = {}
        for name, data in self.items.items():
            if data["existence"].contains(name):
                visible_list[name] = {
                    "needed": data["needed"].get_value(),
                    "acquired": data["acquired"].get_value()
                }
        return visible_list

    def merge(self, other):
        self.clock = max(self.clock, other.clock)
        all_keys = set(self.items.keys()) | set(other.items.keys())
        
        for name in all_keys:
            if name not in self.items:
                self.items[name] = {
                    "needed": PNCounter(),
                    "acquired": PNCounter(),
                    "existence": ORSet()
                }
            if name in other.items:
                other_data = other.items[name]
                self.items[name]["needed"].merge(other_data["needed"])
                self.items[name]["acquired"].merge(other_data["acquired"])
                self.items[name]["existence"].merge(other_data["existence"])

    def to_dict(self):
        def recursive_serialize(obj):
            if isinstance(obj, set):
                return list(obj)
            elif hasattr(obj, "__dict__"):
                return {k: recursive_serialize(v) for k, v in obj.__dict__.items()}
            elif isinstance(obj, dict):
                return {k: recursive_serialize(v) for k, v in obj.items()}
            else:
                return obj
        
        return recursive_serialize(self)

    def to_json(self):
        return json.dumps(self.to_dict())
    
    @staticmethod
    def from_json(json_str):
        def recursive_deserialize(data):
            if isinstance(data, dict):
                if 'positive' in data and 'negative' in data:
                    pc = PNCounter()
                    pc.positive.counts = data['positive']['counts']
                    pc.negative.counts = data['negative']['counts']
                    return pc
                elif 'elements' in data and 'tombstones' in data:
                    ors = ORSet()
                    ors.elements = {tuple(x) for x in data['elements']}
                    ors.tombstones = {tuple(x) for x in data['tombstones']}
                    return ors
                else:
                    return {k: recursive_deserialize(v) for k, v in data.items()}
            else:
                return data
        
        data = json.loads(json_str)
        sl = ShoppingList(list_uuid=data['uuid'], name=data.get('name'))
        sl.clock = data['clock']
        
        for name, item_data in data.get('items', {}).items():
            sl.items[name] = {
                "needed": recursive_deserialize(item_data['needed']),
                "acquired": recursive_deserialize(item_data['acquired']),
                "existence": recursive_deserialize(item_data['existence'])
            }
        
        return sl