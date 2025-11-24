import uuid
from src.common.crdt.lww.lww_map import LWWMap
import json

class ShopList:
    
    def __init__(self, list_id=None):
        self.id = list_id if list_id else uuid.uuid4().hex
        self.list = LWWMap()
        self.logical_clock = 0

    def tick(self):
        """Moves time forward."""
        self.logical_clock += 1
        return self.logical_clock

    def add_item(self, key, name, qty_needed=1, qty_acquired=0):
        ts = self.tick()

        item = {
            "name": name,
            "qty_needed": qty_needed,
            "qty_acquired": qty_acquired,
            "position": len(self.get_all_items()) + 1
        }
        self.list.set(key, item, timestamp=ts)

    def update_item(self, key, **fields):
        if key not in self.list.state:
            self.add_item(key, name=fields.get("name", key))

        ts = self.tick()

        current_item = self.list.state[key].value
        if current_item is None:  # item was deleted
            return
            
        item = current_item.copy()
        for k, v in fields.items():
            item[k] = v

        self.list.set(key, item, timestamp=ts)

    def delete_item(self, key):
        self.list.delete(key)

    def get_item(self, key):
        return self.list.get(key)

    def get_all_items(self):
        return {key: value for key, value in self.list.items()}

    def merge(self, other):
        merged = ShopList(self.id)

        merged.logical_clock = max(self.logical_clock, other.logical_clock)

        merged.list = self.list.merge(other.list)
        return merged

    def to_dict(self):
        """Returns a clean Python dictionary (Good for psycopg2.extras.Json)."""
        return {
            "id": self.id,
            "clock": self.logical_clock, 
            "list": self.list.to_dict()   
        }

    def to_json(self):
        """Returns a JSON String (Good for sending over Network or SQLite)."""
        return json.dumps(self.to_dict())

    @staticmethod
    def from_json(json_str):
        """Recreates the ShopList from a JSON string."""
        data = json.loads(json_str)
        sl = ShopList(list_id=data["id"]) 

        sl.logical_clock = data.get("clock", 0)
        
        sl.list = LWWMap.from_dict(data["list"])
        return sl

    def __repr__(self):
        return f"ShopList(id={self.id}, list={self.list})"