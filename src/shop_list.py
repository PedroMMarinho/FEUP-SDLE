import uuid
from .lww_map import LWWMap


class ShopList:
    
    def __init__(self, list_id=None):
        self.id = list_id if list_id else uuid.uuid4().hex
        self.list = LWWMap()

    def add_item(self, key, name, qty_needed=1, qty_acquired=0):
        item = {
            "name": name,
            "qty_needed": qty_needed,
            "qty_acquired": qty_acquired,
            "position": len(self.get_all_items()) + 1
        }
        self.list.set(key, item)

    def update_item(self, key, **fields):
        if key not in self.list.state:
            return

        current_item = self.list.state[key].value
        if current_item is None:  # item was deleted
            return
            
        item = current_item.copy()
        for k, v in fields.items():
            item[k] = v

        self.list.set(key, item)

    def delete_item(self, key):
        self.list.delete(key)

    def get_item(self, key):
        return self.list.get(key)

    def get_all_items(self):
        return {key: value for key, value in self.list.items()}

    def merge(self, other):
        merged = ShopList(self.id)
        merged.list = self.list.merge(other.list)
        return merged

    def __repr__(self):
        return f"ShopList(id={self.id}, list={self.list})"