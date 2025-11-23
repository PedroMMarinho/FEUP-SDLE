import sys
from src.common.crdt.shop_list import ShopList
from src.client.storage import ShoppingListStorage

class ClientInterface:
    def __init__(self, db_config, client_id, communicator):
        self.client_id = client_id
        self.communicator = communicator 
        self.storage = ShoppingListStorage(db_config)

    def print_help(self):
        print("\n--- COMMANDS ---")
        print("1. [c]reate <name>             -> Create a new list")
        print("2. [l]ist                      -> Show all local lists")
        print("3. [s]how <uuid>               -> Show items in a list")
        print("4. [a]dd <uuid> <name> [qty]   -> Add item (optional: quantity)")
        print("5. [u]pdate <uuid> <name> <need> <got> -> Update item quantities")
        print("6. [d]el <uuid> <name>         -> Delete item entirely")
        print("7. [q]uit                      -> Exit")

    def create_list(self, name):
        sl = ShopList() 
        self.storage.save_list(sl, name)
        print(f"Created list '{name}' with ID: {sl.id}")
        self.communicator.send_update(sl.id, sl.to_json())

    def show_lists(self):
        rows = self.storage.get_all_lists_metadata()
        if not rows:
            print("=== NO LISTS FOUND ===")
            return
        print("\n=== YOUR LISTS ===")
        for r in rows:
            # r is (uuid, name)
            print(f"ID: {r[0]} | Name: {r[1]}")

    def show_list_content(self, list_id):
        """Displays the items inside a specific list using the SQL Read-Cache."""
        rows = self.storage.get_list_items_for_display(list_id)
        
        if rows is None:
            print("Error: List not found.")
            return

        print(f"\n--- CONTENT OF {list_id} ---")
        if not rows:
            print("[Empty]")
            return

        for r in rows:
            name = r[0]
            needed = r[1]
            acquired = r[2]
            
            status = "[x]" if acquired >= needed else "[ ]"
            print(f" {status} {name} (Qty: {needed})")

    def add_item(self, list_id, item_name, quantity=1):
        """Adds an item with a specific target quantity."""
        sl = self.storage.get_list_by_id(list_id)
        if not sl:
            print("Error: List not found.")
            return
        
        try:
            qty_needed = int(quantity)
        except ValueError:
            qty_needed = 1

        sl.add_item(key=item_name.lower(), name=item_name, qty_needed=qty_needed)
        
        self.storage.save_list(sl)
        print(f"Added '{item_name}' (Need: {qty_needed})")
        self.communicator.send_update(sl.id, sl.to_json())


    def update_item(self, list_id, item_name, needed, acquired):
        """Updates the quantities of an existing item."""
        sl = self.storage.get_list_by_id(list_id)
        if not sl:
            print("Error: List not found.")
            return

        try:
            qty_n = int(needed)
            qty_a = int(acquired)
        except ValueError:
            print("Error: Quantities must be numbers.")
            return

        sl.update_item(key=item_name.lower(), qty_needed=qty_n, qty_acquired=qty_a)

        self.storage.save_list(sl)
        print(f"Updated '{item_name}' -> Need: {qty_n}, Got: {qty_a}")
        self.communicator.send_update(sl.id, sl.to_json())

    def delete_item(self, list_id, item_name):
        sl = self.storage.get_list_by_id(list_id)
        if not sl:
            print("Error: List not found.")
            return

        # CRDT Logic: This sets the tombstone (value=None)
        sl.delete_item(key=item_name.lower())

        self.storage.save_list(sl)
        print(f"Deleted '{item_name}' (Tombstone set).")

        self.communicator.send_update(sl.id, sl.to_json())

    def loop(self):
        print(f"--- Shopping List Client ({self.client_id}) ---")
        self.print_help()
        
        while True:
            try:
                user_input = input(f"\n({self.client_id}) > ").strip().split()
                if not user_input: continue
                
                cmd = user_input[0].lower()
                args = user_input[1:]

                match cmd:
                    case 'q':
                        break
                    case 'help':
                        self.print_help()
                    case 'l':
                        self.show_lists()
                    case 's' if len(args) >= 1:
                        self.show_list_content(args[0])
                    case 'c' if len(args) >= 1:
                        self.create_list(args[0])
                    case 'd' if len(args) >= 2:
                        self.delete_item(args[0], args[1])
                    case 'a' if len(args) == 2:
                        self.add_item(args[0], args[1], quantity=1)
                    case 'a' if len(args) >= 3:
                        self.add_item(args[0], args[1], quantity=args[2])
                    case 'u' if len(args) >= 4:
                        self.update_item(args[0], args[1], args[2], args[3])
                    case _:
                        print("Invalid command.")

            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error: {e}")