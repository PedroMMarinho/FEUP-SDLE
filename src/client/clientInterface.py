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
        print("1. [c]reate <name>       -> Create a new list")
        print("2. [l]ist                -> Show all local lists")
        print("3. [s]how <uuid>         -> Show items in a list")
        print("4. [a]dd <uuid> <item>   -> Add item to list")
        print("5. [d]el <uuid> <item>   -> Delete item from list")
        print("6. [u]pdate <uuid> <item> <qty_acquired> -> Update item quantity acquired")
        print("7. [q]uit                -> Exit")

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
        name, rows = self.storage.get_list_items_for_display(list_id)
        
        if rows is None:
            print("Error: List not found.")
            return

        print(f"\n--- CONTENT OF {name} {list_id} ---")
        if not rows:
            print("[Empty]")
            return

        for r in rows:
            name = r[0]
            needed = r[1]
            acquired = r[2]
            
            status = "[x]" if acquired >= needed else "[ ]"
            print(f" {status} {name} (Need: {needed}) | (Got: {acquired})")

    def add_item(self, list_id, item_name):
        sl = self.storage.get_list_by_id(list_id)
        if not sl:
            print("Error: List not found.")
            return
        
        sl.add_item(key=item_name.lower(), name=item_name)
        
        self.storage.save_list(sl)
        print(f"Added '{item_name}' to list.")
        
        self.communicator.send_update(sl.id, sl.to_json())

    def update_item(self, list_id, item_name, **fields):
        sl = self.storage.get_list_by_id(list_id)
        if not sl:
            print("Error: List not found.")
            return

        sl.update_item(key=item_name.lower(), **fields)

        self.storage.save_list(sl)
        print(f"Updated '{item_name}' with {fields}.")

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
                        print("Exiting...")
                        break
                    
                    case 'help':
                        self.print_help()

                    case 'c' if len(args) >= 1:
                        self.create_list(args[0])

                    case 'l':
                        self.show_lists()

                    case 's' if len(args) >= 1:
                        self.show_list_content(args[0])

                    case 'a' if len(args) >= 2:
                        self.add_item(list_id=args[0], item_name=args[1])

                    case 'd' if len(args) >= 2:
                        self.delete_item(list_id=args[0], item_name=args[1])
                    case 'u' if len(args) >= 3:
                        self.update_item(list_id=args[0], item_name=args[1], qty_acquired=int(args[2]))

                    case _:
                        print("Invalid command or missing arguments. Type 'help' for usage.")

            except KeyboardInterrupt:
                print("\nStopping...")
                break
            except Exception as e:
                print(f"Unexpected Error: {e}")