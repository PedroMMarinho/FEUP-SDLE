import sys
import uuid
from src.common.crdt.improved.ShoppingList import ShoppingList 
from src.client.storage import ShoppingListStorage
from src.common.threadPool.threadPool import ThreadPool

class ClientInterface:
    def __init__(self, client_id, communicator, storage):
        self.client_id = client_id
        self.communicator = communicator
        self.storage = storage
        self.thread_pool = ThreadPool(num_threads=4)
        self.thread_pool.submit(self.communicator.run)
        self.thread_pool.submit(self.communicator.heartbeat)

    def print_help(self):
        print("\n--- COMMANDS ---")
        print("1. [c]reate <name>       -> Create a new list")
        print("2. [l]ist                -> Show all local lists")
        print("3. [s]how <uuid>         -> Show items in a list")
        print("4. [a]dd <uuid> <item>   -> Add item to list")
        print("5. [d]el <uuid> <item>   -> Delete item from list")
        print("6. [u]pdate <uuid> <item> <qty_needed> [qty_acquired] -> Update item quantities")
        print("7. [r]equest <uuid>      -> Request full list from network")
        print("8. [delete] <uuid>         -> Delete local list")
        print("9. [q]uit                -> Exit")

    def create_list(self, name):
        list_uuid = str(uuid.uuid4())
        
        sl = ShoppingList(list_uuid, name)

        self.storage.save_list(sl, name)
        print(f"Created list '{name}' with ID: {list_uuid}")

        self.thread_pool.submit(self.communicator.send_full_list, sl)
        self.communicator.subscribe_to_list(sl.uuid)

    def show_lists(self):
        rows = self.storage.get_all_lists_metadata()
        if not rows:
            print("=== NO LISTS FOUND ===")
            return
        print("\n=== YOUR LISTS ===")
        for r in rows:
            print(f"ID: {r[0]} | Name: {r[1]}")

    def show_list_content(self, list_uuid):
        """Displays the items using the CRDT's get_visible_items method."""
        sl = self.storage.get_list_by_id(list_uuid)
        
        if not sl:
            print("Error: List not found.")
            return

        visible_items = sl.get_visible_items()
        
        print(f"\n--- CONTENT OF {list_uuid} ---")
        if not visible_items:
            print("[Empty]")
            return

        for name, data in visible_items.items():
            needed = data['needed']
            acquired = data['acquired']
            status = "[x]" if acquired >= needed else "[ ]"
            print(f" {status} {name} (Need: {needed}) | (Got: {acquired})")

    def add_item(self, list_uuid, item_name, quantity=1, acquired=0):
        sl = self.storage.get_list_by_id(list_uuid)
        if not sl:
            print("Error: List not found.")
            return
        
        try:
            qty_needed = int(quantity)
        except ValueError:
            qty_needed = 1
        
        try:
            acquired = int(acquired)
        except ValueError:
            acquired = 0

        sl.add_item(name=item_name, needed_amount=qty_needed, acquired_amount=acquired)
        
        self.storage.save_list(sl,sl.name)
        print(f"Added '{item_name}' (Need: {qty_needed}) | (Got: {acquired}) to list.")
        self.thread_pool.submit(self.communicator.send_full_list, sl)

    def update_item(self, list_uuid, item_name, needed, acquired=None):
        sl = self.storage.get_list_by_id(list_uuid)
        if not sl:
            print("Error: List not found.")
            return

        visible_items = sl.get_visible_items()
        if item_name not in visible_items:
            print(f"Error: Item '{item_name}' not found in list.")
            return

        current_needed = visible_items[item_name]['needed']
        current_acquired = visible_items[item_name]['acquired']

        try:
            target_needed = int(needed)
            target_acquired = int(acquired) if acquired is not None else current_acquired
        except ValueError:
            print("Error: Quantities must be numbers.")
            return

        diff_needed = target_needed - current_needed
        diff_acquired = target_acquired - current_acquired

        if diff_needed != 0:
            sl.update_needed(item_name, diff_needed)
        
        if diff_acquired != 0:
            sl.update_acquired(item_name, diff_acquired)

        self.storage.save_list(sl, sl.name)
        print(f"Updated '{item_name}' -> Need: {target_needed}, Got: {target_acquired}")
        self.thread_pool.submit(self.communicator.send_full_list, sl)

    def delete_item(self, list_uuid, item_name):
        sl = self.storage.get_list_by_id(list_uuid)
        if not sl:
            print("Error: List not found.")
            return

        sl.remove_item(item_name)

        self.storage.save_list(sl, sl.name)
        print(f"Deleted '{item_name}'.")
        self.thread_pool.submit(self.communicator.send_full_list, sl)

    def delete_list (self, list_uuid):
        self.storage.delete_list(list_uuid)
        self.communicator.unsubscribe_from_list(list_uuid)
        print(f"Deleted local list with ID: {list_uuid}")

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
                        self.delete_item(list_uuid=args[0], item_name=args[1])
                    case 'u' if len(args) >= 4:
                        self.update_item(list_uuid=args[0], item_name=args[1], needed=args[2], acquired=args[3])
                    case 'u' if len(args) >= 3:
                        self.update_item(list_uuid=args[0], item_name=args[1], needed=args[2])
                    case 'a' if len(args) >= 4:
                        print( args)
                        self.add_item(args[0], args[1], quantity=args[2], acquired=args[3])
                    case 'a' if len(args) >= 3:
                        self.add_item(args[0], args[1], quantity=args[2])
                    case 'a' if len(args) == 2:
                        self.add_item(args[0], args[1], quantity=1)
                    case 'r' if len(args) >= 1:
                        list_uuid = args[0]
                        print(f"Requesting full list for ID: {list_uuid} from network...")
                        self.communicator.request_full_list(list_uuid)
                    case 'delete' if len(args) >= 1:
                        list_uuid = args[0]
                        self.delete_list(list_uuid)
                    case _:
                        print("Invalid command.")

            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error: {e}")