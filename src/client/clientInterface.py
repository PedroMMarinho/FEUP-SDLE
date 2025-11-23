import sqlite3
from src.common.crdt.shop_list import ShopList

class ClientInterface:
    def __init__(self, db_path, client_id, communicator):
        self.db_path = db_path
        self.client_id = client_id
        self.communicator = communicator # Reference to the thread to send data
        self.conn = sqlite3.connect(db_path, check_same_thread=False) # Main thread connection
        self.cursor = self.conn.cursor()

    def print_help(self):
        print("\n--- COMMANDS ---")
        print("1. [c]reate <name>  -> Create a new list")
        print("2. [l]ist           -> Show all local lists")
        print("3. [a]dd <id> <item>-> Add item to list")
        print("4. [s]how <id>      -> Show content of a list")
        print("5. [q]uit           -> Exit")

    def create_list(self, name):
        sl = ShopList() 
        
        # Save to DB 
        self.cursor.execute("INSERT INTO ShoppingList (uuid, name, crdt) VALUES (?, ?, ?)", 
                            (sl.id, name, sl.to_json()))
        self.conn.commit()
        print(f"Created list '{name}' with ID: {sl.id}")
        
        # Notify Network
        self.communicator.send_update(sl.id, sl.to_json())

    def show_lists(self):
        self.cursor.execute("SELECT uuid, name FROM ShoppingList")
        rows = self.cursor.fetchall()
        if not rows:
            print("=== NO LISTS FOUND ===")
            return
        print("\n=== YOUR LISTS ===")
        for r in rows:
            print(f"ID: {r[0]} | Name: {r[1]}")

    def loop(self):
        print(f"--- Shopping List Client ({self.client_id}) ---")
        self.print_help()
        
        while True:
            try:
                cmd_raw = input(f"\n({self.client_id}) > ").strip().split()
                if not cmd_raw: continue
                
                cmd = cmd_raw[0].lower()

                if cmd == 'q':
                    print("Exiting...")
                    break
                elif cmd == 'c' and len(cmd_raw) > 1:
                    self.create_list(cmd_raw[1])
                elif cmd == 'l':
                    self.show_lists()
                else:
                    print("Unknown command or missing arguments.")

            except KeyboardInterrupt:
                break