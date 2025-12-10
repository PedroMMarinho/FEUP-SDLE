import time
import uuid
from src.client.storage import ShoppingListStorage
from src.common.crdt.improved.ShoppingList import ShoppingList

PG_USER = "postgres"
PG_PASSWORD = "password"  
PG_HOST = "localhost"
PG_PORT = "5432"

# --- CONFIG ---
DB_CONFIG = {
    "dbname": PG_USER,
    "user": PG_USER,
    "password": PG_PASSWORD,
    "host": PG_HOST,
    "port": PG_PORT
}

def test_stale_read_merge():
    print("\n=== STARTING CONCURRENCY TEST ===\n")

    # 1. SETUP STORAGE
    storage = ShoppingListStorage(DB_CONFIG)
    storage.initialize_schema()

    # 2. CREATE A LIST 
    list_id = str(uuid.uuid4())
    print(f"1. Creating shared list: {list_id}")
    
    initial_list = ShoppingList(list_id)
    initial_list.add_item("Bread", 1) 
    storage.save_list(initial_list, name="Merge Test List")

    # 3. SIMULATE "STALE READS"
    print("2. Alice and Bob download the list simultaneously.")
    
    alice_list_copy = storage.get_list_by_id(list_id)
    bob_list_copy = storage.get_list_by_id(list_id)
    
    # 4. ALICE MAKES A CHANGE
    print("3. Alice adds 'Milk' (locally).")
    alice_list_copy.add_item("Milk", 2)

    # 5. BOB MAKES A CHANGE 
    print("4. Bob adds 'Eggs' (locally).")
    bob_list_copy.add_item("Eggs", 12)

    # 6. ALICE SAVES FIRST
    print("5. Alice saves to DB.")
    storage.save_list(alice_list_copy)

    # 7. BOB SAVES SECOND 
    print("6. Bob saves to DB (This triggers the merge).")
    storage.save_list(bob_list_copy)

    # 8. VERIFY RESULTS
    print("\n7. Verifying final state in DB...")
    final_list = storage.get_list_by_id(list_id)
    visible_items = final_list.get_visible_items()
    
    print(f"   [Result] Items found: {list(visible_items.keys())}")

    success = True
    if "Bread" not in visible_items: success = False
    if "Milk" not in visible_items: success = False
    if "Eggs" not in visible_items: success = False

    if success:
        print("\n \u2705 TEST PASSED: All items are present after merge.")
    else:
        print("\n \u274C TEST FAILED: Some items were lost.")

if __name__ == "__main__":
    try:
        test_stale_read_merge()
    except Exception as e:
        print(f"An error occurred: {e}")