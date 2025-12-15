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
    storage.save_list(initial_list, name="Merge Test List")

    # 3. SIMULATE "STALE READS"
    print("2. Alice and Bob download the list simultaneously.")
    
    alice_list_copy = storage.get_list_by_id(list_id)
    bob_list_copy = storage.get_list_by_id(list_id)
    
    # 4. ALICE MAKES A CHANGE
    print("3. Alice adds 'Milk' (locally).")
    alice_list_copy.add_item("Milk", "Alice", 3)

    print("4. Bob adds 'Milk' (locally).")
    bob_list_copy.add_item("Milk", "Bob", 12)

    # 5. BOTH SAVE THEIR CHANGES
    print("5. Alice saves her changes to storage.")
    storage.save_list(alice_list_copy, name="Merge Test List")
    print("6. Bob saves his changes to storage.")
    storage.save_list(bob_list_copy, name="Merge Test List")

    # 6. VERIFY MERGE RESULT
    print("7. Retrieving merged list from storage.")
    final_list = storage.get_list_by_id(list_id)
    visible_items = final_list.get_visible_items()
    print(f"Visible items after merge: {visible_items}")

    print(final_list.to_json())
    print(final_list.get_visible_items())

    expected_needed = 15
    actual_needed = visible_items.get("Milk", {}).get("needed", 0)
    assert actual_needed == expected_needed, f"Merge failed: expected {expected_needed}, got {actual_needed}"

if __name__ == "__main__":
    try:
        test_stale_read_merge()
    except Exception as e:
        print(f"An error occurred: {e}")