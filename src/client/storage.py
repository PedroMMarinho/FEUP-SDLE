import psycopg2
from psycopg2 import DataError 
import json
from src.common.crdt.improved.ShoppingList import ShoppingList, PNCounter, ORSet
from src.common.readWriteLock.read_write_lock import ReadWriteLock

class ShoppingListStorage:
    def __init__(self, db_config):
        self.db_config = db_config
        try:
            conn = self._get_conn()
            conn.close()
        except Exception as e:
            raise ConnectionError(f"Could not connect to Postgres: {e}")
        self.lock = ReadWriteLock()

    def _get_conn(self):
        conn = psycopg2.connect(**self.db_config)
        conn.autocommit = True
        return conn

    def initialize_schema(self):
        """Creates tables if they don't exist."""
        conn = self._get_conn()
        try:
            with conn:
                with open('src/client/db.sql', 'r') as f:
                    with conn.cursor() as cur:
                        cur.execute(f.read())
                print("[Storage] Schema initialized.")
        finally:
            conn.close()

    def _crdt_to_dict(self, obj):
        """Recursively converts CRDT objects to a dictionary for JSON storage."""
        if hasattr(obj, "__dict__"):
            data = {}
            for key, val in obj.__dict__.items():
                data[key] = self._crdt_to_dict(val)
            return data
        elif isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, dict):
            return {k: self._crdt_to_dict(v) for k, v in obj.items()}
        else:
            return obj

    def _reconstruct_crdt(self, data):
        sl = ShoppingList(data['uuid'])
        sl.clock = data['clock']
        sl.name = data.get('name', None)
        
        if 'uuid' in data:
            sl.uuid = data['uuid']

        for name, item_data in data.get('items', {}).items():
            
            needed = PNCounter()
            if 'positive' in item_data['needed']:
                needed.positive.counts = item_data['needed']['positive']['counts']
            if 'negative' in item_data['needed']:
                needed.negative.counts = item_data['needed']['negative']['counts']

            acquired = PNCounter()
            if 'positive' in item_data['acquired']:
                acquired.positive.counts = item_data['acquired']['positive']['counts']
            if 'negative' in item_data['acquired']:
                acquired.negative.counts = item_data['acquired']['negative']['counts']

            existence = ORSet()
            if 'elements' in item_data['existence']:
                existence.elements = {tuple(x) for x in item_data['existence']['elements']}
            if 'tombstones' in item_data['existence']:
                existence.tombstones = {tuple(x) for x in item_data['existence']['tombstones']}

            sl.items[name] = {
                "needed": needed,
                "acquired": acquired,
                "existence": existence
            }

        return sl

    def save_list(self, shop_list, name=None, not_sent=False):
        self.lock.acquire_write()
        conn = self._get_conn()
        try:
            with conn:
                with conn.cursor() as cursor:
                    list_uuid = shop_list.uuid

                    cursor.execute(
                        "SELECT crdt, name FROM ShoppingList WHERE uuid=%s FOR UPDATE", 
                        (list_uuid,)
                    )
                    row = cursor.fetchone()

                    if row:
                        db_crdt_json = row[0]
                        existing_name = row[1]
                        if name is None:
                            name = existing_name

                        if isinstance(db_crdt_json, str):
                            db_crdt_json = json.loads(db_crdt_json)
                        
                        db_list = self._reconstruct_crdt(db_crdt_json)
                        #print(f"[Storage] Merging local list '{list_uuid}' with database version.")
                        shop_list.merge(db_list)

                    crdt_blob = json.dumps(self._crdt_to_dict(shop_list))
                    
                    cursor.execute(
                        """
                        INSERT INTO ShoppingList (uuid, name, crdt, logical_clock, notSent)
                        VALUES (%s, %s, %s, %s, %s)
                        ON CONFLICT(uuid) DO UPDATE
                        SET crdt = excluded.crdt,
                            name = COALESCE(excluded.name, ShoppingList.name),
                            logical_clock = excluded.logical_clock,
                            notSent = excluded.notSent
                        """,
                        (list_uuid, name, crdt_blob, shop_list.clock, not_sent)
                    )
                    cursor.execute("DELETE FROM ShoppingListItem WHERE shopping_list_uuid=%s", (list_uuid,))
                    
                    visible_items = shop_list.get_visible_items()
                    items_to_insert = []
                    
                    for item_name, counts in visible_items.items():

                        raw_needed = counts['needed']
                        raw_acquired = counts['acquired']

                        display_needed = max(0, raw_needed)
                        display_acquired = max(0, raw_acquired)

                        items_to_insert.append((
                            list_uuid,
                            item_name,
                            display_needed,
                            display_acquired,
                            0 
                        ))
                        
                    if items_to_insert:
                        cursor.executemany(
                            "INSERT INTO ShoppingListItem (shopping_list_uuid, name, quantityNeeded, quantityAcquired, position) "
                            "VALUES (%s, %s, %s, %s, %s)",
                            items_to_insert
                        )
        except Exception as e:
            print(f"[Storage] Error saving list: {e}")
            raise e
        finally:
            conn.close()
            self.lock.release_write()

    def get_list_by_id(self, list_id):
        self.lock.acquire_read()
        conn = self._get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT crdt FROM ShoppingList WHERE uuid=%s", (list_id,))
                row = cursor.fetchone()
                
                if row:
                    crdt_data = row[0]
                    if isinstance(crdt_data, str):
                        crdt_data = json.loads(crdt_data)
                    
                    return self._reconstruct_crdt(crdt_data)
                return None
        except DataError:
            return None
        finally:
            conn.close()
            self.lock.release_read()

    def get_all_lists_metadata(self):
        self.lock.acquire_read()
        conn = self._get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT uuid, name FROM ShoppingList")
                return cursor.fetchall()
        finally:
            conn.close()
            self.lock.release_read()

    def get_list_items_for_display(self, list_id):
        self.lock.acquire_read()
        conn = self._get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT name FROM ShoppingList WHERE uuid=%s", 
                    (list_id,)
                )
                row = cursor.fetchone()
                if not row:
                    return (None, None)
                
                cursor.execute(
                    "SELECT name, quantityNeeded, quantityAcquired FROM ShoppingListItem WHERE shopping_list_uuid=%s", 
                    (list_id,)
                )
                return (row[0], cursor.fetchall())
        except DataError:
            return None
        finally:
            conn.close()
            self.lock.release_read()

    def get_all_not_sent_lists_and_metadata(self):
        """
        Returns all not sent shopping lists with full CRDT data and metadata.
        """
        self.lock.acquire_read()
        conn = self._get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT uuid, name, crdt, logical_clock, notSent
                    FROM ShoppingList
                    WHERE notSent = TRUE
                    """
                )

                rows = cursor.fetchall()
                shopping_lists = []

                for row in rows:
                    uuid, name, crdt_data, logical_clock, not_sent = row

                    if isinstance(crdt_data, str):
                        crdt_data = json.loads(crdt_data)

                    shopping_list = self._reconstruct_crdt(crdt_data)
                    shopping_list.uuid = uuid
                    shopping_list.name = name
                    shopping_list.clock = logical_clock

                    shopping_lists.append(shopping_list)

                return shopping_lists

        finally:
            conn.close()
            self.lock.release_read()
            
    def delete_list(self, list_id):
        self.lock.acquire_write()
        conn = self._get_conn()
        try:
            with conn:
                with conn.cursor() as cursor:
                    cursor.execute(
                        "DELETE FROM ShoppingList WHERE uuid=%s",
                        (list_id,)
                    )
                    cursor.execute(
                        "DELETE FROM ShoppingListItem WHERE shopping_list_uuid=%s",
                        (list_id,)
                    )
            print(f"[Storage] Deleted list with ID: {list_id}")
        except Exception as e:
            print(f"[Storage] Error deleting list: {e}")
            raise e
        finally:
            conn.close()
            self.lock.release_write()

