import psycopg2
import psycopg2.extras
from psycopg2 import DataError 
from src.common.crdt.shop_list import ShopList
from src.common.readWriteLock.read_write_lock import ReadWriteLock
import json

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
        """Helper to get a fresh connection (Thread-Safe approach)."""
        conn = psycopg2.connect(**self.db_config)
        conn.autocommit = True
        return conn

    def initialize_schema(self):
        """Creates tables if they don't exist."""
        conn = self._get_conn()
        try:
            with open('src/client/db.sql', 'r') as f:
                with conn.cursor() as cur:
                    cur.execute(f.read())
            print("[Storage] Schema initialized.")
        finally:
            conn.close()

    def save_list(self, shop_list, name=None):
        self.lock.acquire_write()
        conn = self._get_conn()
        try:
            with conn.cursor() as cursor:
                if name:
                    cursor.execute(
                        "INSERT INTO ShoppingList (uuid, name, crdt, logical_clock) VALUES (%s, %s, %s, %s) "
                        "ON CONFLICT(uuid) DO UPDATE SET crdt=excluded.crdt, name=excluded.name, logical_clock=excluded.logical_clock",
                        (shop_list.id, name, psycopg2.extras.Json(shop_list.to_dict()), shop_list.logical_clock)
                    )
                else:
                    cursor.execute(
                        "UPDATE ShoppingList SET crdt=%s, logical_clock=%s WHERE uuid=%s",
                        (psycopg2.extras.Json(shop_list.to_dict()), shop_list.logical_clock, shop_list.id)
                    )

                cursor.execute("DELETE FROM ShoppingListItem WHERE shopping_list_uuid=%s", (shop_list.id,))
                
                items_to_insert = []
                for key, item in shop_list.list.items():
                    items_to_insert.append((
                        shop_list.id,
                        item['name'],
                        item['qty_needed'],
                        item['qty_acquired'],
                        item.get('position', 0)
                    ))
                    
                if items_to_insert:
                    cursor.executemany(
                        "INSERT INTO ShoppingListItem (shopping_list_uuid, name, quantityNeeded, quantityAcquired, position) "
                        "VALUES (%s, %s, %s, %s, %s)",
                        items_to_insert
                    )
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
                    json_str = json.dumps(row[0]) 
                    return ShopList.from_json(json_str)
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