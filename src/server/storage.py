import psycopg2
import psycopg2.extras
from psycopg2 import DataError 
from src.common.crdt.shop_list import ShopList
import json
import src.common.readWriteLock as ReadWriteLock

class ShoppingListStorage:
    def __init__(self, db_config):
        self.db_config = db_config
        self.lock = ReadWriteLock.ReadWriteLock()
        try:
            conn = self._get_conn()
            conn.close()
        except Exception as e:
            raise ConnectionError(f"Could not connect to Postgres: {e}")

    def _get_conn(self):
        """Helper to get a fresh connection (Thread-Safe approach)."""
        conn = psycopg2.connect(**self.db_config)
        conn.autocommit = True
        return conn

    def initialize_schema(self):
        """Creates tables if they don't exist."""
        conn = self._get_conn()
        try:
            with open('src/server/db.sql', 'r') as f:
                with conn.cursor() as cur:
                    cur.execute(f.read())
            print("[Storage] Schema initialized.")
        finally:
            conn.close()

    def save_list(self, shop_list, name=None, is_replica=False, intended_server_hash=None):
        self.lock.acquire_write()
        conn = self._get_conn()
        try:
            with conn.cursor() as cursor:
                if name:
                    cursor.execute(
                        "INSERT INTO ShoppingList (uuid, name, crdt, logical_clock, isReplica, intended_server_hash) VALUES (%s, %s, %s, %s, %s, %s) "
                        "ON CONFLICT(uuid) DO UPDATE SET crdt=excluded.crdt, name=excluded.name, logical_clock=excluded.logical_clock, isReplica=excluded.isReplica, intended_server_hash=excluded.intended_server_hash",
                        (shop_list.id, name, psycopg2.extras.Json(shop_list.to_dict()), shop_list.logical_clock, is_replica, intended_server_hash)
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

    def get_all_temporarily_stored_lists(self):
        self.lock.acquire_read()
        conn = self._get_conn()
        lists = []
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT crdt FROM ShoppingList WHERE intended_server_hash IS NOT NULL")
                rows = cursor.fetchall()
                
                for row in rows:
                    json_str = json.dumps(row[0]) 
                    lists.append(ShopList.from_json(json_str))
            return lists
        finally:
            conn.close()

    def get_all_non_replica_lists(self):
        self.lock.acquire_read()
        conn = self._get_conn()
        lists = []
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT crdt FROM ShoppingList WHERE isReplica=FALSE")
                rows = cursor.fetchall()
                
                for row in rows:
                    json_str = json.dumps(row[0]) 
                    lists.append(ShopList.from_json(json_str))
            return lists
        finally:
            conn.close()


