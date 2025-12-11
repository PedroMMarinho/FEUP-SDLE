import psycopg2
import psycopg2.extras
from psycopg2 import DataError
import json

from src.common.crdt.improved.ShoppingList import ShoppingList, PNCounter, ORSet
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
        conn = psycopg2.connect(**self.db_config)
        conn.autocommit = True
        return conn


    def _crdt_to_dict(self, obj):
        """Recursively converts CRDT objects to a dict for JSON."""
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
        """Mirror of the client's reconstruction logic."""
        sl = ShoppingList(list_id=data['id'])
        sl.clock = data.get('clock', 0)

        if 'uuid' in data:
            sl.uuid = data['uuid']

        for name, item_data in data.get('items', {}).items():

            # reconstruct needed PNCounter
            needed = PNCounter()
            needed.positive.counts = item_data["needed"]["positive"]["counts"]
            needed.negative.counts = item_data["needed"]["negative"]["counts"]

            # reconstruct acquired PNCounter
            acquired = PNCounter()
            acquired.positive.counts = item_data["acquired"]["positive"]["counts"]
            acquired.negative.counts = item_data["acquired"]["negative"]["counts"]

            # reconstruct OR-Set
            existence = ORSet()
            existence.elements = {tuple(x) for x in item_data["existence"]["elements"]}
            existence.tombstones = {tuple(x) for x in item_data["existence"]["tombstones"]}

            sl.items[name] = {
                "needed": needed,
                "acquired": acquired,
                "existence": existence
            }

        return sl

    def save_list(self, shop_list, name=None, is_replica=False, intended_server_hash=None):
        self.lock.acquire_write()
        conn = self._get_conn()

        try:
            with conn:
                with conn.cursor() as cursor:
                    list_uuid = getattr(shop_list, "uuid", shop_list.id)

                    # Load existing CRDT
                    cursor.execute(
                        "SELECT crdt, name FROM ShoppingList WHERE uuid=%s FOR UPDATE",
                        (list_uuid,)
                    )
                    row = cursor.fetchone()

                    if row:
                        db_crdt_json, existing_name = row

                        if name is None:
                            name = existing_name

                        if isinstance(db_crdt_json, str):
                            db_crdt_json = json.loads(db_crdt_json)

                        # Reconstruct server CRDT
                        db_list = self._reconstruct_crdt(db_crdt_json)

                        # Merge like client
                        shop_list.merge(db_list)

                    if name is None:
                        name = "Unnamed List"

                    # convert final merged crdt to dict
                    crdt_blob = json.dumps(self._crdt_to_dict(shop_list))

                    # Save merged version with server-only fields
                    cursor.execute(
                        """
                        INSERT INTO ShoppingList (uuid, name, crdt, logical_clock, isReplica, intended_server_hash)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT(uuid) DO UPDATE SET
                            crdt = excluded.crdt,
                            name = excluded.name,
                            logical_clock = excluded.logical_clock,
                            isReplica = excluded.isReplica,
                            intended_server_hash = excluded.intended_server_hash
                        """,
                        (
                            list_uuid,
                            name,
                            crdt_blob,
                            shop_list.clock,
                            is_replica,
                            intended_server_hash
                        )
                    )

                    # Rebuild materialized items exactly like client
                    cursor.execute("DELETE FROM ShoppingListItem WHERE shopping_list_uuid=%s", (list_uuid,))

                    visible_items = shop_list.get_visible_items()
                    items_to_insert = []

                    for item_name, counts in visible_items.items():
                        display_needed = max(0, counts['needed'])
                        display_acquired = max(0, counts['acquired'])

                        items_to_insert.append(
                            (list_uuid, item_name, display_needed, display_acquired, 0)
                        )

                    if items_to_insert:
                        cursor.executemany(
                            """
                            INSERT INTO ShoppingListItem (shopping_list_uuid, name, quantityNeeded, quantityAcquired, position)
                            VALUES (%s, %s, %s, %s, %s)
                            """,
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

                if not row:
                    return None

                crdt_data = row[0]
                if isinstance(crdt_data, str):
                    crdt_data = json.loads(crdt_data)

                return self._reconstruct_crdt(crdt_data)

        finally:
            conn.close()
            self.lock.release_read()

    def get_all_temporarily_stored_lists(self):
        self.lock.acquire_read()
        conn = self._get_conn()
        lists = []

        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT crdt FROM ShoppingList WHERE intended_server_hash IS NOT NULL")
                rows = cursor.fetchall()

                for row in rows:
                    crdt_data = row[0]
                    if isinstance(crdt_data, str):
                        crdt_data = json.loads(crdt_data)
                    lists.append(self._reconstruct_crdt(crdt_data))

            return lists

        finally:
            conn.close()
            self.lock.release_read()

    def get_all_non_replica_lists(self):
        self.lock.acquire_read()
        conn = self._get_conn()
        lists = []

        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT crdt FROM ShoppingList WHERE isReplica=FALSE")
                rows = cursor.fetchall()

                for row in rows:
                    crdt_data = row[0]
                    if isinstance(crdt_data, str):
                        crdt_data = json.loads(crdt_data)
                    lists.append(self._reconstruct_crdt(crdt_data))

            return lists

        finally:
            conn.close()
            self.lock.release_read()

    def get_all_replicas(self):
        self.lock.acquire_read()
        conn = self._get_conn()
        lists = []

        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT crdt FROM ShoppingList WHERE isReplica=TRUE")
                rows = cursor.fetchall()

                for row in rows:
                    crdt_data = row[0]
                    if isinstance(crdt_data, str):
                        crdt_data = json.loads(crdt_data)
                    lists.append(self._reconstruct_crdt(crdt_data))

            return lists

        finally:
            conn.close()
            self.lock.release_read()