import argparse
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from src.client.clientInterface import ClientInterface
from src.client.clientCommunication import ClientCommunicator
from src.client.storage import ShoppingListStorage

# --- LOCAL POSTGRES CONFIG ---
PG_USER = "postgres"
PG_PASSWORD = "password"  
PG_HOST = "localhost"
PG_PORT = "5432"

def ensure_database_exists(db_name):
    """Checks if the specific client DB exists. If not, creates it."""
    con = psycopg2.connect(
        dbname="postgres", user=PG_USER, password=PG_PASSWORD, host=PG_HOST, port=PG_PORT
    )
    con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    cur.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s", (db_name,))
    exists = cur.fetchone()

    if not exists:
        print(f"[System] Creating new DB: '{db_name}'...")
        cur.execute(f"CREATE DATABASE {db_name}")
    
    cur.close()
    con.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", required=True, help="Unique Client ID")
    parser.add_argument("--db", required=True, help="DB Name (e.g. client_A)")
    args = parser.parse_args()

    clean_db_name = args.db.replace(".db", "").lower().replace("-", "_")

    try:
        ensure_database_exists(clean_db_name)
    except Exception as e:
        print(f"[Fatal] Postgres Error: {e}")
        return

    db_config = {
        "dbname": clean_db_name,
        "user": PG_USER,
        "password": PG_PASSWORD,
        "host": PG_HOST,
        "port": PG_PORT
    }

    storage = ShoppingListStorage(db_config)
    storage.initialize_schema()

    comm = ClientCommunicator(db_config, args.id)
    comm.start()

    ui = ClientInterface(db_config, args.id, comm)
    
    try:
        ui.loop()
    except Exception as e:
        print(f"Crash: {e}")
    finally:
        comm.running = False
        comm.join()

if __name__ == "__main__":
    main()