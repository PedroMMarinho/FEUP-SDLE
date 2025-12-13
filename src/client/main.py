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
    parser.add_argument("--proxies", type=str, help="file containing known proxies", default=None)
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

    known_proxy_ports = []
    if args.proxies:
        try:
            with open(args.proxies, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        name, port_str = line.split(":")
                        known_proxy_ports.append(port_str)
        except Exception as e:
            print(f"[Warning] Could not read known proxies file: {e}")

    storage = ShoppingListStorage(db_config)
    storage.initialize_schema()

    comm = ClientCommunicator(db_config, args.id, known_proxy_ports, storage)

    ui = ClientInterface(args.id, comm, storage)
    
    try:
        ui.loop()
    except Exception as e:
        print(f"Crash: {e}")

if __name__ == "__main__":
    main()