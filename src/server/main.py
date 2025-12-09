import argparse
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from src.server.serverCommunication import ServerCommunicator
from src.server.storage import ServerStorage



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
    parser = argparse.ArgumentParser(description="Shopping List Server")
    parser.add_argument("--port", required=True, help="Port to run the server on")
    parser.add_argument("--db", required=True, help="DB Name (e.g. server_1)")
    parser.add_argument("--seed", type=bool, default=False, help="Whether it is the first server to start")
    parser.add_argument("--known_server_port", type=int, help="Port of a known server to connect to if not seeding")
    args = parser.parse_args()

    clean_db_name = args.db.replace(".db", "").lower().replace("-", "_")

    if not args.seed and not args.known_server_port:
        print("[Fatal] Non-seed servers must provide --known_server_port to connect to an existing server.")
        return
    

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

    storage = ServerStorage(db_config)
    storage.initialize_schema()

    comm = ServerCommunicator(storage, args.port, args.seed, args.known_server_port)
    comm.start()
    



    
    




if __name__ == "__main__":  
    main()