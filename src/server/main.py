import argparse
import hashlib
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
    parser.add_argument("--servers", type=str, help="file containing known servers", default=None)
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

    storage = ServerStorage(db_config)
    storage.initialize_schema()

    known_servers = []
    if args.servers:
        try:
            with open(args.servers, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        name, port_str = line.split(":")
                        known_servers.append((port_str, hashlib.sha256(f"server_{port_str}".encode()).hexdigest()))
        except Exception as e:
            print(f"[Warning] Could not read known servers file: {e}")



    for name, port in known_servers:
        print(f"[System] Known server: {name} at port {port}")
    
    comm = ServerCommunicator(storage, args.port, known_servers)
    comm.start()
    



    
    




if __name__ == "__main__":  
    main()