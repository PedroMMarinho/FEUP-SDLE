import argparse
import os
import sqlite3
from src.client.clientInterface import ClientInterface
from src.client.clientCommunication import ClientCommunicator

def init_db(db_path):
    """Initializes the database table if it doesn't exist."""
    if not os.path.exists(db_path):
        print(f"[System] Creating new database: {db_path}")
    
    conn = sqlite3.connect(db_path)
    with open('src/client/db.sql', 'r') as f:
        conn.executescript(f.read())
    conn.close()

def main():
    parser = argparse.ArgumentParser(description="Shopping List Client")
    parser.add_argument("--id", required=True, help="Unique Client ID (e.g., UserA)")
    parser.add_argument("--db", default="shopping.db", help="Local DB file")
    args = parser.parse_args()

    init_db(args.db)

    comm = ClientCommunicator(args.db, args.id)
    comm.start() 

    ui = ClientInterface(args.db, args.id, comm)
    
    try:
        ui.loop()
    except Exception as e:
        print(f"Error: {e}")
    finally:
        comm.running = False
        comm.join() 

if __name__ == "__main__":
    main()