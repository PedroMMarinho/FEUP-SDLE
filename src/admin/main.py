import argparse
import subprocess
import os
import zmq
import json
import sys
import time
from src.common.messages.messages import MessageType, Message

# --- Configuration Constants ---
DB_USER_BASE_PORT = 7000    # Clients start here
DB_SERVER_BASE_PORT = 8000  # Servers start here

SERVER_BASE_PORT = 5555
PROXY_BASE_PORT = 6000
PROXY_INCREMENT = 2

SERVER_LOG_DIR = "src/server/server_logs"
PROXY_LOG_DIR = "src/proxy/proxy_logs"
DB_REG_DIR = "src/db"

# PID files
SERVER_PIDS_FILE = os.path.join(SERVER_LOG_DIR, "server_pids.txt")
PROXY_PIDS_FILE = os.path.join(PROXY_LOG_DIR, "proxy_pids.txt")
DB_REGISTRY_FILE = os.path.join(DB_REG_DIR, "db_registry.txt")

DB_PASSWORD = "password"


def run_command(cmd, check=True):
    """Helper to run shell commands quietly."""
    return subprocess.run(cmd, shell=True, check=check, stdout=subprocess.PIPE, stderr=subprocess.PIPE)


def provision_db_container(identifier, base_port_start):
    """
    Generic function to spin up a Postgres container for EITHER a User OR a Server.
    Returns: (int) The assigned port.
    """
    os.makedirs(DB_REG_DIR, exist_ok=True)

    registry = {}
    
    if os.path.exists(DB_REGISTRY_FILE):
        with open(DB_REGISTRY_FILE, "r") as f:
            for line in f:
                line = line.strip()
                if ":" in line:
                    u, p = line.split(":")
                    registry[u] = int(p)

    if identifier in registry:
        port = registry[identifier]
        print(f"[Admin] Found existing DB port {port} for {identifier}", file=sys.stderr)
    else:
        used_ports_in_range = [p for p in registry.values() if p >= base_port_start and p < base_port_start + 1000]
        if used_ports_in_range:
            port = max(used_ports_in_range) + 1
        else:
            port = base_port_start
        
        with open(DB_REGISTRY_FILE, "a") as f:
            f.write(f"{identifier}:{port}\n")
        print(f"[Admin] Assigned new DB port {port} to {identifier}", file=sys.stderr)

    container_name = f"shopping-db-{identifier}"
    
    check_exists = run_command(f"docker ps -aq -f name={container_name}", check=False)
    container_id = check_exists.stdout.decode().strip()

    if container_id:
        check_running = run_command(f"docker ps -q -f name={container_name}", check=False)
        if not check_running.stdout.decode().strip():
            print(f"[Admin] Container {container_name} stopped. Starting...", file=sys.stderr)
            run_command(f"docker start {container_name}")
            time.sleep(2) 
        else:
            print(f"[Admin] Container {container_name} is already running.", file=sys.stderr)
    else:
        print(f"[Admin] Creating new container {container_name} on port {port}...", file=sys.stderr)
        run_command(
            f"docker run --name {container_name} "
            f"-e POSTGRES_PASSWORD={DB_PASSWORD} "
            f"-p {port}:5432 -d postgres"
        )
        print(f"[Admin] Waiting for Database to initialize...", file=sys.stderr)
        time.sleep(3) 

    return port


def setup_db(user_id):
    """
    Called by Makefile for Clients.
    Prints the port to stdout so Makefile captures it.
    """
    port = provision_db_container(user_id, DB_USER_BASE_PORT)
    print(port)


def initial_setup():
    """
    Matches: make servers
    Starts: Server_1..5 (With DBs) and Proxy_1..2
    """
    os.makedirs(SERVER_LOG_DIR, exist_ok=True)
    os.makedirs(PROXY_LOG_DIR, exist_ok=True)

    # Clean old lists and PID files
    for fpath in [
        f"{SERVER_LOG_DIR}/known_servers.txt",
        f"{PROXY_LOG_DIR}/known_proxies.txt",
        SERVER_PIDS_FILE,
        PROXY_PIDS_FILE
    ]:
        if os.path.exists(fpath):
            os.remove(fpath)

    print("Generating network topology...")
    with open(f"{SERVER_LOG_DIR}/known_servers.txt", "w") as f:
        for i in range(1, 6):
            port = SERVER_BASE_PORT + i - 1
            f.write(f"Server_{i}:{port}\n")

    with open(f"{PROXY_LOG_DIR}/known_proxies.txt", "w") as f:
        for i in range(1, 3):
            port = PROXY_BASE_PORT + PROXY_INCREMENT*i - 1
            f.write(f"Proxy_{i}:{port}\n")

    print("Starting Servers...")
    with open(SERVER_PIDS_FILE, "w") as pid_f:
        for i in range(1, 6):
            server_name = f"Server_{i}"
            server_port = SERVER_BASE_PORT + i - 1
            
            db_port = provision_db_container(server_name, DB_SERVER_BASE_PORT)
            
            server_env = os.environ.copy()
            server_env["DB_HOST"] = "localhost"
            server_env["DB_PORT"] = str(db_port)
            server_env["DB_USER"] = "postgres"     
            server_env["DB_PASSWORD"] = DB_PASSWORD

            proc = subprocess.Popen([
                sys.executable, "-u", "-m", "src.server.main", 
                "--port", str(server_port),
                "--db", server_name, 
                "--servers", f"{SERVER_LOG_DIR}/known_servers.txt"
            ], env=server_env, stdout=open(f"{SERVER_LOG_DIR}/server{i}.log", "w"), stderr=subprocess.STDOUT)
            
            pid_f.write(f"{proc.pid}\n")
            
    print(f"Initial 5 servers started (DBs on ports 8000+). PIDs in {SERVER_PIDS_FILE}.")

    # --- 3. START PROXIES ---
    print("Starting Proxies...")
    with open(PROXY_PIDS_FILE, "w") as pid_w:
        for i in range(1, 3):
            port = PROXY_BASE_PORT + PROXY_INCREMENT*i - 1
            proc = subprocess.Popen([
                sys.executable, "-u", "-m", "src.proxy.main",   
                "--port", str(port),
                "--proxies", f"{PROXY_LOG_DIR}/known_proxies.txt",
                "--servers", f"{SERVER_LOG_DIR}/known_servers.txt",
            ], stdout=open(f"{PROXY_LOG_DIR}/proxy{i}.log", "w"), stderr=subprocess.STDOUT)
            
            pid_w.write(f"{proc.pid}\n")

    print(f"Initial 2 proxies started.")


def add_server():
    """
    Matches: make additional_server
    """
    os.makedirs(SERVER_LOG_DIR, exist_ok=True)

    existing = sorted([
        f for f in os.listdir(SERVER_LOG_DIR)
        if f.startswith("server") and f.endswith(".log")
    ])

    next_id = len(existing) + 1
    server_name = f"Server_{next_id}"
    next_port = SERVER_BASE_PORT + next_id - 1
    logfile = f"{SERVER_LOG_DIR}/server{next_id}.log"

    print(f"Provisioning {server_name} on port {next_port}...")

    db_port = provision_db_container(server_name, DB_SERVER_BASE_PORT)

    with open(f"{SERVER_LOG_DIR}/known_servers.txt", "a") as f:
        f.write(f"{server_name}:{next_port}\n")

    server_env = os.environ.copy()
    server_env["DB_HOST"] = "localhost"
    server_env["DB_PORT"] = str(db_port)
    server_env["DB_USER"] = "postgres"
    server_env["DB_PASSWORD"] = DB_PASSWORD

    proc = subprocess.Popen([
        sys.executable, "-m", "src.server.main",
        "--port", str(next_port),
        "--db", server_name,
        "--servers", f"{SERVER_LOG_DIR}/known_servers.txt",
    ], env=server_env, stdout=open(logfile, "w"), stderr=subprocess.STDOUT)

    with open(SERVER_PIDS_FILE, "a") as pid_f:
        pid_f.write(f"{proc.pid}\n")

    print(f"{server_name} launched (PID: {proc.pid}, DB Port: {db_port}).")


def remove_server(server_name):
    """
    Sends a REMOVE_SERVER message to the target server.
    """
    target_port = None
    try:
        with open(f"{SERVER_LOG_DIR}/known_servers.txt", "r") as f:
            for line in f:
                line = line.strip()
                if line.startswith(f"{server_name}:"):
                    _, port_str = line.split(":")
                    target_port = int(port_str)
                    break
    except FileNotFoundError:
        print(f"[Admin] ERROR: known_servers.txt not found.")
        return

    if target_port is None:
        print(f"[Admin] ERROR: Server {server_name} not found in known servers.")
        return



    message = Message(msg_type=MessageType.REMOVE_SERVER, payload={})
    remove_message = message.serialize()

    retries = 3
    timeout = 1000

    ctx = zmq.Context()
    sock = ctx.socket(zmq.DEALER)
    sock.connect(f"tcp://localhost:{target_port}")

    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    for attempt in range(1, retries + 1):
        print(f"[Admin] Sending REMOVE_SERVER to {target_port} (attempt {attempt}/{retries})")
        sock.send(remove_message)

        socks = dict(poller.poll(timeout))

        if sock in socks and socks[sock] == zmq.POLLIN:
            reply = sock.recv()
            if reply:
                reply_msg = Message(json_str=reply)
                if reply_msg.msg_type == MessageType.REMOVE_SERVER_ACK:
                    print(f"[Admin] Server {target_port} acknowledged removal.")
                    
                    # Update the topology file
                    with open(f"{SERVER_LOG_DIR}/known_servers.txt", "r") as f:
                        lines = f.readlines()
                    with open(f"{SERVER_LOG_DIR}/known_servers.txt", "w") as f:
                        for line in lines:
                            if not line.startswith(f"{server_name}:"):
                                f.write(line)
                    
                    sock.close()
                    ctx.term()
                    return
        else:
            print(f"[Admin] No reply from {target_port}, retrying...")
        
        timeout = min(timeout * 2, 8000)

    print(f"[Admin] Server {target_port} DID NOT RESPOND after {retries} attempts.")
    sock.close()
    ctx.term()


def main():
    parser = argparse.ArgumentParser(description="Shopping List Admin Tool")
    parser.add_argument("--action", required=True,
                        choices=["initial_setup", "add_server", "remove_server", "setup_db"])
    parser.add_argument("--server_name", type=str,
                        help="Name of the server to add/remove")
    parser.add_argument("--user_id", type=str,
                        help="User ID for DB setup")

    args = parser.parse_args()

    match args.action:
        case "initial_setup":
            initial_setup()

        case "add_server":
            add_server()

        case "remove_server":
            if not args.server_name:
                print("ERROR: --server_name required for remove_server")
                return
            remove_server(args.server_name)
            
        case "setup_db":
            if not args.user_id:
                print("ERROR: --user_id required for setup_db", file=sys.stderr)
                sys.exit(1)
            setup_db(args.user_id)


if __name__ == "__main__":
    main()