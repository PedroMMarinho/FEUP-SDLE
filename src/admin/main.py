import argparse
import subprocess
import os
import zmq
import json
import sys
from src.common.messages.messages import MessageType, Message


SERVER_BASE_PORT = 5555
PROXY_BASE_PORT = 6000
PROXY_INCREMENT = 2
SERVER_LOG_DIR = "src/server/server_logs"
PROXY_LOG_DIR = "src/proxy/proxy_logs"

# New PID file constants
SERVER_PIDS_FILE = os.path.join(SERVER_LOG_DIR, "server_pids.txt")
PROXY_PIDS_FILE = os.path.join(PROXY_LOG_DIR, "proxy_pids.txt")

def initial_setup():
    """
    Matches: make servers
    Starts: Server_1..5 and Proxy_1..2
    Stores PIDs in text files for clean shutdown.
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

    # --- 1. GENERATE CONFIGURATION FILES FIRST ---
    print("Generating network topology...")
    
    with open(f"{SERVER_LOG_DIR}/known_servers.txt", "w") as f:
        for i in range(1, 6):
            port = SERVER_BASE_PORT + i - 1
            f.write(f"Server_{i}:{port}\n")

    with open(f"{PROXY_LOG_DIR}/known_proxies.txt", "w") as f:
        for i in range(1, 3):
            port = PROXY_BASE_PORT + PROXY_INCREMENT*i - 1
            f.write(f"Proxy_{i}:{port}\n")

    # --- 2. START SERVERS AND TRACK PIDS ---
    print("Starting Servers...")
    with open(SERVER_PIDS_FILE, "w") as pid_f:
        for i in range(1, 6):
            port = SERVER_BASE_PORT + i - 1
            proc = subprocess.Popen([
                sys.executable, "-u", "-m", "src.server.main", 
                "--port", str(port),
                "--db", f"server_{i}",
                "--servers", f"{SERVER_LOG_DIR}/known_servers.txt"
            ], stdout=open(f"{SERVER_LOG_DIR}/server{i}.log", "w"), stderr=subprocess.STDOUT)
            
            # Write PID to file
            pid_f.write(f"{proc.pid}\n")
            
    print(f"Initial 5 servers started (PIDs saved to {SERVER_PIDS_FILE}).")

    # --- 3. START PROXIES AND TRACK PIDS ---
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
            
            # Write PID to file
            pid_w.write(f"{proc.pid}\n")

    print(f"Initial 2 proxies started (PIDs saved to {PROXY_PIDS_FILE}).")


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

    print(f"Starting {server_name} on port {next_port}")

    # Append to known_servers.txt
    with open(f"{SERVER_LOG_DIR}/known_servers.txt", "a") as f:
        f.write(f"{server_name}:{next_port}\n")

    proc = subprocess.Popen([
        sys.executable, "-m", "src.server.main",
        "--id", server_name,
        "--port", str(next_port),
        "--db", f"server_{next_id}",
        "--servers", f"{SERVER_LOG_DIR}/known_servers.txt",
    ], stdout=open(logfile, "w"), stderr=subprocess.STDOUT)

    # Append new PID to the PID file
    with open(SERVER_PIDS_FILE, "a") as pid_f:
        pid_f.write(f"{proc.pid}\n")

    print(f"{server_name} launched (PID: {proc.pid}).")


def remove_server(server_name):
    """
    Sends a REMOVE_SERVER message to the target server.
    NOTE: This shuts down the server gracefully. The PID will remain in the file
    until 'make clean' runs, but 'kill' handles stale PIDs gracefully.
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
    base_timeout = 1000
    timeout = base_timeout

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
                        choices=["initial_setup", "add_server", "remove_server"])
    parser.add_argument("--server_name", type=str,
                        help="Name of the server to add/remove")

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


if __name__ == "__main__":
    main()