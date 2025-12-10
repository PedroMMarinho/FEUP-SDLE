import argparse
import subprocess
import os
import zmq
import json
from src.common.messages.messages import MessageType, Message


SERVER_BASE_PORT = 5555
PROXY_BASE_PORT = 6000
SERVER_LOG_DIR = "src/server/server_logs"
PROXY_LOG_DIR = "src/proxy/proxy_logs"


def initial_setup():
    """
    Matches: make servers
    Starts: Server_1, Server_2, Server_3, Server_4, Server_5 + Proxy_1, Proxy_2
    """
    os.makedirs(SERVER_LOG_DIR, exist_ok=True)
    os.makedirs(PROXY_LOG_DIR, exist_ok=True)

    with open(f"{SERVER_LOG_DIR}/known_servers.txt", "w") as f:
        for i in range(1, 6):
            subprocess.Popen([
                "python3", "-m", "src.server.main",
                "--id", f"Server_{i}",
                "--port", str(SERVER_BASE_PORT + i - 1),
                "--db", f"server_{i}",
                "--servers", "known_servers.txt"
            ], stdout=open(f"{SERVER_LOG_DIR}/server{i}.log", "w"), stderr=subprocess.STDOUT)
            f.write(f"Server_{i}:{SERVER_BASE_PORT + i - 1}\n")



    print("Initial 5 servers started (matching `make servers`).")

   # TODO add proxies later 


def add_server():
    """
    Matches: make additional_server
    Starts a new server with the next available index and port.
    Always points to the seed server at port 5555.
    """
    os.makedirs(SERVER_LOG_DIR, exist_ok=True)

    existing = sorted([
        f for f in os.listdir(SERVER_LOG_DIR)
        if f.startswith("server") and f.endswith(".log")
    ])

    next_id = len(existing) + 1
    server_name = f"Server_{next_id}"
    next_port = SERVER_BASE_PORT + next_id

    logfile = f"{SERVER_LOG_DIR}/server{next_id}.log"

    print(f"Starting {server_name} on port {next_port}")

    subprocess.Popen([
        "python3", "-m", "src.server.main",
        "--id", server_name,
        "--seed", "False",
        "--port", str(next_port),
        "--db", f"server_{next_id}",
        "--known_server_port", str(SERVER_BASE_PORT)
    ], stdout=open(logfile, "w"), stderr=subprocess.STDOUT)

    # store known servers in a file for later use
    known_servers_file = f"{SERVER_LOG_DIR}/known_servers.txt"
    with open(known_servers_file, "a") as f:
        f.write(f"{server_name}:{next_port}\n")

    print(f"{server_name} launched (matching `make additional_server`).")


def remove_server(server_name):
    """
    Sends a REMOVE_SERVER message to the target server using the same logic
    as notify_server():
    - 3 retries
    - exponential backoff (1s → 2s → 4s → max 8s)
    """

    # Determine target server port from known servers file
    target_port = None
    with open(f"{SERVER_LOG_DIR}/known_servers.txt", "r") as f:
        for line in f:
            line = line.strip()
            if line.startswith(f"{server_name}:"):
                _, port_str = line.split(":")
                target_port = int(port_str)
                break

    if target_port is None:
        print(f"[Admin] ERROR: Server {server_name} not found in known servers.")
        return

    message = Message(msg_type=MessageType.REMOVE_SERVER, payload={})
    remove_message = message.serialize()

    retries = 3
    base_timeout = 1000  # 1 second
    timeout = base_timeout

    ctx = zmq.Context()
    sock = ctx.socket(zmq.DEALER)
    sock.connect(f"tcp://localhost:{target_port}")

    poller = zmq.Poller()
    poller.register(sock, zmq.POLLIN)

    for attempt in range(1, retries + 1):
        print(f"[Admin] Sending REMOVE_SERVER to {target_port} "
              f"(attempt {attempt}/{retries})")

        # Send the message
        sock.send(remove_message)

        # Wait for a reply or timeout
        socks = dict(poller.poll(timeout))

        if sock in socks and socks[sock] == zmq.POLLIN :
            # SUCCESS
            reply = sock.recv()
            if reply:
                reply_msg = Message(json_str=reply)
                if reply_msg.msg_type == MessageType.REMOVE_SERVER_ACK:
                    print(f"[Admin] Server {target_port} acknowledged removal.")
                    with open(f"{SERVER_LOG_DIR}/known_servers.txt", "r") as f:
                        lines = f.readlines()
                    lines = [line for line in lines if not line.startswith(f"{server_name}:")]
                    with open(f"{SERVER_LOG_DIR}/known_servers.txt", "w") as f:
                        f.writelines(lines)
                    return
                else:
                    print(f"[Admin] Server {target_port} sent unexpected reply: {reply_msg.msg_type}")
        else:
            print(f"[Admin] No reply from {target_port}, retrying...")
        timeout = min(timeout * 2, 8000)  # exponential backoff

    # FAILED AFTER ALL RETRIES
    print(f"[Admin] Server {target_port} DID NOT RESPOND after {retries} attempts.")



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
