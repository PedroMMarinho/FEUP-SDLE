import hashlib
import threading
import pyzmq
from src.server.messages import Message, MessageType


class Server():
    def __init__(self, port, hash):
        self.hash = hash
        self.port = port
        self.socket = None
        self.unreachable = False

    def setSocket(self, socket):
        self.socket = socket

class ServerCommunicator(threading.Thread):
    def __init__(self, db_config, port, seed, known_server_port):
        threading.Thread.__init__(self)
        self.db_config = db_config
        self.port = port
        self.hash = hashlib.sha256(f"server_{port}".encode()).hexdigest()
        self.seed = seed
        self.known_server_port = known_server_port
        self.running = True
        self.daemon = True 
        self.context = pyzmq.Context()
        self.poller = pyzmq.Poller()
        self.server_interface_socket = None
        self.servers = []


    def start(self):
        

        print(f"[System] Starting server on port {self.port}...")
        if self.seed:
            print("[System] Seeding as the first server.")
            self.setup_server_interface_socket()
            self.loop()

        else:
            print(f"[System] Connecting to known server on port {self.known_server_port}...")


            if(self.connect_to_known_server()): 
                print("[System] Connected to known server successfully.")
            else:
                print("[System] Failed to connect to known server. Exiting.")
                return
            self.setup_server_interface_socket()
            self.notify_other_servers()
            self.loop()

    def setup_server_interface_socket(self):
        self.server_interface_socket = self.context.socket(pyzmq.ROUTER)
        self.server_interface_socket.bind(f"tcp://localhost:{self.port}")
        self.poller.register(self.server_interface_socket, pyzmq.POLLIN)

    def connect_to_known_server(self):
        print(f"[Network] Connecting to server on port {self.known_server_port}...")
        self.server_list.append(self.known_server_port)

        seedSocket = self.context.socket(pyzmq.DEALER)
        seedSocket.connect(f"tcp://localhost:{self.known_server_port}")

        seedPoller = pyzmq.Poller()
        seedPoller.register(seedSocket, pyzmq.POLLIN)

        message = Message(
            MessageType.SERVER_INTRODUCTION,
            {"port": self.port, "hash": self.hash}
        )

        retries = 5
        timeout = 2000  

        for attempt in range(1, retries + 1):
            print(f"[Network] Sending SERVER_INTRODUCTION attempt {attempt}/{retries}")
            seedSocket.send(message.serialize())

            socks = dict(seedPoller.poll(timeout))

            if seedSocket in socks and socks[seedSocket] == pyzmq.POLLIN:
                # SUCCESS
                msg_bytes = seedSocket.recv()
                response = Message(msg_bytes)

                if response.msg_type == MessageType.SERVER_INTRODUCTION_ACK:
                    hashes = response.payload["hashes"]
                    ports = response.payload["ports"]
                    for port, hash in zip(ports, hashes):
                        server = Server(port, hash)
                        self.servers.append(server)
                    print(f"[Network] Updated server hashes: {hashes} and ports: {ports}")
                    return True

                else:
                    print(f"[Network] Unexpected response type: {response.msg_type}")
                    return False

            # TIMEOUT → retry
            print(f"[Network] No response, retrying in {timeout/1000:.1f}s...")

            # Exponential backoff (optional)
            timeout = min(timeout * 2, 8000)

        print("[Network] Failed to connect to seed server after retries.")
        return False


    def notify_other_servers(self):
        print("[Network] Notifying other servers...")

        notify_message = Message(
            MessageType.SERVER_INTRODUCTION,
            {"port": self.port, "hash": self.hash}
        )

        retries = 3
        base_timeout = 1000  # 1 second

        for server in self.servers:
            if server.port == self.port or server.port == self.known_server_port:
                continue  # skip myself or seed server (already connected)

            # Create or reuse socket
            if server.socket is None:
                server_socket = self.context.socket(pyzmq.DEALER)
                server_socket.connect(f"tcp://localhost:{server.port}")
                server.setSocket(server_socket)
            else:
                server_socket = server.socket

            poller = pyzmq.Poller()
            poller.register(server_socket, pyzmq.POLLIN)

            timeout = base_timeout

            for attempt in range(1, retries + 1):
                print(f"[Notify] Sending intro to server {server.port} (attempt {attempt}/{retries})")

                server_socket.send(notify_message.serialize())

                socks = dict(poller.poll(timeout))

                if server_socket in socks and socks[server_socket] == pyzmq.POLLIN:
                    # SUCCESS
                    reply_bytes = server_socket.recv()
                    reply = Message(reply_bytes)

                    print(f"[Notify] Server {server.port} ACK: {reply.msg_type}")

                    server.unreachable = False
                    break  # stop retrying this server

                else:
                    print(f"[Notify] No reply from {server.port}, retrying...")
                    timeout = min(timeout * 2, 8000)

            else:
                # FAILED AFTER RETRIES
                server.unreachable = True
                print(f"[Notify] Server {server.port} marked UNREACHABLE.")


        


    def loop(self):
        print("[System] Entering main server loop...")
        while self.running:
            socks = dict(self.poller.poll(1000))
            for sock in socks:
                if socks[sock] == pyzmq.POLLIN: # idk
                    if sock == self.server_interface_socket: # idk
                        self.handle_server_interface_socket()

        self.context.destroy()
        print("[System] Server stopped.")


    def handle_server_interface_socket(self):
        identity, msg_bytes = self.server_interface_socket.recv_multipart()
        message = Message(msg_bytes)
        print(f"[Network] Received message from {identity}: {message.msg_type}, {message.payload}")
        match message.msg_type:
            case MessageType.SERVER_INTRODUCTION:
                self.handle_server_introduction(identity, message.payload)
            case _:
                print(f"[Network] Unknown message type: {message.msg_type}")

    def handle_server_introduction(self, identity, payload):
        print(f"[Network] Handling SERVER_INTRODUCTION from {identity}: {payload}")

        identity, msg_bytes = self.seedSocket.recv_multipart()
        message = Message(msg_bytes)
        print(f"[Network] Received message from {identity}: {message.msg_type}, {message.payload}")
        new_port = message.payload["port"]
        new_hash = message.payload["hash"]

        new_server = Server(new_port, new_hash)
        self.servers.append(new_server)
        new_socket = self.context.socket(pyzmq.DEALER)
        new_socket.connect(f"tcp://localhost:{new_port}")
        new_server.setSocket(new_socket)

        if self.seed:
            ack_message = Message(MessageType.SERVER_INTRODUCTION_ACK, {"ports": [s.port for s in self.servers], "hashes": [s.hash for s in self.servers]})
            self.seedSocket.send_multipart([identity, ack_message.serialize()])
            print(f"[Network] Sent SERVER_INTRODUCTION_ACK to new server on port {new_port}")
        else:
            ack_message = Message(MessageType.SERVER_INTRODUCTION_ACK, {})
            new_socket.send(ack_message.serialize())
            print(f"[Network] Sent SERVER_INTRODUCTION_ACK to new server on port {new_port}")
        print(f"[Network] Added new server: port {new_port}, hash {new_hash}")

        

