import hashlib
import threading
import pyzmq
from src.server.messages import Message, MessageType

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
        self.hashes = {}  # Mapping of server port to hash
        self.context = pyzmq.Context()
        self.poller = pyzmq.Poller()
        

    def start(self):
        

        print(f"[System] Starting server on port {self.port}...")
        if self.seed:
            print("[System] Seeding as the first server.")
            self.setupSeedSocket()
            self.loop()

        else:
            print(f"[System] Connecting to known server on port {self.known_server_port}...")
            self.connect_to_known_server()
            self.loop()

    def setupSeedSocket(self):
        self.seedSocket = self.context.socket(pyzmq.ROUTER)
        self.seedSocket.bind(f"tcp://localhost:{self.port}")
        self.poller.register(self.seedSocket, pyzmq.POLLIN)

        

    def connect_to_known_server(self):
        print(f"[Network] Connecting to server on port {self.known_server_port}...")
        self.server_list.append(self.known_server_port)
        self.seedSocket = self.context.socket(pyzmq.REQ)
        self.seedSocket.connect(f"tcp://localhost:{self.known_server_port}")
        message = Message(MessageType.SERVER_INTRODUCTION, {"port": self.port, "hash": self.hash})
        self.seedSocket.send(message.serialize())
        try:
            response = Message(self.seedSocket.recv())
            print(f"[Network] Received response from server: {response}")
            self.hashes = response.payload.get("hashes", {})
        except pyzmq.Again:
            pass

    def loop(self):
        print("[System] Entering main server loop...")
        while self.running:
            socks = dict(self.poller.poll(1000))
            if self.seedSocket in socks and socks[self.seedSocket] == pyzmq.POLLIN:
                self.handle_seed_socket()

        self.context.destroy()
        print("[System] Server stopped.")

    def handle_seed_socket(self):
        if self.seed:
            identity, msg_bytes = self.seedSocket.recv_multipart()
            message = Message(msg_bytes)
            print(f"[Network] Received message from {identity}: {message.msg_type}, {message.payload}")
            if message.msg_type == MessageType.SERVER_INTRODUCTION:
                new_port = message.payload["port"]
                new_hash = message.payload["hash"]
                self.hashes[new_port] = new_hash
                ack_message = Message(MessageType.SERVER_INTRODUCTION_ACK, {"hashes": self.hashes})
                self.seedSocket.send_multipart([identity, ack_message.serialize()])
        else:
            msg_bytes = self.seedSocket.recv()
            message = Message(msg_bytes)
            print(f"[Network] Received message: {message.msg_type}, {message.payload}")

