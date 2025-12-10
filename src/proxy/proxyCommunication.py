import pyzmq
from src.common.threadPool.threadPool import ThreadPool
from src.common.messages.messages import Message, MessageType
import hashlib

class Server():
    def __init__(self, port, hash):
        self.hash = hash
        self.port = port
        self.socket = None
        self.unreachable = False
        self.actionQueue = []

    def setSocket(self, socket):
        self.socket = socket

class Proxy():
    def __init__(self, port):
        self.port = port
        self.socket = None
        self.unreachable = False
        self.actionQueue = []

    def setSocket(self, socket):
        self.socket = socket

class ProxyCommunicator:
    def __init__(self, port, is_seed, known_server_port, known_proxy_port):
        self.port = port
        self.is_seed = is_seed
        self.known_server_port = known_server_port
        self.known_proxy_port = known_proxy_port
        self.context = pyzmq.Context()
        self.thread_pool = ThreadPool(num_threads=4)
        self.proxy_interface_socket = None  
        self.proxies = []
        self.servers = []

    def start(self):
        print(f"[ProxyCommunicator] Starting proxy on port {self.port}")
        self.setup_proxy_interface_socket()
        if self.is_seed:
            print("[ProxyCommunicator] Running as seed proxy. Trying to connect to known server...")
            self.connect_to_known_server()

        else:
            print(f"[ProxyCommunicator] Connecting to known proxy on port {self.known_proxy_port}.")
            self.connect_to_known_proxy()
        self.loop()

    def setup_proxy_interface_socket(self):
        self.proxy_interface_socket = self.context.socket(pyzmq.ROUTER)
        self.proxy_interface_socket.bind(f"tcp://*:{self.port}")
        print(f"[ProxyCommunicator] Proxy interface socket bound to port {self.port}")

    def loop(self):
        print("[ProxyCommunicator] Entering main server loop...")
        while self.running:
            socks = dict(self.poller.poll(1000))
            for sock in socks:
                if socks[sock] == pyzmq.POLLIN: # idk
                    if sock == self.proxy_interface_socket: # idk
                        self.handle_proxy_interface_socket()

        self.context.destroy()
        self.thread_pool.shutdown()
        print("[ProxyCommunicator] Server stopped.")

    def handle_proxy_interface_socket(self):
        identity, msg_bytes = self.proxy_interface_socket.recv_multipart()
        message = Message(json_str=msg_bytes)
        print(f"[Network] Received message from {identity}: {message.msg_type}, {message.payload}")
        match message.msg_type:
            case _:
                print(f"[Network] Unknown message type: {message.msg_type}")

    def handle_heartbeat(self, identity, payload):
        print(f"[Network] Handling HEARTBEAT from {identity}: {payload}")
        ack_message = Message(msg_type=MessageType.HEARTBEAT_ACK, payload={})
        self.proxy_interface_socket.send_multipart([identity, ack_message.serialize()])
        print(f"[Network] Sent HEARTBEAT_ACK to {identity}")
    
    def handle_proxy_introduction(self, identity, payload):
        print(f"[Network] Handling PROXY_INTRODUCTION from {identity}: {payload}")

        new_proxy = Proxy(payload["port"])
        self.proxies.append(new_proxy)
        new_socket = self.context.socket(pyzmq.DEALER)
        new_socket.connect(f"tcp://localhost:{payload['port']}")
        new_proxy.setSocket(new_socket)

        ack_message = Message(msg_type=MessageType.PROXY_INTRODUCTION_ACK, payload={})
        self.proxy_interface_socket.send_multipart([identity, ack_message.serialize()])
        print(f"[Network] Sent PROXY_INTRODUCTION_ACK to {identity}")

    def handle_hashring_update(self, identity, payload):
        print(f"[Network] Handling HASHRING_UPDATE from {identity}: {payload}")
        new_ports = payload["ports"]
        new_hashes = payload["hashes"]

        for port, hash in zip(new_ports, new_hashes):
            if port not in [s.port for s in self.servers]:
                new_server = Server(port, hash)
                self.servers.append(new_server)
                new_socket = self.context.socket(pyzmq.DEALER)
                new_socket.connect(f"tcp://localhost:{port}")
                new_server.setSocket(new_socket)

        for port in [s.port for s in self.servers]: # very inefficient but whatever
            if port not in new_ports:
                self.servers = [s for s in self.servers if s.port != port]

        message = Message(msg_type=MessageType.HASHRING_UPDATE_ACK, payload={})
        self.proxy_interface_socket.send_multipart([identity, message.serialize()])
        
        print(f"[Network] Updated hashring with ports: {new_ports} and hashes: {new_hashes}")

    
