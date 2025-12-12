import pyzmq
from src.common.threadPool.threadPool import ThreadPool
from src.common.messages.messages import Message, MessageType
import hashlib
import random
import time

GOSSIP_FANOUT = 2

class Server():
    def __init__(self, port, hash):
        self.hash = hash
        self.port = port
        self.socket = None

    def setSocket(self, socket):
        self.socket = socket

class Proxy():
    def __init__(self, port):
        self.port = port
        self.socket = None

    def setSocket(self, socket):
        self.socket = socket

class ProxyCommunicator:
    def __init__(self, port, known_server_ports, known_proxy_ports):
        self.port = port
        
        self.known_server_ports = known_server_ports
        self.known_proxy_ports = known_proxy_ports

        self.context = pyzmq.Context()
        self.poller = pyzmq.Poller()
        self.thread_pool = ThreadPool(num_threads=4)
        self.running = True

        self.proxy_interface_socket = None  
        self.proxies = []
        self.servers = []

    def start(self):
        print(f"[ProxyCommunicator] Starting proxy on port {self.port}")
        self.setup_proxy_interface_socket()

        self.thread_pool.submit(self.gossip_loop)

        self.loop()

    def gossip_loop(self):
        print("[Gossip] Loop started.")
        while self.running:
            try:
                # (Peer Discovery) + (Registration)
                self.gossip()
                                
                time.sleep(5) 
            except Exception as e:
                print(f"[Gossip] Error in loop: {e}")

    def gossip(self):
        
        current_proxy_ports = [str(p.port) for p in self.proxies]
        current_proxy_ports.append(str(self.port))
        current_proxy_ports = list(set(current_proxy_ports))

        current_server_ports = [str(s.port) for s in self.servers]
        current_server_ports = list(set(current_server_ports))

        payload = {
            "proxies": current_proxy_ports,
            "servers": current_server_ports
        }
        
        message = Message(msg_type=MessageType.GOSSIP, payload=payload)
        serialized_msg = message.serialize()

        targets = []
        
        if self.proxies:
            targets.extend(random.sample(self.proxies, min(len(self.proxies), GOSSIP_FANOUT)))
        if self.servers:
            targets.extend(random.sample(self.servers, min(len(self.servers), GOSSIP_FANOUT)))
        
        if not targets:
            return

        for node in targets:
            try:
                if node.socket:
                    node.socket.send(serialized_msg)
            except Exception as e:
                print(f"[Gossip] Failed to send to {node.port}: {e}")

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

    
