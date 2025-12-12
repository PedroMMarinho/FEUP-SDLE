import pyzmq
from src.common.threadPool.threadPool import ThreadPool
from src.common.messages.messages import Message, MessageType
import hashlib
import random
import time

GOSSIP_FANOUT = 2
GOSSIP_INTERVAL = 0.5

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
        self.hash_ring_version = 1 

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

                time.sleep(GOSSIP_INTERVAL)
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
            "servers": current_server_ports,
            "hash_ring_version": self.hash_ring_version
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

    def handle_gossip(self, identity, payload):
        incoming_servers = payload.get("servers", [])
        incoming_proxies = payload.get("proxies", [])
        hash_ring_version = payload.get("hash_ring_version", 1)

        # Process Servers
        my_server_ports = {str(s.port) for s in self.servers}
        my_server_ports.add(str(self.port))

        if hash_ring_version < self.hash_ring_version:
            return # Ignore outdated gossip
        
        if hash_ring_version == self.hash_ring_version:
            self.hash_ring_version = hash_ring_version
            for p in incoming_servers:
                if str(p) not in my_server_ports:
                    print(f"[Gossip] Discovered new Server: {p}")
                    self.connect_to_server(int(p))

            # Process Proxies
            my_proxy_ports = {str(p.port) for p in self.proxies}

            for p in incoming_proxies:
                if str(p) not in my_proxy_ports:
                    print(f"[Gossip] Discovered new Proxy: {p}")
                    self.connect_to_proxy(int(p))
            hash_ring_version += 1
        if hash_ring_version > self.hash_ring_version:
            print(f"[Gossip] Detected newer hash ring version {hash_ring_version}, updating from {self.hash_ring_version}")
            self.hash_ring_version = hash_ring_version

            incoming_servers_set = {str(p) for p in incoming_servers}
            incoming_proxies_set = {str(p) for p in incoming_proxies}

            servers_to_remove = []
            for s in self.servers:
                if str(s.port) not in incoming_servers_set and s.port != self.port:
                    servers_to_remove.append(s)

            for s in servers_to_remove:
                print(f"[Gossip] Removing stale Server: {s.port}")
                self.remove_server(s.port)

            proxies_to_remove = []
            for p in self.proxies:
                if str(p.port) not in incoming_proxies_set:
                    proxies_to_remove.append(p)

            for p in proxies_to_remove:
                print(f"[Gossip] Removing stale Proxy: {p.port}")
                self.remove_proxy(p.port)


            for p in incoming_servers:
                if str(p) not in my_server_ports:
                    print(f"[Gossip] Discovered new Server: {p}")
                    self.connect_to_server(int(p))

            for p in incoming_proxies:
                if str(p) not in my_proxy_ports:
                    print(f"[Gossip] Discovered new Proxy: {p}")
                    self.connect_to_proxy(int(p))

        def remove_server(self, port):
        for s in list(self.servers):  # copy to avoid mutation issues
            if str(s.port) == str(port):
                try:
                    if hasattr(s, "socket") and s.socket is not None:
                        s.socket.close(linger=0)
                except Exception as e:
                    print(f"[Warning] Failed closing server socket {port}: {e}")

                self.servers.remove(s)
                print(f"[Gossip] Server {port} removed")
                return
            
    def remove_proxy(self, port):
        for p in list(self.proxies):
            if str(p.port) == str(port):
                try:
                    if hasattr(p, "socket") and p.socket is not None:
                        p.socket.close(linger=0)
                except Exception as e:
                    print(f"[Warning] Failed closing proxy socket {port}: {e}")

                self.proxies.remove(p)
                print(f"[Gossip] Proxy {port} removed")
                return



    def setup_proxy_interface_socket(self):
        self.proxy_interface_socket = self.context.socket(pyzmq.ROUTER)
        self.proxy_interface_socket.bind(f"tcp://localhost:{self.port}")
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
            case MessageType.GOSSIP:
                self.thread_pool.submit(self.handle_gossip, identity, message.payload)
            case _:
                print(f"[Network] Unknown message type: {message.msg_type}")



    
