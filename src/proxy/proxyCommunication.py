import zmq
from src.common.threadPool.threadPool import ThreadPool
from src.common.messages.messages import Message, MessageType
import hashlib
import random
import time
import src.common.crdt.improved.ShoppingList as ShoppingList

GOSSIP_FANOUT = 2
GOSSIP_INTERVAL = 0.5
NEXT_NUMBER = 5
SUCCESSFUL_READS = 2

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

        self.context = zmq.Context()
        self.poller = zmq.Poller()
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
        self.proxy_interface_socket = self.context.socket(zmq.ROUTER)
        self.proxy_interface_socket.bind(f"tcp://localhost:{self.port}")
        print(f"[ProxyCommunicator] Proxy interface socket bound to port {self.port}")

    def loop(self):
        print("[ProxyCommunicator] Entering main server loop...")
        while self.running:
            socks = dict(self.poller.poll(1000))
            for sock in socks:
                if socks[sock] == zmq.POLLIN: # idk
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
            case MessageType.REQUEST_FULL_LIST:
                self.thread_pool.submit(self.handle_request_full_list, identity, message.payload)
            case MessageType.SENT_FULL_LIST:
                self.thread_pool.submit(self.handle_sent_full_list, identity, message.payload)
            case _:
                print(f"[Network] Unknown message type: {message.msg_type}")


    def _try_send_full_list_to_server(self, server, list_id, shopping_list, retries=3, base_timeout=1000):
        message = Message(
            msg_type=MessageType.SENT_FULL_LIST,
            payload={
                "list_id": list_id,
                "shopping_list": shopping_list
            }
        )


        # Connect or reuse socket
        if server.socket is None:
            sock = self.context.socket(zmq.DEALER)
            sock.connect(f"tcp://localhost:{server.port}")
            server.setSocket(sock)
        else:
            sock = server.socket

        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)

        timeout = base_timeout

        for attempt in range(1, retries + 1):
            print(
                f"[Proxy] Sending FULL_LIST {list_id} → server {server.port} "
                f"(attempt {attempt}/{retries})"
            )

            sock.send(message.serialize())

            socks = dict(poller.poll(timeout))

            if sock in socks:
                reply = Message(json_str=sock.recv())

                if reply.msg_type == MessageType.SENT_FULL_LIST_ACK:
                    print(f"[Proxy] ACK from server {server.port}")
                    return True

                print(f"[Proxy] Unexpected reply {reply.msg_type}")

            else:
                print(f"[Proxy] Timeout waiting for {server.port}")

            timeout = min(8000, timeout * 2)

        return False


    def handle_sent_full_list(self, identity, payload):
        list_id = payload.get("list_id")
        shopping_list = payload.get("shopping_list")

        print(
            f"[Proxy] Received FULL_LIST {list_id} from client {identity}"
        )

        if not self.servers:
            print("[Proxy] No servers available")
            return

        key_hash = hashlib.sha256(list_id.encode()).hexdigest()

        # Sort servers by hash ONLY for this request
        servers = sorted(self.servers, key=lambda s: s.hash)

        # Find first server clockwise on the ring
        start_index = None
        for i, server in enumerate(servers):
            if key_hash <= server.hash:
                start_index = i
                break

        if start_index is None:
            start_index = 0  # wrap around

        num_servers = len(servers)

        # Walk the ring until success or full loop
        for offset in range(num_servers):
            server = servers[(start_index + offset) % num_servers]

            success = self._try_send_full_list_to_server(
                server,
                list_id,
                shopping_list
            )

            if success:
                print(
                    f"[Proxy] FULL_LIST {list_id} stored on server {server.port}"
                )

                message = Message(
                    msg_type=MessageType.SENT_FULL_LIST_ACK,
                    payload={}
                )
                self.proxy_interface_socket.send_multipart(
                    [identity, message.serialize()]
                )
                return

            print(
                f"[Proxy] Server {server.port} failed, trying next"
            )

        print(
            f"[Proxy] FAILED: FULL_LIST {list_id} "
            f"after full ring traversal"
        )
        # All servers failed
        nack_message = Message(
            msg_type=MessageType.SENT_FULL_LIST_NACK,
            payload={}
        )
        self.proxy_interface_socket.send_multipart(
            [identity, nack_message.serialize()]
        )

    def _try_request_full_list_from_server(self, server, list_id, retries=3, timeout=1000):
        message = Message(
            msg_type=MessageType.REQUEST_FULL_LIST,
            payload={"list_id": list_id}
        )

        if server.socket is None:
            sock = self.context.socket(zmq.DEALER)
            sock.connect(f"tcp://localhost:{server.port}")
            server.setSocket(sock)
        else:
            sock = server.socket

        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)

        for attempt in range(1, retries + 1):
            sock.send(message.serialize())
            socks = dict(poller.poll(timeout))

            if sock in socks:
                reply = Message(json_str=sock.recv())
                if reply.msg_type == MessageType.REQUEST_FULL_LIST_ACK:
                    return reply.payload  # CRDT dict
                elif reply.msg_type == MessageType.REQUEST_FULL_LIST_NACK:
                    return None

            timeout = min(8000, timeout * 2)

        return None


    def handle_request_full_list(self, identity, payload):
        list_id = payload.get("list_id")
        if not list_id:
            print("[Proxy] Missing list_id in request")
            return

        print(f"[Proxy] Client {identity} requested full list {list_id}")

        if not self.servers:
            print("[Proxy] No servers available")
            nack = Message(msg_type=MessageType.REQUEST_FULL_LIST_NACK, payload={"list_id": list_id})
            self.proxy_interface_socket.send_multipart([identity, nack.serialize()])
            return

        # Calculate hash for primary server position
        key_hash = hashlib.sha256(list_id.encode()).hexdigest()
        servers = sorted(self.servers, key=lambda s: s.hash)
        start_index = next((i for i, s in enumerate(servers) if key_hash <= s.hash), 0)
        num_servers = len(servers)

        collected_crdts = []
        servers_tried = 0
        successful = 0

        # Try up to NEXT_NUMBER servers
        for offset in range(num_servers):
            if servers_tried >= NEXT_NUMBER:
                break
            if successful >= SUCCESSFUL_READS:
                break

            server = servers[(start_index + offset) % num_servers]
            servers_tried += 1

            crdt_payload = self._try_request_full_list_from_server(server, list_id)
            if crdt_payload is not None:
                collected_crdts.append(crdt_payload)
                successful += 1
            else:
                print(f"[Proxy] Server {server.port} has no list {list_id}")

        if successful < SUCCESSFUL_READS:
            # All servers failed → send NACK to client
            nack = Message(msg_type=MessageType.REQUEST_FULL_LIST_NACK, payload={"list_id": list_id})
            self.proxy_interface_socket.send_multipart([identity, nack.serialize()])
            print(f"[Proxy] Full list {list_id} not found on any server")
            return

        # Merge all CRDTs
        merged_list = ShoppingList.from_dict(collected_crdts[0])
        for crdt in collected_crdts[1:]:
            # Assuming ShoppingList.merge_dict exists or implement merge here
            merged_list = ShoppingList.merge(merged_list, ShoppingList.from_dict(crdt))

        merged_list = merged_list.to_dict()

        # Send ACK with merged list to client
        ack = Message(
            msg_type=MessageType.REQUEST_FULL_LIST_ACK,
            payload={"list_id": list_id, "shopping_list": merged_list}
        )
        self.proxy_interface_socket.send_multipart([identity, ack.serialize()])
        print(f"[Proxy] Sent merged full list {list_id} to client {identity}")







        






    
