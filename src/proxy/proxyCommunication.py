import zmq
from src.common.threadPool.threadPool import ThreadPool
from src.common.messages.messages import Message, MessageType
import hashlib
import random
import time
from src.common.crdt.improved.ShoppingList import ShoppingList

GOSSIP_FANOUT = 2
GOSSIP_INTERVAL = 0.5
NEXT_NUMBER = 5
SUCCESSFUL_READS = 2

class Server():
    def __init__(self, port, hash):
        self.hash = hash
        self.port = port

class Proxy():
    def __init__(self, port):
        self.port = port

class ProxyCommunicator:
    def __init__(self, port, known_server_ports, known_proxy_ports):
        self.port = port
        
        self.known_server_ports = known_server_ports
        self.known_proxy_ports = known_proxy_ports

        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.thread_pool = ThreadPool(8)
        self.running = True

        self.proxy_interface_socket = None  
        self.proxy_publish_socket = None
        self.proxies = []
        self.servers = []
        self.hash_ring_version = 1 

    def start(self):
        print(f"[ProxyCommunicator] Starting proxy on port {self.port}")
        self.setup_proxy_interface_sockets()

        self.setup_proxy()
        self.setup_server()

        self.thread_pool.submit(self.gossip_loop)

        self.loop()


    def setup_proxy(self):
        print("[Network] Setting up known proxies...")
        for port , _ in self.known_proxy_ports:
            self.connect_to_proxy(port)
    
    def setup_server(self):
        print("[Network] Setting up known servers...")
        for port , hash in self.known_server_ports:
            self.connect_to_server(port, hash)


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
        if self.hash_ring_version == 1: 
            message = Message(msg_type=MessageType.GOSSIP_INTRODUCTION, payload=payload)
        else:   
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
                socket = self.context.socket(zmq.DEALER)
                socket.connect(f"tcp://localhost:{node.port}")
                if socket:
                    socket.send(serialized_msg)
                    time.sleep(0.1)  # Good code :D
                    socket.close(linger=0)
            except Exception as e:
                print(f"[Gossip] Failed to send to {node.port}: {e}")

    def connect_to_server(self, port, hash_val=None):
        if any(str(s.port) == str(port) for s in self.servers):
            return
        
        if hash_val is None:
            hash_val = hashlib.sha256(f"server_{port}".encode()).hexdigest()

        server = Server(port, hash_val)
        self.servers.append(server)


    def connect_to_proxy(self, port):
        if any(str(p.port) == str(port) for p in self.proxies):
            return
        proxy = Proxy(port)
        self.proxies.append(proxy)



    def handle_gossip(self, identity, payload):
        incoming_servers = payload.get("servers", [])
        incoming_proxies = payload.get("proxies", [])
        hash_ring_version = payload.get("hash_ring_version", 1)

        # Process Servers
        my_server_ports = {str(s.port) for s in self.servers}
        my_proxy_ports = {str(p.port) for p in self.proxies}
        my_proxy_ports.add(str(self.port))

        #print(f"[Gossip] Handling gossip from {identity}: servers={incoming_servers}, proxies={incoming_proxies}, version={hash_ring_version}")
        if hash_ring_version < self.hash_ring_version:
            return # Ignore outdated gossip
        
        if hash_ring_version == self.hash_ring_version:
            if set(incoming_servers) == my_server_ports and set(incoming_proxies) == {str(p.port) for p in self.proxies}:
                return 
            
            for p in incoming_servers:
                if str(p) not in my_server_ports:
                    #print(f"[Gossip] Discovered new Server: {p}")
                    self.connect_to_server(p)


            for p in incoming_proxies:
                if str(p) not in my_proxy_ports:
                    #print(f"[Gossip] Discovered new Proxy: {p}")
                    self.connect_to_proxy(p)

            self.hash_ring_version += 1
        elif hash_ring_version > self.hash_ring_version:
            #print(f"[Gossip] Detected newer hash ring version {hash_ring_version}, updating from {self.hash_ring_version}")
            self.hash_ring_version = hash_ring_version

            incoming_servers_set = {str(p) for p in incoming_servers}
            incoming_proxies_set = {str(p) for p in incoming_proxies}

            servers_to_remove = []
            for s in self.servers:
                if str(s.port) not in incoming_servers_set and s.port != self.port:
                    servers_to_remove.append(s)

            for s in servers_to_remove:
                #print(f"[Gossip] Removing stale Server: {s.port}")
                self.remove_server(s.port)

            proxies_to_remove = []
            for p in self.proxies:
                if str(p.port) not in incoming_proxies_set:
                    proxies_to_remove.append(p)

            for p in proxies_to_remove:
                #print(f"[Gossip] Removing stale Proxy: {p.port}")
                self.remove_proxy(p.port)


            for p in incoming_servers:
                if str(p) not in my_server_ports:
                    #print(f"[Gossip] Discovered new Server: {p}")
                    self.connect_to_server(p)

            for p in incoming_proxies:
                if str(p) not in my_proxy_ports:
                    #print(f"[Gossip] Discovered new Proxy: {p}")
                    self.connect_to_proxy(p)

    def remove_server(self, port):
        for s in list(self.servers):  # copy to avoid mutation issues
            if str(s.port) == str(port):
                self.servers.remove(s)
                #print(f"[Gossip] Server {port} removed")
                return
            
    def remove_proxy(self, port):
        for p in list(self.proxies):
            if str(p.port) == str(port):
                self.proxies.remove(p)
                #print(f"[Gossip] Proxy {port} removed")
                return



    def setup_proxy_interface_sockets(self):
        self.proxy_interface_socket = self.context.socket(zmq.ROUTER)
        self.proxy_interface_socket.bind(f"tcp://localhost:{self.port}")
        self.poller.register(self.proxy_interface_socket, zmq.POLLIN)
        print(f"[ProxyCommunicator] Proxy interface socket bound to port {self.port}")

        self.proxy_publish_socket = self.context.socket(zmq.PUB)
        pub_port = int(self.port) + 1  # convention: PUB = DEALER + 1
        self.proxy_publish_socket.bind(f"tcp://localhost:{pub_port}")
        print(f"[ProxyCommunicator] Proxy publish socket bound to port {pub_port}")

    def loop(self):
        print("[ProxyCommunicator] Entering main server loop...")
        while self.running:
            socks = dict(self.poller.poll(1000))
            for sock in socks:
                if socks[sock] == zmq.POLLIN:
                    if sock == self.proxy_interface_socket:
                        print("[ProxyCommunicator] Incoming message on proxy interface socket")
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
            case MessageType.GOSSIP_INTRODUCTION:
                self.thread_pool.submit(self.handle_gossip_introduction, identity, message.payload)
            case MessageType.REQUEST_FULL_LIST:
                self.thread_pool.submit(self.handle_request_full_list, identity, message.payload)
            case MessageType.SENT_FULL_LIST:
                self.thread_pool.submit(self.handle_sent_full_list, identity, message.payload)
            case _:
                print(f"[Network] Unknown message type: {message.msg_type}")


    def _try_send_full_list_to_server(self, server, shopping_list_obj, retries=3, base_timeout=1000):
        message = Message(
            msg_type=MessageType.SENT_FULL_LIST,
            payload={
                "shopping_list": shopping_list_obj.to_json()
            }
        )


        # Connect or reuse socket
        sock = self.context.socket(zmq.DEALER)
        sock.connect(f"tcp://localhost:{server.port}")

        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)

        timeout = base_timeout

        for attempt in range(1, retries + 1):
            print(
                f"[Proxy] Sending FULL_LIST {shopping_list_obj.to_json()} → server {server.port} "
                f"(attempt {attempt}/{retries})"
            )

            sock.send(message.serialize())

            socks = dict(poller.poll(timeout))

            if sock in socks:
                reply = Message(json_str=sock.recv())

                if reply.msg_type == MessageType.SENT_FULL_LIST_ACK:
                    sock.close(linger=0)
                    print(f"[Proxy] ACK from server {server.port}")
                    return reply.payload

                print(f"[Proxy] Unexpected reply {reply.msg_type}")

            else:
                print(f"[Proxy] Timeout waiting for {server.port}")

            timeout = min(8000, timeout * 2)

        sock.close(linger=0)
        return None


    def handle_sent_full_list(self, identity, payload):
        shopping_list = payload.get("shopping_list")
        shopping_list_obj = ShoppingList.from_json(shopping_list)
        print(
            f"[Proxy] Received FULL_LIST {shopping_list_obj.uuid} from client {identity}"
        )

        if not self.servers:
            print("[Proxy] No servers available")
            return

        key_hash = hashlib.sha256(shopping_list_obj.uuid.encode()).hexdigest()

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

            result = self._try_send_full_list_to_server(
                server,
                shopping_list_obj,
            )

            if result:
                print(
                    f"[Proxy] FULL_LIST {shopping_list_obj.uuid} stored on server {server.port}"
                )

                message = Message(
                    msg_type=MessageType.SENT_FULL_LIST_ACK,
                    payload={"shopping_list": result['shopping_list']}
                )
                self.proxy_interface_socket.send_multipart(
                    [identity, message.serialize()]
                )

                self.proxy_publish_socket.send_multipart(
                    [shopping_list_obj.uuid.encode('utf-8'), Message(
                        msg_type=MessageType.LIST_UPDATE,
                        payload={
                            "shopping_list": result['shopping_list']
                        }
                    ).serialize()]
                )
                print(f"[Proxy] Sent LIST_UPDATE with UUID {shopping_list_obj.uuid}")
                return

            print(
                f"[Proxy] Server {server.port} failed, trying next"
            )

        print(
            f"[Proxy] FAILED: FULL_LIST {shopping_list_obj.uuid} "
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

        sock = self.context.socket(zmq.DEALER)
        sock.connect(f"tcp://localhost:{server.port}")


        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)

        for attempt in range(1, retries + 1):
            sock.send(message.serialize())
            socks = dict(poller.poll(timeout))

            if sock in socks:
                reply = Message(json_str=sock.recv())
                if reply.msg_type == MessageType.REQUEST_FULL_LIST_ACK:
                    sock.close(linger=0)
                    return reply.payload  # CRDT dict
                elif reply.msg_type == MessageType.REQUEST_FULL_LIST_NACK:
                    sock.close(linger=0)
                    return None

            timeout = min(8000, timeout * 2)
        sock.close(linger=0)
        return None


    def handle_request_full_list(self, identity, payload):
        list_id = payload.get("list_id")
        if not list_id:
            print("[Proxy] Missing list_id in request")
            return

        print(f"[Proxy] Client {identity} requested full list {list_id}")

        if not self.servers:
            print("[Proxy] No servers available")
            nack = Message(msg_type=MessageType.REQUEST_FULL_LIST_NACK, payload={})
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

            crdt_payload = crdt_payload.get("shopping_list") if crdt_payload else None
            if crdt_payload is not None:
                print(f"[Proxy] Received full list from server {server.port}: {crdt_payload}")
                collected_crdts.append(crdt_payload)
                successful += 1
            else:
                print(f"[Proxy] Server {server.port} has no list {list_id}")

        if successful < SUCCESSFUL_READS:
            # All servers failed → send NACK to client
            nack = Message(msg_type=MessageType.REQUEST_FULL_LIST_NACK, payload={})
            self.proxy_interface_socket.send_multipart([identity, nack.serialize()])
            print(f"[Proxy] Full list {list_id} not found on any server")
            return

        # Merge all CRDTs
        merged_list = ShoppingList.from_json(collected_crdts[0])
        print(f"[Proxy] Initial Merged list: {merged_list}")

        for crdt in collected_crdts[1:]:
            print(f"[Proxy] Merging with CRDT: {crdt}")
            # Assuming ShoppingList.merge_dict exists or implement merge here
            merged_list.merge(ShoppingList.from_json(crdt))
            print(f"[Proxy] Merged list: {merged_list}")

        merged_list = merged_list.to_json()

        # Send ACK with merged list to client
        ack = Message(
            msg_type=MessageType.REQUEST_FULL_LIST_ACK,
            payload={"shopping_list": merged_list}
        )
        self.proxy_interface_socket.send_multipart([identity, ack.serialize()])
        print(f"[Proxy] Sent merged full list {list_id} to client {identity}")

    def handle_gossip_introduction(self, identity, payload):
        incoming_servers = payload.get("servers", [])
        incoming_proxies = payload.get("proxies", [])
        hash_ring_version = payload.get("hash_ring_version", 1)

        # Process Servers
        my_server_ports = {str(s.port) for s in self.servers}
        my_proxy_ports = {str(p.port) for p in self.proxies}
        my_proxy_ports.add(str(self.port))

        # always merge introductions
        ##print(f"[Gossip] Handling gossip introduction from {identity}: servers={incoming_servers},proxies={incoming_proxies}, version={hash_ring_version}")
        changed = False
        for p in incoming_servers:
            if str(p) not in my_server_ports:
               # #print(f"[Gossip] Discovered new Server: {p}")
                self.connect_to_server(p)
                changed = True
        for p in incoming_proxies:
            if str(p) not in my_proxy_ports:
                ##print(f"[Gossip] Discovered new Proxy: {p}")
                self.connect_to_proxy(p)
                changed = True

        if changed:
            self.hash_ring_version = max(self.hash_ring_version, hash_ring_version) + 1

        
        


        







        






    
