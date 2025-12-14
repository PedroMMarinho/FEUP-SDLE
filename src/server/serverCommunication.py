import hashlib
import zmq
from src.common.crdt.improved.ShoppingList import ShoppingList
from src.common.messages.messages import Message, MessageType
from src.common.threadPool.threadPool import ThreadPool
import time 
import random 
import json

GOSSIP_FANOUT = 2
REPLICA_COUNT = 2
GOSSIP_INTERVAL = 0.5

class Server():
    def __init__(self, port, hash):
        self.hash = hash
        self.port = port


class Proxy():
    def __init__(self, port):
        self.port = port



class ServerCommunicator:
    def __init__(self, storage, port, known_servers, known_proxies):
        self.storage = storage
        self.port = port
        self.hash = hashlib.sha256(f"server_{port}".encode()).hexdigest()
        self.known_servers = known_servers
        self.known_proxies = known_proxies
        self.running = True
        self.daemon = True 
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.server_interface_socket = None
        self.servers = []
        self.proxies = []
        self.thread_pool = ThreadPool(8)
        self.disconnected = False
        self.hash_ring_version = 1


    def start(self): 
        
        self.setup_server_interface_socket()
        print(f"[System] Starting server on port {self.port}...")
        self.setup_servers()
        self.setup_proxies()
        self.thread_pool.submit(self.gossip_loop)
        self.thread_pool.submit(self.heartbeat)
        self.loop()

    def setup_server_interface_socket(self):
        self.server_interface_socket = self.context.socket(zmq.ROUTER)
        self.server_interface_socket.bind(f"tcp://localhost:{self.port}")
        self.poller.register(self.server_interface_socket, zmq.POLLIN)

    def setup_proxies(self):
        print("[Network] Setting up known proxies...")
        for port, _ in self.known_proxies:
            proxy = Proxy(port)
            self.proxies.append(proxy)


    def setup_servers(self):
        print("[Network] Setting up known servers...")
        for port, hash in self.known_servers:
            server = Server(port, hash)
            self.servers.append(server)


    def gossip_loop(self):
        #print("[Gossip] Starting gossip loop...")
        while self.running:
            try:
                self.gossip()
                time.sleep(GOSSIP_INTERVAL)
            except Exception as e:
                print(f"[Gossip] Error in loop: {e}")

    def gossip(self):   
        server_ports = [str(s.port) for s in self.servers]
        server_ports.append(str(self.port))
        
        proxy_ports = [str(p.port) for p in self.proxies]

        payload = {
            "servers": list(set(server_ports)),
            "proxies": list(set(proxy_ports)),
            "hash_ring_version": self.hash_ring_version
        }

        if self.hash_ring_version == 1:
            self.message = Message(MessageType.GOSSIP_INTRODUCTION, payload)
        else:
            self.message = Message(MessageType.GOSSIP, payload)
        serialized_msg = self.message.serialize()

        targets = []
        if self.servers:
            targets.extend(random.sample(self.servers, min(len(self.servers), GOSSIP_FANOUT)))
        if self.proxies:
            targets.extend(random.sample(self.proxies, min(len(self.proxies), GOSSIP_FANOUT)))

        #print(f"[Gossip] Sending GOSSIP to  {[t.port for t in targets]}")
        for node in targets:
            try:
                sock = self.context.socket(zmq.DEALER)
                sock.connect(f"tcp://localhost:{node.port}")
                if sock:
                    sock.send(serialized_msg)
                    sock.close(linger=0)
            except Exception as e:
                print(f"[Gossip] Failed to send to {node.port}: {e}")


    def loop(self):
        print("[System] Entering main server loop...")
        while self.running:
            socks = dict(self.poller.poll(1000))
            for sock in socks:
                if socks[sock] == zmq.POLLIN: # idk
                    if sock == self.server_interface_socket: # idk
                        self.handle_server_interface_socket()

        self.context.destroy()
        self.thread_pool.shutdown()
        print("[System] Server stopped.")


    def handle_server_interface_socket(self):
        identity, msg_bytes = self.server_interface_socket.recv_multipart()
        message = Message(json_str=msg_bytes)
        print(f"[Network] Received message from {identity}: {message.msg_type}, {message.payload}")
        match message.msg_type:
            case MessageType.GOSSIP:
                self.thread_pool.submit(self.handle_gossip, identity, message.payload)
            case MessageType.GOSSIP_INTRODUCTION:
                self.thread_pool.submit(self.handle_gossip_introduction, identity, message.payload)
            case MessageType.REQUEST_FULL_LIST:
                self.thread_pool.submit(self.handle_request_full_list, identity, message.payload)
            case MessageType.REPLICA:
                self.thread_pool.submit(self.handle_replica, identity, message.payload)
            case MessageType.SENT_FULL_LIST:
                self.thread_pool.submit(self.handle_sent_full_list, identity, message.payload)
            case MessageType.HINTED_HANDOFF:
                self.thread_pool.submit(self.handle_hinted_handoff, identity, message.payload)
            case _:
                print(f"[Network] Unknown message type: {message.msg_type}")


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

    def remove_server(self, port):
        for s in list(self.servers):  # copy to avoid mutation issues
            if str(s.port) == str(port):
                self.servers.remove(s)
                print(f"[Gossip] Server {port} removed")
                return
            
    def remove_proxy(self, port):
        for p in list(self.proxies):
            if str(p.port) == str(port):
                self.proxies.remove(p)
                print(f"[Gossip] Proxy {port} removed")
                return



    def handle_gossip(self, identity, payload):
        incoming_servers = payload.get("servers", [])
        incoming_proxies = payload.get("proxies", [])
        hash_ring_version = payload.get("hash_ring_version", 1)

        # Process Servers
        my_server_ports = {str(s.port) for s in self.servers}
        my_server_ports.add(str(self.port))
        my_proxy_ports = {str(p.port) for p in self.proxies}

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


    def heartbeat(self):

        print("[Heartbeat] Starting heartbeat to monitor server health...")
        while self.running:
            all_lists = self.storage.get_all_lists() # TODO could be more optimized 
            lists_by_server = {}
            port_server_map = {str(s.port): s for s in self.servers}
            for shop_list in all_lists: 
                intended_server = self.get_intended_server(shop_list)
                if intended_server.port != self.port:
                    if intended_server.port not in lists_by_server:
                        lists_by_server[intended_server.port] = []
                    lists_by_server[intended_server.port].append(shop_list)
                    print(f"[Heartbeat] replica: {shop_list.isReplica} list {shop_list.uuid} intended for server {intended_server.port}")

            for server in lists_by_server:
                if len(lists_by_server[server]) > 0:
                    self.thread_pool.submit(self.send_hinted_handoff, port_server_map[server], lists_by_server[server])
            time.sleep(10)


    def handle_request_full_list(self, identity, payload): # TODO missing quorum logic
        print(f"[Network] Handling REQUEST_FULL_LIST from {identity}: {payload}")
        full_list = self.storage.get_list_by_id(payload["list_id"])

        if full_list and hasattr(full_list, 'isReplica'):
            delattr(full_list, 'isReplica')

        shopping_list = full_list.to_json() if full_list else None


        message = None
        if full_list is None:
            print(f"[Network] No list found with ID {payload['list_id']}")
            message = Message(msg_type=MessageType.REQUEST_FULL_LIST_NACK, payload={})

        else:
            message = Message(msg_type=MessageType.REQUEST_FULL_LIST_ACK, payload={"shopping_list": shopping_list})

        self.server_interface_socket.send_multipart([identity, message.serialize()])
        print(f"[Network] Sent {message.msg_type} to {identity}")

    def handle_replica(self, identity, payload):
        print(f"[Network] Handling REPLICA from {identity}")

        replica_item = payload["replica_list"]
        replica_id = payload["replicaID"]
        
        self.storage.save_list(ShoppingList.from_json(replica_item), is_replica=True, replica_id=replica_id, name=json.loads(replica_item).get('name', None))

        ack_message = Message(msg_type=MessageType.REPLICA_ACK, payload={})
        self.server_interface_socket.send_multipart([identity, ack_message.serialize()])

        print(f"[Network] Sent REPLICA_ACK to {identity}")


#    def get_hashring_neighbors(self):
#        sorted_servers = sorted(self.servers, 
#                                key=lambda s: s.hash)
#
#        # Only 1 server → no neighbors
#        if len(sorted_servers) == 1:
#            return None, None
#
#        current_index = next(i for i, s in enumerate(sorted_servers) if s.port == self.port)
#
#        left_neighbor  = sorted_servers[current_index - 1]
#        right_neighbor = sorted_servers[(current_index + 1) % len(sorted_servers)]
#
#        return left_neighbor, right_neighbor


    def get_intended_server(self, shop_list):
        sorted_servers = sorted(
            self.servers,
            key=lambda s: s.hash
        )

        list_hash = hashlib.sha256(shop_list.uuid.encode()).hexdigest()

        hash_server = None

        for server in sorted_servers:
            if list_hash <= server.hash:
                hash_server = server
                break

        if shop_list.isReplica:
            hash_server_index = sorted_servers.index(hash_server)
            return sorted_servers[(hash_server_index + shop_list.replicaID) % len(sorted_servers)]
        else:
            return hash_server


    def handle_sent_full_list(self, identity, payload):
        print(f"[Network] Handling SENT_FULL_LIST from {identity}: {payload}")
        full_list = payload["shopping_list"]
        shopping_list = ShoppingList.from_json(full_list)

        self.storage.save_list(shopping_list, is_replica=False, name=shopping_list.name)
        merged_list = self.storage.get_list_by_id(shopping_list.uuid, 0)

        
        self.thread_pool.submit(self.send_replica, merged_list)

        if hasattr(merged_list, 'isReplica'):
            delattr(merged_list, 'isReplica')

        ack_message = Message(msg_type=MessageType.SENT_FULL_LIST_ACK, payload={"shopping_list": merged_list.to_json()})
        self.server_interface_socket.send_multipart([identity, ack_message.serialize()])
        print(f"[Network] Sent SENT_FULL_LIST_ACK to {identity}")

    def send_replica(self, shop_list):
        ring = sorted(self.servers, key=lambda s: s.hash)
        ring_len = len(ring)

        list_hash = hashlib.sha256(shop_list.uuid.encode()).hexdigest()

        primary_index = None
        for i, srv in enumerate(ring):
            if list_hash <= srv.hash:
                primary_index = (i + 1) % ring_len
                break
        if primary_index is None:
            primary_index = 0  

        successes = 0
        attempted = 0
        index = primary_index

        results = []  # (server, accepted: True/False)

        while successes < REPLICA_COUNT and attempted < ring_len:
            server = ring[index]
            replicaID = successes + 1  
            ok = self._try_send_replica_to_server(server, shop_list, replicaID)
            results.append((server, ok))

            if ok:
                successes += 1

            attempted += 1
            index = (index + 1) % ring_len

    
    def _try_send_replica_to_server(self, server, replica_list, replicaID):
        print(f"[Network] Attempting to send {len(replica_list.items)} items to server {server.port}")

        if hasattr(replica_list, 'isReplica'):
            delattr(replica_list, 'isReplica')

        replica_message = Message(
            msg_type=MessageType.REPLICA,
            payload={"replica_list": replica_list.to_json(), "replicaID": replicaID}
        )

        # Connect or reuse socket
        server_socket = self.context.socket(zmq.DEALER)
        server_socket.connect(f"tcp://localhost:{server.port}")


        retries = 3
        timeout = 1000  # ms

        poller = zmq.Poller()
        poller.register(server_socket, zmq.POLLIN)

        for attempt in range(1, retries + 1):
            print(f"[Replica] Sending REPLICA x{len(replica_list.items)} to {server.port} "
                f"(attempt {attempt}/{retries})")

            server_socket.send(replica_message.serialize())
            socks = dict(poller.poll(timeout))

            if server_socket in socks and socks[server_socket] == zmq.POLLIN:
                reply = Message(json_str=server_socket.recv())

                if reply.msg_type == MessageType.REPLICA_ACK:
                    print(f"[Replica] ACK received from {server.port}")
                    server_socket.close(linger=0)
                    return True

                print(f"[Replica] Unexpected reply {reply.msg_type}")

            else:
                print(f"[Replica] No reply from {server.port}, retrying...")

            timeout = min(8000, timeout * 2)

        print(f"[Replica] FAILED after retries → {server.port}")
        server_socket.close(linger=0)
        return False



    def send_hinted_handoff(self, server, shop_lists):
        
        for shop_list in shop_lists:
            print(f"[Network] Sending HINTED_HANDOFF to server {server.port} for list {shop_list.uuid}")
            print(f"test: {shop_list.isReplica}")

        replica_lists = []
        main_lists = []

        for shop_list in shop_lists:
            if not shop_list.isReplica:
                if hasattr(shop_list, 'isReplica'):
                    delattr(shop_list, 'isReplica')
                main_lists.append(shop_list.to_json())
            else:
                if hasattr(shop_list, 'isReplica'):
                    delattr(shop_list, 'isReplica')
                replica_lists.append(shop_list.to_json())

        

        message = Message(
            msg_type=MessageType.HINTED_HANDOFF,
            payload={
                "main_lists": main_lists,
                "replica_lists": replica_lists
            }
        )

        # Create / reuse socket
        s = self.context.socket(zmq.DEALER)
        s.connect(f"tcp://localhost:{server.port}")


        retries = 3
        timeout = 1000  # ms

        poller = zmq.Poller()
        poller.register(s, zmq.POLLIN)

        for attempt in range(1, retries + 1):
            print(f"[Handoff] Sending HINTED_HANDOFF to {server.port} "
                f"(attempt {attempt}/{retries})")

            s.send(message.serialize())

            socks = dict(poller.poll(timeout))

            if s in socks and socks[s] == zmq.POLLIN:
                reply_bytes = s.recv()
                reply = Message(json_str=reply_bytes)

                if reply.msg_type == MessageType.HINTED_HANDOFF_ACK:
                    print(f"[Handoff] HINTED_HANDOFF_ACK received from {server.port}")

                    for shop_list in shop_lists:
                        self.storage.delete_list(shop_list.uuid)
                    s.close(linger=0)
                    return True

                print(f"[Handoff] Unexpected reply: {reply.msg_type}")

            else:
                print(f"[Handoff] No ACK from {server.port}, retrying...")

            timeout = min(timeout * 2, 8000)

        # FAILED → queue it
        print(f"[Handoff] FAILED to send hinted handoff to {server.port}, queueing action")
        s.close(linger=0)
        return False

    def handle_hinted_handoff(self, identity, payload):
        print(f"[Network] Handling HINTED_HANDOFF from {identity}: {payload}")

        main_lists = payload["main_lists"]
        replica_lists = payload["replica_lists"]

        for shop_list in main_lists:
            self.storage.save_list(ShoppingList.from_json(shop_list), is_replica=False, name=json.loads(shop_list).get('name', None))

        for shop_list in replica_lists:
            self.storage.save_list(ShoppingList.from_json(shop_list), is_replica=True, name=json.loads(shop_list).get('name', None))

        ack_message = Message(msg_type=MessageType.HINTED_HANDOFF_ACK, payload={})
        self.server_interface_socket.send_multipart([identity, ack_message.serialize()])

        print(f"[Network] Sent HINTED_HANDOFF_ACK to {identity}")

    def handle_gossip_introduction(self, identity, payload):
        incoming_servers = payload.get("servers", [])
        incoming_proxies = payload.get("proxies", [])
        hash_ring_version = payload.get("hash_ring_version", 1)

        # Process Servers
        my_server_ports = {str(s.port) for s in self.servers}
        my_proxy_ports = {str(p.port) for p in self.proxies}
        my_proxy_ports.add(str(self.port))

        # always merge introductions
        #print(f"[Gossip] Handling gossip introduction from {identity}: servers={incoming_servers},proxies={incoming_proxies}, version={hash_ring_version}")
        changed = False
        for p in incoming_servers:
            if str(p) not in my_server_ports:
                #print(f"[Gossip] Discovered new Server: {p}")
                self.connect_to_server(p)
                changed = True
        for p in incoming_proxies:
            if str(p) not in my_proxy_ports:
                #print(f"[Gossip] Discovered new Proxy: {p}")
                self.connect_to_proxy(p)
                changed = True

        if changed:
            self.hash_ring_version = max(self.hash_ring_version, hash_ring_version) + 1



