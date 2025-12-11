import hashlib
import pyzmq
from common.messages.messages import Message, MessageType
from src.common.threadPool.threadPool import ThreadPool
import time 
import random 

GOSSIP_FANOUT = 2
REPLICA_COUNT = 3

class Server():
    def __init__(self, port, hash):
        self.hash = hash
        self.port = port
        self.socket = None

    def setSocket(self, socket):
        self.socket = socket

class ServerCommunicator:
    def __init__(self, storage, port, known_servers):
        self.storage = storage
        self.port = port
        self.hash = hashlib.sha256(f"server_{port}".encode()).hexdigest()
        self.known_servers = known_servers
        self.running = True
        self.daemon = True 
        self.context = pyzmq.Context()
        self.poller = pyzmq.Poller()
        self.server_interface_socket = None
        self.servers = []
        self.thread_pool = ThreadPool(8)
        self.disconnected = False


    def start(self): 
        
        self.setup_server_interface_socket()
        print(f"[System] Starting server on port {self.port}...")
        self.setup_servers()
        self.thread_pool.submit(self.gossip)
        self.thread_pool.submit(self.heartbeat)
        self.loop()

    def setup_server_interface_socket(self):
        self.server_interface_socket = self.context.socket(pyzmq.ROUTER)
        self.server_interface_socket.bind(f"tcp://localhost:{self.port}")
        self.poller.register(self.server_interface_socket, pyzmq.POLLIN)

    def setup_servers(self):
        print("[Network] Setting up known servers...")
        for port, hash in self.known_servers:
            server = Server(port, hash)
            self.servers.append(server)
            socket = self.context.socket(pyzmq.DEALER)
            socket.connect(f"tcp://localhost:{port}")
            server.setSocket(socket)

    def gossip(self):   
        print("[Gossip] Starting gossip to notify other servers...")
        while self.running:
            random_servers = random.sample(self.servers, min(len(self.servers), GOSSIP_FANOUT))
            for server in random_servers:
                self.notify_server(server)
            time.sleep(5)


    def notify_server(self, server):
        message = Message(
            msg_type=MessageType.GOSSIP_SERVER_LIST,
            payload={ "ports": [s.port for s in self.servers] , "hashes": [s.hash for s in self.servers]}
        )
        server.socket.send(message.serialize())
        print(f"[Gossip] Notified server {server.port} of my presence.")

    def loop(self):
        print("[System] Entering main server loop...")
        while self.running:
            socks = dict(self.poller.poll(1000))
            for sock in socks:
                if socks[sock] == pyzmq.POLLIN: # idk
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
            case MessageType.GOSSIP_SERVER_LIST:
                self.thread_pool.submit(self.handle_gossip_server_list, identity, message.payload)
            case MessageType.GOSSIP_SERVER_REMOVAL:
                self.thread_pool.submit(self.handle_gossip_server_removal, identity, message.payload)
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

    def handle_gossip_server_list(self, identity, payload):

        print(f"[Network] Handling GOSSIP_SERVER_LIST from {identity}: {payload}")
        ports = payload["ports"]
        hashes = payload["hashes"]

        
        new_servers = []
        for port, hash in zip(ports, hashes):
            if port not in [s.port for s in self.servers] and port != self.port:
                print(f"[Network] Discovered new server from gossip: {port}")
                new_server = Server(port, hash)
                self.servers.append(new_server)
                new_socket = self.context.socket(pyzmq.DEALER)
                new_socket.connect(f"tcp://localhost:{port}")
                new_server.setSocket(new_socket)
                new_servers.append(new_server)


    def handle_gossip_server_removal(self, identity, payload):
        print(f"[Network] Handling GOSSIP_SERVER_REMOVAL from {identity}: {payload}")
        removed_port = payload["port"]
        self.servers = [s for s in self.servers if s.port != removed_port]
        print(f"[Network] Removed server {removed_port} from known servers.")

        message = Message(msg_type=MessageType.GOSSIP_SERVER_REMOVAL, payload={"port": removed_port})
        for server in random.sample(self.servers, min(len(self.servers), GOSSIP_FANOUT)):
            server.socket.send(message.serialize())
            print(f"[Gossip] Informed server {server.port} of removal of server {removed_port}.")

    def heartbeat(self):

        print("[Heartbeat] Starting heartbeat to monitor server health...")
        while self.running:
            all_lists = self.storage.get_all_lists() # TODO could be more optimized 
            lists_by_server = {}
            for shop_list in all_lists: 
                intended_servers = self.get_intended_servers(shop_list['uuid'])
                for intended_server in intended_servers:
                    if intended_server.port is not self.port:
                        if intended_server.port not in lists_by_server:
                            lists_by_server[intended_server.port] = []
                        lists_by_server[intended_server.port].append(shop_list)

            for server in lists_by_server:
                self.thread_pool.submit(self.send_hinted_handoff, server, lists_by_server[server])
            time.sleep(10)


    def handle_request_full_list(self, identity, payload): # TODO missing quorum logic
        print(f"[Network] Handling REQUEST_FULL_LIST from {identity}: {payload}")
        full_list = self.storage.get_list_by_id(payload["uuid"])
        message = None
        if full_list is None:
            print(f"[Network] No list found with ID {payload['uuid']}")
            message = Message(msg_type=MessageType.REQUEST_FULL_LIST_NACK, payload={})

        else:
            message = Message(msg_type=MessageType.REQUEST_FULL_LIST_ACK, payload={"full_list": full_list})

        self.server_interface_socket.send_multipart([identity, message.serialize()])
        print(f"[Network] Sent {message.msg_type} to {identity}")

    def handle_replica(self, identity, payload):
        print(f"[Network] Handling REPLICA from {identity}")

        replica_item = payload["replica_list"]

        self.storage.save_list(replica_item, is_replica=True)

        ack_message = Message(msg_type=MessageType.REPLICA_ACK, payload={})
        self.server_interface_socket.send_multipart([identity, ack_message.serialize()])

        print(f"[Network] Sent REPLICA_ACK to {identity}")


    def get_hashring_neighbors(self):
        sorted_servers = sorted(self.servers + [Server(self.port, self.hash)], 
                                key=lambda s: s.hash)

        # Only 1 server → no neighbors
        if len(sorted_servers) == 1:
            return None, None

        current_index = next(i for i, s in enumerate(sorted_servers) if s.port == self.port)

        left_neighbor  = sorted_servers[current_index - 1]
        right_neighbor = sorted_servers[(current_index + 1) % len(sorted_servers)]

        return left_neighbor, right_neighbor


    def get_next_servers(self, server):

        sorted_servers = sorted(
            self.servers, key=lambda s: s.hash
        )
        current_index = next(i for i, s in enumerate(sorted_servers) if s.port == server.port)
        next_servers = []
        for i in range(1, min(REPLICA_COUNT, len(sorted_servers)) + 1):
            next_server = sorted_servers[(current_index + i) % len(sorted_servers)]
            next_servers.append(next_server)
        return next_servers

    def get_intended_servers(self, shop_list):
        sorted_servers = sorted(
            self.servers + [Server(self.port, self.hash)],
            key=lambda s: s.hash
        )

        list_hash = hashlib.sha256(shop_list['uuid'].encode()).hexdigest()

        hash_server = None

        for server in sorted_servers:
            if list_hash <= server.hash:
                hash_server = server
                break

        if shop_list['isReplica']:
            return self.get_next_servers(hash_server)
        else:
            return [hash_server] if hash_server else []


    def handle_sent_full_list(self, identity, payload):
        print(f"[Network] Handling SENT_FULL_LIST from {identity}: {payload}")
        full_list = payload["full_list"]

        self.storage.save_list(full_list, is_replica=False)

        self.thread_pool.submit(self.send_replica, full_list)

        ack_message = Message(msg_type=MessageType.SENT_FULL_LIST_ACK, payload={})
        self.server_interface_socket.send_multipart([identity, ack_message.serialize()])
        print(f"[Network] Sent SENT_FULL_LIST_ACK to {identity}")

    def send_replica(self, shop_list):
        ring = sorted(self.servers, key=lambda s: s.hash)
        ring_len = len(ring)

        list_hash = hashlib.sha256(shop_list['uuid'].encode()).hexdigest()

        primary_index = None
        for i, srv in enumerate(ring):
            if srv.hash >= list_hash:
                primary_index = i
                break
        if primary_index is None:
            primary_index = 0  

        successes = 0
        attempted = 0
        index = primary_index

        results = []  # (server, accepted: True/False)

        while successes < REPLICA_COUNT and attempted < ring_len:
            server = ring[index]

            ok = self._try_send_replica_to_server(server, replica_list)
            results.append((server, ok))

            if ok:
                successes += 1

            attempted += 1
            index = (index + 1) % ring_len

        return results
    
    def _try_send_replica_to_server(self, server, replica_list):
        print(f"[Network] Attempting to send {len(replica_list)} items to server {server.port}")

        replica_message = Message(
            msg_type=MessageType.REPLICA,
            payload={"replica_list": replica_list}
        )

        # Connect or reuse socket
        if server.socket is None:
            server_socket = self.context.socket(pyzmq.DEALER)
            server_socket.connect(f"tcp://localhost:{server.port}")
            server.setSocket(server_socket)
        else:
            server_socket = server.socket

        retries = 3
        timeout = 1000  # ms

        poller = pyzmq.Poller()
        poller.register(server_socket, pyzmq.POLLIN)

        for attempt in range(1, retries + 1):
            print(f"[Replica] Sending REPLICA x{len(replica_list)} to {server.port} "
                f"(attempt {attempt}/{retries})")

            server_socket.send(replica_message.serialize())
            socks = dict(poller.poll(timeout))

            if server_socket in socks and socks[server_socket] == pyzmq.POLLIN:
                reply = Message(json_str=server_socket.recv())

                if reply.msg_type == MessageType.REPLICA_ACK:
                    print(f"[Replica] ACK received from {server.port}")
                    return True

                print(f"[Replica] Unexpected reply {reply.msg_type}")

            else:
                print(f"[Replica] No reply from {server.port}, retrying...")

            timeout = min(8000, timeout * 2)

        print(f"[Replica] FAILED after retries → {server.port}")
        return False



    def send_hinted_handoff(self, server, shop_lists):
        for shop_list in shop_lists:
            print(f"[Network] Sending HINTED_HANDOFF to server {server.port} for list {shop_list['uuid']}")

        replica_lists = []
        main_lists = []

        for shop_list in shop_lists:
            if not shop_list['isReplica']:
                main_lists.append(shop_list)
            else:
                replica_lists.append(shop_list)

        message = Message(
            msg_type=MessageType.HINTED_HANDOFF,
            payload={
                "main_lists": main_lists,
                "replica_lists": replica_lists
            }
        )

        # Create / reuse socket
        if server.socket is None:
            s = self.context.socket(pyzmq.DEALER)
            s.connect(f"tcp://localhost:{server.port}")
            server.setSocket(s)
        else:
            s = server.socket

        retries = 3
        timeout = 1000  # ms

        poller = pyzmq.Poller()
        poller.register(s, pyzmq.POLLIN)

        for attempt in range(1, retries + 1):
            print(f"[Handoff] Sending SENT_FULL_LIST to {server.port} "
                f"(attempt {attempt}/{retries})")

            s.send(message.serialize())

            socks = dict(poller.poll(timeout))

            if s in socks and socks[s] == pyzmq.POLLIN:
                reply_bytes = s.recv()
                reply = Message(json_str=reply_bytes)

                if reply.msg_type == MessageType.HINTED_HANDOFF_ACK:
                    print(f"[Handoff] HINTED_HANDOFF_ACK received from {server.port}")

                    for shop_list in shop_lists:
                        self.storage.delete_list(shop_list['uuid'])
                    return True

                print(f"[Handoff] Unexpected reply: {reply.msg_type}")

            else:
                print(f"[Handoff] No ACK from {server.port}, retrying...")

            timeout = min(timeout * 2, 8000)

        # FAILED → queue it
        print(f"[Handoff] FAILED to send hinted handoff to {server.port}, queueing action")
        return False

    def handle_hinted_handoff(self, identity, payload):
        print(f"[Network] Handling HINTED_HANDOFF from {identity}: {payload}")

        main_lists = payload["main_lists"]
        replica_lists = payload["replica_lists"]

        for shop_list in main_lists:
            self.storage.save_list(shop_list, is_replica=False)

        for shop_list in replica_lists:
            self.storage.save_list(shop_list, is_replica=True)

        ack_message = Message(msg_type=MessageType.HINTED_HANDOFF_ACK, payload={})
        self.server_interface_socket.send_multipart([identity, ack_message.serialize()])

        print(f"[Network] Sent HINTED_HANDOFF_ACK to {identity}")



