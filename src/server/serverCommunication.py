import hashlib
import pyzmq
from common.messages.messages import Message, MessageType
from src.common.threadPool.threadPool import ThreadPool
from src.server.actions import ServerActions
import time 
import random 

GOSSIP_FANOUT = 2

class Server():
    def __init__(self, port, hash):
        self.hash = hash
        self.port = port
        self.socket = None
        self.unreachable = False
        self.actionQueue = []

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


    def start(self): # TODO load action queue
        
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
            case MessageType.HEARTBEAT:
                self.thread_pool.submit(self.handle_heartbeat, identity, message.payload)
            case MessageType.SERVER_CONFIG_UPDATE:
                self.thread_pool.submit(self.handle_server_config_update, identity, message.payload)
            case MessageType.REQUEST_FULL_LIST:
                self.thread_pool.submit(self.handle_request_full_list, identity, message.payload)
            case MessageType.REPLICA:
                self.thread_pool.submit(self.handle_replica, identity, message.payload)
            case MessageType.SENT_FULL_LIST:
                self.thread_pool.submit(self.handle_sent_full_list, identity, message.payload)
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

        for new_server in new_servers:
            back, forward = self.get_hashring_neighbors()  # TODO incorrect logic here - new servers need to asked to send load balance instead
            if back and back.port == new_server.port:
                print(f"[Network] New server {new_server.port} is my back neighbor. Sending lists.")
            self.thread_pool.submit(self.load_balance_back, new_server)

        if forward and forward.port == new_server.port:
            print(f"[Network] New server {new_server.port} is my forward neighbor. Sending lists.")
            self.thread_pool.submit(self.load_balance_forward, new_server)

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
        while self.running:
            print("[Heartbeat] Sending heartbeat to all servers...")

            heartbeat_msg = Message(
                msg_type=MessageType.HEARTBEAT,
                payload={"port": self.port, "hash": self.hash}
            )

            poller = pyzmq.Poller()

            # 1. Send heartbeat to all servers and register sockets for poll
            for server in self.servers:

                if server.socket is None:
                    s = self.context.socket(pyzmq.DEALER)
                    s.connect(f"tcp://localhost:{server.port}")
                    server.setSocket(s)

                server.socket.send(heartbeat_msg.serialize())
                poller.register(server.socket, pyzmq.POLLIN)

            # 2. Poll all at once (shared timeout)
            socks = dict(poller.poll(timeout=2000))  # 2 seconds for all

            # 3. Process replies
            for server in self.servers:
                if server.socket in socks:
                    reply_bytes = server.socket.recv()
                    reply = Message(json_str=reply_bytes)
                    print(f"[Heartbeat] ACK from {server.port}: {reply.msg_type}")
                    server.unreachable = False
                    
                else:
                    print(f"[Heartbeat] No ACK from {server.port} → UNREACHABLE")
                    server.unreachable = True

            if all(s.unreachable for s in self.servers):
                print("[Heartbeat] All servers unreachable → disconnected")
                self.disconnected = True

            if any(not s.unreachable for s in self.servers) and self.disconnected:
                print("[Heartbeat] Reconnected to at least one server, requesting config update")
                self.disconnected = self.update_server_config( [s for s in self.servers if not s.unreachable] )

        
            # 4. Handle queued actions for now reachable servers
            for server in self.servers:
                if not server.unreachable and server.actionQueue:
                    self.thread_pool.submit(self.handle_action_queue, server)
                    
            time.sleep(10)

        
    

    def update_server_config(self, reachable_servers): # TODO fix this 
        print("[Network] Updating server configuration from reachable servers...")

        update_msg = Message(
            msg_type=MessageType.SERVER_CONFIG_UPDATE,
            payload={"port": self.port, "hash": self.hash}
        )

        poller = self.context.socket(pyzmq.Poller)
        
        updated = False

        for server in reachable_servers:

            s = server.socket
            if s is None:
                s = self.context.socket(pyzmq.DEALER)
                s.connect(f"tcp://localhost:{server.port}")
                server.setSocket(s)

            # Send update request
            print(f"[Network] Requesting config update from {server.port}")
            s.send(update_msg.serialize())

            # Poll for response
            poller = pyzmq.Poller()
            poller.register(s, pyzmq.POLLIN)

            socks = dict(poller.poll(timeout=2000))
            if s not in socks:
                print(f"[Network] No CONFIG_UPDATE_ACK from {server.port}.")
                continue

            # Receive ACK
            reply_bytes = s.recv()
            reply = Message(json_str=reply_bytes)

            if reply.msg_type != MessageType.SERVER_CONFIG_UPDATE_ACK:
                print(f"[Network] Wrong reply type from {server.port}: {reply.msg_type}")
                continue

            print(f"[Network] Received CONFIG_UPDATE_ACK from {server.port}")

            ports = reply.payload["ports"]
            hashes = reply.payload["hashes"]

            for p, h in zip(ports, hashes):
                if not any(srv.port == p for srv in self.servers):
                    print(f"[Network] Added new server from update: {p}")
                    new_server = Server(p, h)
                    self.servers.append(new_server)

            updated = True

        return not updated  # return new disconnected state
    
    def handle_action_queue(self, server):
        print(f"[Network] Handling action queue for server {server.port}")

        while server.actionQueue:
            action_type, payload = server.actionQueue[0]   # FIFO

            match action_type:
                case ServerActions.NOTIFY:
                    result = self.notify_server(server)

                case ServerActions.SEND_HINTED_HANDOFF:
                    result = self.send_hinted_handoff(server, payload)

                case ServerActions.REPLICA:
                    result = self.send_replica(server, payload)

                case ServerActions.LOAD_BALANCE:
                    result = self.load_balance_back(server)

                case _:
                    print(f"[Network] Unknown action: {action_type}")
                    result = True  # drop unknown actions

            # If the action failed → stop processing for now
            if not result:
                break

            # If success → remove from queue
            server.actionQueue.pop(0)


    def handle_server_config_update(self, identity, payload):
        print(f"[Network] Handling SERVER_CONFIG_UPDATE from {identity}: {payload}")

        ports = [s.port for s in self.servers]
        hashes = [s.hash for s in self.servers]

        ack_message = Message(
            msg_type=MessageType.SERVER_CONFIG_UPDATE_ACK,
            payload={"ports": ports, "hashes": hashes}
        )

        self.server_interface_socket.send_multipart([identity, ack_message.serialize()])
        print(f"[Network] Sent SERVER_CONFIG_UPDATE_ACK to {identity}")

    def handle_heartbeat(self, identity, payload):
        print(f"[Network] Handling HEARTBEAT from {identity}: {payload}")
        ack_message = Message(msg_type=MessageType.HEARTBEAT_ACK, payload={})
        self.server_interface_socket.send_multipart([identity, ack_message.serialize()])
        print(f"[Network] Sent HEARTBEAT_ACK to {identity}")

    def handle_request_full_list(self, identity, payload): # TODO missing quorum logic
        print(f"[Network] Handling REQUEST_FULL_LIST from {identity}: {payload}")
        full_list = self.storage.get_list_by_id(payload["list_id"])
        message = None
        if full_list is None:
            print(f"[Network] No list found with ID {payload['list_id']}")
            message = Message(msg_type=MessageType.REQUEST_FULL_LIST_NACK, payload={})

        else:
            message = Message(msg_type=MessageType.REQUEST_FULL_LIST_ACK, payload={"full_list": full_list})

        self.server_interface_socket.send_multipart([identity, message.serialize()])
        print(f"[Network] Sent {message.msg_type} to {identity}")

    def handle_replica(self, identity, payload):
        print(f"[Network] Handling REPLICA from {identity}")

        replica_items = payload["replica_list"]

        # Normalize to a list
        if isinstance(replica_items, dict):
            replica_items = [replica_items]

        print(f"[Network] Saving {len(replica_items)} replica items")

        for item in replica_items:
            self.storage.save_list(item, is_replica=True)

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

    
    def get_next2_servers(self):
        sorted_servers = sorted(
            self.servers + [Server(self.port, self.hash)],
            key=lambda s: s.hash
        )

        # Only yourself → no next servers
        if len(sorted_servers) <= 1:
            return []

        current_index = next(
            i for i, s in enumerate(sorted_servers)
            if s.port == self.port
        )

        next_servers = []
        seen = {self.port}  # avoid yourself

        # Walk forward in the ring
        for i in range(1, len(sorted_servers)):
            next_server = sorted_servers[(current_index + i) % len(sorted_servers)]

            if next_server.port not in seen:
                next_servers.append(next_server)
                seen.add(next_server.port)

            if len(next_servers) == 2:
                break

        return next_servers
    
    def get_intended_server(self, list_id):
        sorted_servers = sorted(
            self.servers + [Server(self.port, self.hash)],
            key=lambda s: s.hash
        )

        list_hash = hashlib.sha256(list_id.encode()).hexdigest()

        for server in sorted_servers:
            if server.hash <= list_hash:
                return server

        return sorted_servers[0]  # wrap around

    def load_balance_back(self, new_server):
        print(f"[Network] Load balancing to {new_server.port}")

        # 1. Collect local lists that now belong to new_server
        all_lists = self.storage.get_all_non_replica_lists()

        lists_to_send = [
            lst for lst in all_lists
            if self.get_intended_server(lst["uuid"]).port == new_server.port
        ]

        if not lists_to_send:
            print("[LoadBalance] Nothing to transfer.")
            return

        print(f"[LoadBalance] Preparing to send {len(lists_to_send)} lists to server {new_server.port}")

        # 2. Build message
        message = Message(
            msg_type=MessageType.LOAD_BALANCE,
            payload={"full_list": lists_to_send}
        )

        # 3. Ensure socket exists
        if new_server.socket is None:
            s = self.context.socket(pyzmq.DEALER)
            s.connect(f"tcp://localhost:{new_server.port}")
            new_server.setSocket(s)

        socket = new_server.socket

        # 4. Set up polling for ACK
        poller = pyzmq.Poller()
        poller.register(socket, pyzmq.POLLIN)

        retries = 3
        timeout = 1000  # ms

        for attempt in range(1, retries + 1):

            print(f"[LoadBalance] Sending LOAD_BALANCE to {new_server.port} "
                f"(attempt {attempt}/{retries})")

            socket.send(message.serialize())

            socks = dict(poller.poll(timeout))

            if socket in socks and socks[socket] == pyzmq.POLLIN:
                # Received something
                reply_bytes = socket.recv()
                reply = Message(json_str=reply_bytes)

                if reply.msg_type == MessageType.LOAD_BALANCE_ACK:
                    print(f"[LoadBalance] LOAD_BALANCE_ACK received from {new_server.port}")

                    return True

                else:
                    print(f"[LoadBalance] Unexpected reply ({reply.msg_type}). Retrying...")

            else:
                print(f"[LoadBalance] No LOAD_BALANCE_ACK from {new_server.port}, retrying...")
                timeout = min(timeout * 2, 8000)

        # FAILED AFTER RETRIES
        print(f"[LoadBalance] FAILED: {new_server.port} didnt respond.")
        new_server.actionQueue.append( (ServerActions.LOAD_BALANCE, None) )
        return False


            

    def load_balance_forward(self, new_server):
        print(f"[Network] Load balancing forward to {new_server.port}")
        result = self.load_balance_back(new_server)
        if result:
            print("[LoadBalance] Forward load balance successful. Deleting local copies of sent lists.")
            replica_lists = self.storage.get_all_replica_lists()
            for lst in replica_lists:
                intended_server = self.get_intended_server(lst['uuid'])
                if intended_server.port == new_server.port:
                    self.storage.delete_list_by_id(lst['uuid'])

    def handle_load_balance(self, identity, payload):
        print(f"[Network] Handling LOAD_BALANCE from {identity}: {payload}")
        full_lists = payload["full_list"]
        for shop_list in full_lists:
            self.storage.save_list(shop_list, is_replica=False)

        ack_message = Message(msg_type=MessageType.LOAD_BALANCE_ACK, payload={})
        self.server_interface_socket.send_multipart([identity, ack_message.serialize()])
        print(f"[Network] Sent LOAD_BALANCE_ACK to {identity}")

        self.thread_pool.submit(self.send_replicas_for_load_balanced_lists, full_lists)

        


    def handle_sent_full_list(self, identity, payload):
        print(f"[Network] Handling SENT_FULL_LIST from {identity}: {payload}")
        full_list = payload["full_list"]
        back,forward = self.get_hashring_neighbors()
        if forward and self.hash <= hashlib.sha256(full_list['uuid'].encode()).hexdigest() < forward.hash:
            self.storage.save_list(full_list, is_replica=False)
            for next_server in self.get_next2_servers():
                self.thread_pool.submit(self.send_replica, next_server, full_list)
        else: # hinted handoff
            intended_server = self.get_intended_server(full_list['uuid'])
            self.storage.save_list(full_list, is_replica=True, intended_server_hash=intended_server.hash)
            intended_server.actionQueue.append( (ServerActions.SEND_HINTED_HANDOFF, full_list) )


        ack_message = Message(msg_type=MessageType.SENT_FULL_LIST_ACK, payload={})
        self.server_interface_socket.send_multipart([identity, ack_message.serialize()])
        print(f"[Network] Sent SENT_FULL_LIST_ACK to {identity}")

    def send_replica(self, server, shop_list): # TODO quorum logic - must send to 3 servers total if not correct they must do hinted handoff
        # Normalize input: allow single item or list
        if isinstance(shop_list, dict):
            replica_list = [shop_list]
        else:
            replica_list = list(shop_list)  # make a shallow copy

        print(f"[Network] Sending {len(replica_list)} replica items to server {server.port}")

        replica_message = Message(
            msg_type=MessageType.REPLICA,
            payload={"replica_list": replica_list}
        )

        # Create or reuse socket
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
            print(f"[Replica] Sending REPLICA x{len(replica_list)} to server "
                f"{server.port} (attempt {attempt}/{retries})")

            server_socket.send(replica_message.serialize())

            socks = dict(poller.poll(timeout))

            if server_socket in socks and socks[server_socket] == pyzmq.POLLIN:
                reply_bytes = server_socket.recv()
                reply = Message(json_str=reply_bytes)

                if reply.msg_type == MessageType.REPLICA_ACK:
                    print(f"[Replica] ACK received from {server.port}")
                    return True

                print(f"[Replica] Unexpected reply: {reply.msg_type}")

            else:
                print(f"[Replica] No reply from {server.port}, retrying...")

            timeout = min(timeout * 2, 8000)

        # FAILED AFTER RETRIES
        print(f"[Replica] FAILED to send replicas to {server.port}, queueing action")
        server.actionQueue.append((ServerActions.REPLICA, replica_list))

        return False
        # TODO the failed status + info of the replica send should be stored in db

    def send_hinted_handoff(self, server, shop_list):
        print(f"[Network] Sending HINTED_HANDOFF to server {server.port} for list {shop_list['uuid']}")

        message = Message(
            msg_type=MessageType.SENT_FULL_LIST,
            payload={"full_list": shop_list}
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

                if reply.msg_type == MessageType.SENT_FULL_LIST_ACK:
                    print(f"[Handoff] SENT_FULL_LIST_ACK received from {server.port}")
                    return True

                print(f"[Handoff] Unexpected reply: {reply.msg_type}")

            else:
                print(f"[Handoff] No ACK from {server.port}, retrying...")

            timeout = min(timeout * 2, 8000)

        # FAILED → queue it
        print(f"[Handoff] FAILED to send hinted handoff to {server.port}, queueing action")
        server.actionQueue.append((ServerActions.SEND_HINTED_HANDOFF, shop_list))
        return False


        


