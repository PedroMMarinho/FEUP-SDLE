import zmq
import time
from src.common.crdt.improved.ShoppingList import ShoppingList
from src.common.messages.messages import Message, MessageType
import random

class Proxy():
    def __init__(self, port):
        self.port = port
        self.requestSocket = None
        self.subscriberSocket = None

    def setRequestSocket(self, socket):
        self.requestSocket = socket

    def setSubscriberSocket(self, socket):  
        self.subscriberSocket = socket


class ClientCommunicator():
    def __init__(self, db_path, known_proxies, storage):
        self.db_path = db_path
        self.known_proxies = known_proxies
        self.context = zmq.Context()
        self.poller = zmq.Poller()
        self.running = True
        self.storage = storage
        self.proxies = []
        self.init_proxies()
        self.init_subscriber_socket()

    def init_proxies(self):
        for port in self.known_proxies:
            proxy = Proxy(port)
            self.proxies.append(proxy)
            proxy_socket = self.context.socket(zmq.DEALER)
            proxy_socket.connect(f"tcp://localhost:{port}")

            subscribe_socket = self.context.socket(zmq.SUB)
            subscribe_socket.setsockopt_string(zmq.SUBSCRIBE, "")
            pub_port = port + 1  # convention: PUB = DEALER + 1
            subscribe_socket.connect(f"tcp://localhost:{pub_port}")
            self.poller.register(subscribe_socket, zmq.POLLIN)
            print(f"[Network] Subscribed to proxy PUB {pub_port}")

            proxy.setSubscriberSocket(subscribe_socket)
            proxy.setRequestSocket(proxy_socket)


    def init_subscriber_socket(self):
        self.subscriber = self.context.socket(zmq.SUB)
        self.subscriber.setsockopt_string(zmq.SUBSCRIBE, "")

        for proxy in self.proxies:
            pub_port = proxy.port + 1  # convention: PUB = DEALER + 1
            self.subscriber.connect(f"tcp://localhost:{pub_port}")
            print(f"[Network] Subscribed to proxy PUB {pub_port}")
        
       


    def run(self):
        """
        Background thread:
        - Subscribes to proxy updates
        - Receives CRDT updates
        - Merges them into local storage
        """
        print("[Network] Background sync thread started")

        poller = zmq.Poller()
        poller.register(self.subscriber, zmq.POLLIN)

        while self.running:
            try:
                socks = dict(poller.poll(1000))

                if self.subscriber in socks:
                    raw = self.subscriber.recv()
                    message = Message(json_str=raw)

                    if message.msg_type == MessageType.LIST_UPDATE:

                        self._handle_list_update(message.payload)

            except zmq.ContextTerminated:
                break
            except Exception as e:
                print(f"[Network] Subscriber error: {e}")

        self.subscriber.close()
        print("[Network] Background sync thread stopped")


    def _try_send_full_list_to_proxy(self, proxy, message, retries=3, base_timeout=1000):
        """
        Attempt to send a full list to a single proxy with retries.
        Returns True on ACK, False otherwise.
        """
        socket = proxy.requestSocket
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)

        timeout = base_timeout

        for attempt in range(1, retries + 1):
            print(
                f"[Network] Sending FULL_LIST to proxy {proxy.port} "
                f"(attempt {attempt}/{retries})"
            )

            socket.send(message.serialize())

            socks = dict(poller.poll(timeout))

            if socket in socks and socks[socket] == zmq.POLLIN:
                reply = Message(json_str=socket.recv())

                if reply.msg_type == MessageType.SENT_FULL_LIST_ACK:
                    print(f"[Network] ACK received from proxy {proxy.port}")
                    return reply.payload
                if reply.msg_type == MessageType.SENT_FULL_LIST_NACK:
                    print(f"[Network] NACK received from proxy {proxy.port}")
                    return None
                else:
                    print(
                        f"[Network] Unexpected reply from {proxy.port}: "
                        f"{reply.msg_type}"
                    )
            else:
                print(f"[Network] No reply from {proxy.port}, retrying...")

            timeout = min(8000, timeout * 2)

        print(f"[Network] Proxy {proxy.port} failed after retries")
        return None


    def send_full_list(self,shopping_list):
        """
        Sends a full list to proxies with retries and proxy failover.
        """

        list_uuid = shopping_list.uuid

        message = Message(
            msg_type=MessageType.SENT_FULL_LIST,
            payload={
                "shopping_list": shopping_list.to_json()
            }
        )

        print(f"[Network] Sending full list '{list_uuid}' to proxies")

        # Shuffle proxies so load is distributed
        proxies = self.proxies[:]
        random.shuffle(proxies)

        for proxy in proxies:
            print(f"[Network] Trying proxy {proxy.port}")

            result = self._try_send_full_list_to_proxy(proxy, message)

            if result is not None:
                print(
                    f"[Network] Full list '{list_uuid}' successfully sent "
                    f"via proxy {proxy.port}"
                )

                self.storage.save_list(ShoppingList.from_json(result["shopping_list"]), not_sent=False)
                return

            print(f"[Network] Switching proxy...")

        print(
            f"[Network] FAILED: Could not send full list '{list_uuid}' "
            f"to any proxy"
        )
        self.storage.save_list(shopping_list, not_sent=True)
    
    def _try_request_full_list_from_proxy(self,proxy,message,retries=3,base_timeout=1000):
        """
        Request a full shopping list from a single proxy with retries.
        Returns the CRDT payload on success, None on failure.
        """
        socket = proxy.socket
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)

        timeout = base_timeout
        # TODO IS THE PLAYLOAD'list_uuid'?
        for attempt in range(1, retries + 1):
            print(
                f"[Network] Requesting FULL_LIST '{message.payload['list_id']}' "
                f"from proxy {proxy.port} "
                f"(attempt {attempt}/{retries})"
            )

            socket.send(message.serialize())

            socks = dict(poller.poll(timeout))

            if socket in socks and socks[socket] == zmq.POLLIN:
                reply = Message(json_str=socket.recv())

                if reply.msg_type == MessageType.REQUEST_FULL_LIST_ACK:
                    print(f"[Network] Full list received from proxy {proxy.port}")
                    return reply.payload    

                if reply.msg_type == MessageType.REQUEST_FULL_LIST_NACK:
                    print(f"[Network] Error received from proxy {proxy.port}")
                    return None


            else:
                print(f"[Network] No reply from {proxy.port}, retrying...")

            timeout = min(8000, timeout * 2)

        print(f"[Network] Proxy {proxy.port} failed after retries")
        return None

    
    def request_full_list(self, list_uuid):
        """
        Requests a full shopping list from proxies with retries and failover.
        Returns the CRDT payload on success, None on failure.
        """
        message = Message(
            msg_type=MessageType.REQUEST_FULL_LIST,
            payload={"list_id": list_uuid}
        )

        print(f"[Network] Requesting full list '{list_uuid}'")

        proxies = self.proxies[:]
        random.shuffle(proxies)

        for proxy in proxies:
            print(f"[Network] Trying proxy {proxy.port}")

            result = self._try_request_full_list_from_proxy(proxy, message)

            if result is not None:
                print(
                    f"[Network] Full list '{list_uuid}' received "
                    f"via proxy {proxy.port}"
                )

                self.storage.save_list(ShoppingList.from_json(result["shopping_list"]), not_sent=False)
                self.subscribe_to_list(list_uuid)
                return 

            print(f"[Network] Switching proxy...")

        print(
            f"[Network] FAILED: Could not retrieve full list '{list_uuid}' "
            f"from any proxy"
        )
        return None
    
    def heartbeat(self):
        """Try to send not sent lists periodically."""
        while self.running:
            not_sent_lists = self.storage.get_all_not_sent_lists_and_metadata()
            for shopping_list in not_sent_lists:
                self.send_full_list(shopping_list)
            time.sleep(10)  # Send heartbeat every 10 seconds

    def pick_random_proxy(self):
        """Picks a random proxy from the known proxies list."""
        # For now, just return the default proxy address
        return random.choice(self.proxies)
    
    def subscribe_to_list(self, list_uuid):
        for proxy in self.proxies:
            proxy.subscriberSocket.setsockopt_string(zmq.SUBSCRIBE, list_uuid)
        print(f"[Network] Subscribed to updates for list {list_uuid}")
    
    def unsubscribe_from_list(self, list_uuid):
        for proxy in self.proxies:
            proxy.subscriberSocket.setsockopt_string(zmq.UNSUBSCRIBE, list_uuid)
        print(f"[Network] Unsubscribed from updates for list {list_uuid}")

    def _handle_list_update(self, payload):
        crdt_json = payload['shopping_list']
        crdt_json_obj = ShoppingList.from_json(crdt_json)
        print(f"[Network] Received LIST_UPDATE for list {crdt_json_obj.uuid}")

        self.storage.save_list(crdt_json_obj)

        print(f"[Network] Merged LIST_UPDATE for list {crdt_json_obj.uuid} into local storage")


