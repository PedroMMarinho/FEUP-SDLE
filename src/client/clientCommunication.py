import threading
import zmq
import time

class ClientCommunicator(threading.Thread):
    def __init__(self, db_path, client_id, proxy_address="tcp://localhost:5555"):
        threading.Thread.__init__(self)
        self.db_path = db_path
        self.client_id = client_id
        self.proxy_address = proxy_address
        self.running = True
        self.daemon = True  # Kills this thread if the main app closes

    def run(self):
        """The function that runs in the background thread."""
        print(f"[Network] Background thread started. Connecting to {self.proxy_address}...")
        
        context = zmq.Context()
        
        self.sender = context.socket(zmq.DEALER)
        self.sender.identity = self.client_id.encode('utf-8')
        self.sender.connect(self.proxy_address)

        self.subscriber = context.socket(zmq.SUB)
        self.subscriber.connect(self.proxy_address.replace("5555", "5556")) # Example pub port
        self.subscriber.setsockopt_string(zmq.SUBSCRIBE, "") # Subscribe to all

        # 2. Setup Independent DB Connection (Required for threads)
        # conn = sqlite3.connect(self.db_path) 
        
        # 3. Network Loop
        while self.running:
            # Non-blocking check for messages (simulated for now)
            try:
                # In real code: message = self.subscriber.recv_string(flags=zmq.NOBLOCK)
                # Parse message -> Merge into DB -> print("Synced!")
                time.sleep(1)
            except zmq.Again:
                pass
            except Exception as e:
                print(f"[Network Error] {e}")

        # Clean up
        context.destroy()
        print("[Network] Thread stopped.")

    def send_update(self, list_id, crdt_json):
        """Call this from the Interface to send data out."""
        # implementation placeholder
        print(f"[Network] Sending update for list {list_id}...")