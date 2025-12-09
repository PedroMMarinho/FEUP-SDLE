import threading
from queue import Queue

def worker(q):
    while True:
        task = q.get()
        if task is None:  # Shutdown signal
            break
        
        func, args = task
        func(*args)
        q.task_done()


class ThreadPool:
    def __init__(self, num_threads):
        self.q = Queue()
        self.threads = []
        for _ in range(num_threads):
            t = threading.Thread(target=worker, args=(self.q,), daemon=True)
            t.start()
            self.threads.append(t)

    def submit(self, func, *args):
        self.q.put((func, args))

    def shutdown(self):
        # Send sentinel None to stop threads
        for _ in self.threads:
            self.q.put(None)
        # Wait for all threads to exit
        for t in self.threads:
            t.join()