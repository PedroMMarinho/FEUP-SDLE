import threading

class ReadWriteLock:
    """A read-write lock allowing multiple readers or one writer."""
    def __init__(self):
        self._readers = 0
        self._writers = 0
        self._read_ready = threading.Condition(threading.Lock())
        self._write_ready = threading.Condition(threading.Lock())
    
    def acquire_read(self):
        """Acquire a read lock. Blocks if a writer is active."""
        self._read_ready.acquire()
        try:
            while self._writers > 0:
                self._read_ready.wait()
            self._readers += 1
        finally:
            self._read_ready.release()
    
    def release_read(self):
        """Release a read lock."""
        self._read_ready.acquire()
        try:
            self._readers -= 1
            if self._readers == 0:
                self._read_ready.notify_all()
        finally:
            self._read_ready.release()
    
    def acquire_write(self):
        """Acquire a write lock. Blocks until no readers/writers."""
        self._write_ready.acquire()
        self._writers += 1
        self._write_ready.release()
        
        self._read_ready.acquire()
        try:
            while self._readers > 0:
                self._read_ready.wait()
        finally:
            self._read_ready.release()
    
    def release_write(self):
        """Release a write lock."""
        self._write_ready.acquire()
        self._writers -= 1
        self._write_ready.release()
        
        self._read_ready.acquire()
        try:
            self._read_ready.notify_all()
        finally:
            self._read_ready.release()
