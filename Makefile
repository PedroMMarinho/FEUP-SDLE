# Variables
PYTHON = python3
SRC_DIR = src

# Default target
all: help

# --- SETUP ---
install:
	pip install -r requirements.txt

init-db:
	@echo "Initializing Client DBs..."
	rm -f *.db
	@echo "Cleaned old databases."

# --- RUNNING COMPONENTS ---

# Run the Proxy (The Middleman)
# Usage: make proxy
proxy:
	export PYTHONPATH=$(PWD) && $(PYTHON) -m src.proxy.main

# Run a Server Node (Simulating Cloud Storage)
# Usage: make server port=5556
server:
	export PYTHONPATH=$(PWD) && $(PYTHON) -m src.server.main --port $(or $(port),5556)

# Run Client 1 (User A)
# Creates/Uses client_A.db
client1:
	export PYTHONPATH=$(PWD) && $(PYTHON) -m src.client.main --id "User_A" --db "client_A.db"

# Run Client 2 (User B)
# Creates/Uses client_B.db
client2:
	export PYTHONPATH=$(PWD) && $(PYTHON) -m src.client.main --id "User_B" --db "client_B.db"

# --- UTILS ---

# Clean up all generated database files and pycache
clean:
	rm -f *.db
	find . -type d -name "__pycache__" -exec rm -rf {} +
	@echo "Project cleaned."

help:
	@echo "Available commands:"
	@echo "  make install    - Install dependencies"
	@echo "  make proxy      - Run the load balancer/proxy"
	@echo "  make server     - Run a storage node (default port 5556)"
	@echo "  make client1    - Run Client A"
	@echo "  make client2    - Run Client B"
	@echo "  make clean      - Remove .db files and pycache"