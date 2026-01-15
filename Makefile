# --- Variables ---
PYTHON = venv/bin/python3
SRC_DIR = src
SERVER_LOG_DIR = $(SRC_DIR)/server/server_logs
PROXY_LOG_DIR = $(SRC_DIR)/proxy/proxy_logs
DB_REGISTRY_FILE = $(SRC_DIR)/db/db_registry.txt
ID ?= User_Default

all: help

# --- SETUP & INFRASTRUCTURE ---
init: stop-servers servers
	@echo "--------------------------------------------------"
	@echo "System Initialized Successfully!"
	@echo "  1. Dependencies installed."
	@echo "  2. Database running."
	@echo "  3. Servers & Proxies started."
	@echo ""
	@echo "You can now run clients:"
	@echo "  make client ID=User_A"
	@echo "--------------------------------------------------"


install:
	pip install -r requirements.txt


clean-dbs:
	@echo "Stopping all shopping-db containers..."
	@docker ps -a --filter "name=shopping-db" -q | xargs -r docker stop
	@docker ps -a --filter "name=shopping-db" -q | xargs -r docker rm
	@# FIX: Point to the correct location of the registry file
	@rm -f $(DB_REGISTRY_FILE)
	@echo "Database containers and registry removed."

# --- RUNNING CLIENTS ---
# Usage: make client ID=User_A

client:
	@echo "--- [CLIENT] Initializing for $(ID) ---"
	@# capture the port printed by python to stdout
	$(eval DB_PORT := $(shell $(PYTHON) -m src.admin.main --action setup_db --user_id "$(ID)"))
	@echo "--- [CLIENT] DB Ready on Port $(DB_PORT). Launching App... ---"
	@PYTHONPATH=$(PWD) DB_HOST=localhost DB_PORT=$(DB_PORT) $(PYTHON) -m src.client.main \
		--id "$(ID)" \
		--db "client_$(ID).db" \
		--proxies $(PROXY_LOG_DIR)/known_proxies.txt

# --- RUNNING SERVERS VIA ADMIN TOOL ---
servers:
	mkdir -p $(SERVER_LOG_DIR) $(PROXY_LOG_DIR)
	PYTHONPATH=$(PWD) $(PYTHON) -m src.admin.main --action initial_setup

add-server:
	mkdir -p $(SERVER_LOG_DIR)
	PYTHONPATH=$(PWD) $(PYTHON) -m src.admin.main --action add_server

remove-server:
	@if [ -z "$(SERVER_NAME)" ]; then \
		echo "ERROR: Please provide SERVER_NAME, e.g. make remove-server SERVER_NAME=Server_3"; \
		exit 1; \
	fi
	PYTHONPATH=$(PWD) $(PYTHON) -m src.admin.main --action remove_server --server_name $(SERVER_NAME)

# --- CLEANUP ---
clean-logs: 
	rm -f $(SERVER_LOG_DIR)/server*.log
	rm -f $(PROXY_LOG_DIR)/proxy*.log
	@echo "Server logs cleaned."

stop-servers:
	@echo "Stopping servers and proxies..."
	@if [ -f src/server/server_logs/server_pids.txt ]; then \
		echo "Killing Servers..."; \
		xargs kill < src/server/server_logs/server_pids.txt 2>/dev/null || true; \
		rm src/server/server_logs/server_pids.txt; \
	fi
	@if [ -f src/proxy/proxy_logs/proxy_pids.txt ]; then \
		echo "Killing Proxies..."; \
		xargs kill < src/proxy/proxy_logs/proxy_pids.txt 2>/dev/null || true; \
		rm src/proxy/proxy_logs/proxy_pids.txt; \
	fi
	@echo "All background processes stopped."

clean-lists:
	rm -f $(SERVER_LOG_DIR)/known_servers.txt
	rm -f $(PROXY_LOG_DIR)/known_proxies.txt

clean: clean-logs stop-servers clean-lists clean-dbs
	@echo "Cleanup complete."

help:
	@echo "Available commands:"
	@echo "  make install           - Install python dependencies"
	@echo "  make clean-dbs         - Remove all PostgreSQL containers"
	@echo "  make init              - Initialize the system (db, servers, proxies)"
	@echo "  make client ID=<User_X>  - Run a client with identifier User_X"
	@echo "  make servers           - Run initial setup via admin tool"
	@echo "  make add-server        - Add a new server via admin tool"
	@echo "  make remove-server     - Remove a server via admin tool (use SERVER_NAME)"
	@echo "  make clean-logs        - Remove server log files"
	@echo "  make stop-servers      - Stop all running servers"
	@echo "  make clean             - Full cleanup (logs, servers, db)"
