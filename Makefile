# --- Variables ---
PYTHON = venv/bin/python3
SRC_DIR = src
DB_CONTAINER_NAME = shopping-db
DB_PASSWORD = password
DB_PORT = 5432
SERVER_LOG_DIR = $(SRC_DIR)/server/server_logs
PROXY_LOG_DIR = $(SRC_DIR)/proxy/proxy_logs
ID ?= User_Default

all: help

# --- SETUP & INFRASTRUCTURE ---
init: db servers
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

db:
	@echo "Checking for database container..."
	@docker start $(DB_CONTAINER_NAME) 2>/dev/null || \
	docker run --name $(DB_CONTAINER_NAME) \
		-e POSTGRES_PASSWORD=$(DB_PASSWORD) \
		-p $(DB_PORT):5432 \
		-d postgres
	@echo "Waiting for Postgres to be ready..."
	@sleep 5
	@echo "Database is running!"

stop-db:
	@docker stop $(DB_CONTAINER_NAME)
	@echo "Database stopped."

clean-db: stop-db
	@docker rm $(DB_CONTAINER_NAME)
	@echo "Database container removed."

# --- RUNNING CLIENTS ---
# Usage: make client ID=User_A

client:
	PYTHONPATH=$(PWD) $(PYTHON) -m src.client.main \
		--id "$(ID)" \
		--db "client_$(ID).db" \
		--proxies $(PROXY_LOG_DIR)/known_proxies.txt

# --- RUNNING SERVERS VIA ADMIN TOOL ---
servers:
	mkdir -p $(SERVER_LOG_DIR) $(PROXY_LOG_DIR)
	PYTHONPATH=$(PWD) $(PYTHON) -m  src.admin.main --action initial_setup

add-server:
	mkdir -p $(SERVER_LOG_DIR)
	PYTHONPATH=$(PWD) $(PYTHON) -m src.admin.main --action add_server

remove-server:
	@if [ -z "$(SERVER_NAME)" ]; then \
		echo "ERROR: Please provide SERVER_NAME, e.g. make remove_server SERVER_NAME=Server_3"; \
		exit 1; \
	fi
	PYTHONPATH=$(PWD) $(PYTHON) src.admin.main --action remove_server --server_name $(SERVER_NAME)

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

clean: clean-logs stop-servers clean-lists clean-db
	@echo "Cleanup complete."

help:
	@echo "Available commands:"
	@echo "  make install           - Install python dependencies"
	@echo "  make db                - Start PostgreSQL in Docker"
	@echo "  make stop-db           - Stop PostgreSQL"
	@echo "  make client1           - Run Client A"
	@echo "  make client2           - Run Client B"
	@echo "  make servers           - Run initial setup via admin tool"
	@echo "  make add_server        - Add a new server via admin tool"
	@echo "  make remove_server     - Remove a server via admin tool (use SERVER_NAME)"
	@echo "  make clean-logs        - Remove server log files"
	@echo "  make stop-servers      - Stop all running servers"
	@echo "  make clean             - Remove Python cache"
