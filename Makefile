# --- Variables ---
PYTHON = python3
SRC_DIR = src
DB_CONTAINER_NAME = shopping-db
DB_PASSWORD = password
DB_PORT = 5432
SERVER_LOG_DIR = $(SRC_DIR)/server/server_logs
PROXY_LOG_DIR = $(SRC_DIR)/proxy/proxy_logs

all: help

# --- SETUP & INFRASTRUCTURE ---

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
	@sleep 3
	@echo "Database is running!"

stop-db:
	@docker stop $(DB_CONTAINER_NAME)
	@echo "Database stopped."

clean-db: stop-db
	@docker rm $(DB_CONTAINER_NAME)
	@echo "Database container removed."

# --- RUNNING CLIENTS ---
client1:
	PYTHONPATH=$(PWD) $(PYTHON) -m src.client.main --id "User_A" --db "client_A.db"

client2:
	PYTHONPATH=$(PWD) $(PYTHON) -m src.client.main --id "User_B" --db "client_B.db"

# --- RUNNING SERVERS VIA ADMIN TOOL ---
servers:
	mkdir -p $(SERVER_LOG_DIR) $(PROXY_LOG_DIR)
	PYTHONPATH=$(PWD) $(PYTHON) src.admin.main --action initial_setup

add_server:
	mkdir -p $(SERVER_LOG_DIR)
	PYTHONPATH=$(PWD) $(PYTHON) src.admin.main --action add_server

remove_server:
	@if [ -z "$(SERVER_NAME)" ]; then \
		echo "ERROR: Please provide SERVER_NAME, e.g. make remove_server SERVER_NAME=Server_3"; \
		exit 1; \
	fi
	PYTHONPATH=$(PWD) $(PYTHON) src.admin.main --action remove_server --server_name $(SERVER_NAME)

# --- CLEANUP ---
clean-logs:
	rm -f $(SERVER_LOG_DIR)/server*.log
	@echo "Server logs cleaned."

stop-servers:
	@pkill -f "python3 -m src.server.main" || true
	@echo "All servers stopped."

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	@echo "Python cache cleaned."

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
