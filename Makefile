# --- Variables ---
PYTHON = python3
SRC_DIR = src
DB_CONTAINER_NAME = shopping-db
DB_PASSWORD = password
DB_PORT = 5432
LOG_DIR = $(SRC_DIR)/server/server_logs
BASE_PORT = 5555

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

# --- RUNNING SERVERS ---
servers:
	mkdir -p $(LOG_DIR)
	@# Clean old server logs before starting
	$(MAKE) clean-logs
	@# Start Server_1
	PYTHONPATH=$(PWD) $(PYTHON) -m src.server.main --id "Server_1" --seed "True" --port "5555" > $(LOG_DIR)/server1.log 2>&1 &
	@# Start Server_2
	PYTHONPATH=$(PWD) $(PYTHON) -m src.server.main --id "Server_2" --seed "False" --port "5556" --known_server_port "5555" > $(LOG_DIR)/server2.log 2>&1 &
	@echo "Server_1 and Server_2 started in background. Logs in $(LOG_DIR)."

additional_server:
	mkdir -p $(LOG_DIR)
	@# Count existing servers, but start from 3 if logs were cleaned
	@LAST_NUM=$$(ls $(LOG_DIR)/server*.log 2>/dev/null | wc -l); \
	if [ $$LAST_NUM -lt 2 ]; then \
		NEXT_NUM=3; \
	else \
		NEXT_NUM=$$((LAST_NUM + 1)); \
	fi; \
	NEXT_PORT=$$((5554 + NEXT_NUM)); \
	LOG_FILE="$(LOG_DIR)/server$$NEXT_NUM.log"; \
	echo "Starting Server_$$NEXT_NUM on port $$NEXT_PORT, logging to $$LOG_FILE"; \
	PYTHONPATH=$(PWD) $(PYTHON) -m src.server.main --id "Server_$$NEXT_NUM" --seed "False" --port "$$NEXT_PORT" --known_server_port "5555" > $$LOG_FILE 2>&1 &

clean-logs:
	rm -f $(LOG_DIR)/server*.log
	@echo "Server logs cleaned."


stop-servers:
	@pkill -f "python3 -m src.server.main" || true
	@echo "All servers stopped."

# --- CLEANUP ---
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
	@echo "  make servers           - Start Server_1 and Server_2 in background"
	@echo "  make additional_server - Add a new server in background"
	@echo "  make clean-logs        - Remove server log files"
	@echo "  make stop-servers      - Stop all running servers"
	@echo "  make clean             - Remove Python cache"
