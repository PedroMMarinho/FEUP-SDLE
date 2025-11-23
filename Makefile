# Variables
PYTHON = python3
SRC_DIR = src
DB_CONTAINER_NAME = shopping-db
DB_PASSWORD = password
DB_PORT = 5432

all: help

# --- SETUP & INFRASTRUCTURE ---

install:
	pip install -r requirements.txt

# This command spins up Postgres in Docker
db:
	@echo "Checking for database container..."
	@# Try to start existing container. If it fails (doesn't exist), run a new one.
	@docker start $(DB_CONTAINER_NAME) 2>/dev/null || \
	docker run --name $(DB_CONTAINER_NAME) \
		-e POSTGRES_PASSWORD=$(DB_PASSWORD) \
		-p $(DB_PORT):5432 \
		-d postgres
	@echo "Waiting for Postgres to be ready..."
	@sleep 3  # Give it a moment to boot up
	@echo "Database is running!"

# Stop the Docker container
stop-db:
	@docker stop $(DB_CONTAINER_NAME)
	@echo "Database stopped."

# Remove the Docker container (clean slate)
clean-db: stop-db
	@docker rm $(DB_CONTAINER_NAME)
	@echo "Database container removed."

# --- RUNNING CLIENTS ---

# Run Client 1 (User A)
# Connects to localhost:5432 (mapped to Docker)
client1:
	export PYTHONPATH=$(PWD) && $(PYTHON) -m src.client.main --id "User_A" --db "client_A.db"

# Run Client 2 (User B)
client2:
	export PYTHONPATH=$(PWD) && $(PYTHON) -m src.client.main --id "User_B" --db "client_B.db"

# --- CLEANUP ---
clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	@echo "Python cache cleaned."

help:
	@echo "Available commands:"
	@echo "  make install    - Install python dependencies"
	@echo "  make db         - Start PostgreSQL in Docker"
	@echo "  make stop-db    - Stop PostgreSQL"
	@echo "  make client1    - Run Client A"
	@echo "  make client2    - Run Client B"