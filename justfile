# Justfile for ArbiRich bot
set dotenv-load

# Variables
python := "poetry run python"
alembic := "poetry run alembic"
pytest := "poetry run pytest"
dev_db := "arbirich_db"
dev_user := "arbiuser"
REDIS_CONTAINER := "redis-arbirich"

# Default recipe to display help
default:
    @just --list

# Install dependencies using Poetry
install:
    poetry install --with dev --all-extras

# Format code using ruff and isort
format:
    poetry run ruff check . --select I --fix # sort imports
    poetry run ruff format .
    poetry run black .

# Run linter for python
lint:
    poetry run ruff check src tests

# Run tests
test:
    {{ pytest }} -v

# Run unit tests
u_test *arguments:
    poetry run pytest tests/units {{arguments}}

# Run integration tests
i_test *arguments:
    poetry run pytest tests/integrations {{arguments}}

# Run all checks
all: u_test i_test format lint

# Redis management with script
start-redis:
    ./scripts/redis-manager.sh start

stop-redis:
    ./scripts/redis-manager.sh stop

restart-redis:
    ./scripts/redis-manager.sh restart

check-redis:
    ./scripts/redis-manager.sh check

clean-redis:
    ./scripts/redis-manager.sh clean

# Setup local PostgreSQL database
setup-local-db:
    chmod +x ./scripts/create-local-db.sh
    ./scripts/create-local-db.sh

# Run the application (dev) with Redis check
run-bot: setup-local-db
    RUST_BACKTRACE=1 {{ python }} -m main

# Stop running ArbiRich application
stop-bot:
    {{ python }} -m src.arbirich.tools.stop_app

# Force kill ArbiRich processes (for when stop-bot doesn't work)
force-kill:
    {{ python }} -m src.arbirich.tools.force_kill

# Emergency abort - kills all ArbiRich processes forcibly (LAST RESORT)
abort:
    {{ python }} -m src.arbirich.tools.emergency_abort

# Check database connection
check-db:
    python scripts/check_db_connection.py

# Check database connection in Docker
docker-check-db:
    docker-compose run --rm app python scripts/check_db_connection.py

# Create database migrations
migrations message="auto":
    {{ alembic }} revision --autogenerate -m "{{ message }}"

# Apply database migrations
migrate:
    {{ alembic }} upgrade head

# Docker database reset command with volume cleanup
docker-reset-db:
    # Stop any running containers and remove the volume
    docker compose down -v
    # Recreate and start postgres container
    docker compose up -d postgres
    # Wait for Postgres to be ready
    chmod +x ./scripts/wait-for-postgres.sh
    ./scripts/wait-for-postgres.sh
    # Now that PostgreSQL is ready, reset the database
    # First check if container is running and get correct container ID/name
    docker ps | grep postgres-arbirich || echo "Container not running!"
    # Find which database we can connect to and use it to recreate the target database
    docker exec $(docker ps -q -f name=postgres-arbirich) sh -c 'for DB in postgres arbiuser template1; do echo "Trying $DB"; if psql -U arbiuser -d $DB -c "SELECT 1" >/dev/null 2>&1; then echo "Using $DB"; PGDATABASE=$DB; break; fi; done; echo "Dropping database if exists"; psql -U arbiuser -d $PGDATABASE -c "DROP DATABASE IF EXISTS arbirich_db;"; echo "Creating database"; psql -U arbiuser -d $PGDATABASE -c "CREATE DATABASE arbirich_db WITH OWNER arbiuser;"'
    # Run migrations
    docker compose run --rm migrate
    echo "Database reset complete!"
    
# Docker helper commands
docker-start:
    chmod +x ./scripts/docker-compose-helper.sh
    ./scripts/docker-compose-helper.sh start

docker-stop:
    chmod +x ./scripts/docker-compose-helper.sh
    ./scripts/docker-compose-helper.sh stop

docker-logs:
    chmod +x ./scripts/docker-compose-helper.sh
    ./scripts/docker-compose-helper.sh logs

# Connect to the database in Docker
docker-db-connect:
    docker exec -it postgres-arbirich psql -U arbiuser -d arbirich_db

# Export database schema
docker-db-schema:
    docker exec -it postgres-arbirich pg_dump -U arbiuser -d arbirich_db --schema-only > schema.sql

# Export database data (without schema)
docker-db-data:
    docker exec -it postgres-arbirich pg_dump -U arbiuser -d arbirich_db --data-only > data.sql

# Run a custom SQL query
docker-db-query query:
    echo "{{query}}" | docker exec -i postgres-arbirich psql -U arbiuser -d arbirich_db

# Clean up virtual environment and caches
clean-venv:
    rm -rf .venv
    rm -f poetry.lock
    poetry install

# Clean up __pycache__ files
clean-cache:
    find . -type d -name "__pycache__" -exec rm -rf {} +
    find . -name "*.pyc" -delete
    find . -name ".pytest_cache" -exec rm -rf {} +

# Clean everything
clean: clean-cache clean-venv