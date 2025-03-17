# Justfile for ArbiRich bot
set dotenv-load

# Variables
python := "poetry run python"
alembic := "poetry run alembic"
pytest := "poetry run pytest"
dev_db := "arbidb"
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
    poetry run isort .

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

# Redis management
start-redis:
    docker run -d --rm --name {{REDIS_CONTAINER}} -p 6379:6379 redis:latest

stop-redis:
    @docker stop {{REDIS_CONTAINER}} || true

# Run the application (dev) with Redis check
run-bot:
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

# Run with redis lifecycle management
run: start-redis run-bot stop-redis

# Create database migrations
migrations message="auto":
    {{ alembic }} revision --autogenerate -m "{{ message }}"

# Apply database migrations
migrate:
    {{ alembic }} upgrade head

db-reset:
    poetry run python -m src.arbirich.tools.db_reset

# Docker commands
docker-up:
    docker-compose up

docker-down:
    docker-compose down

docker-run:
    docker-compose up --abort-on-container-exit

# Reset database in Docker (DROP and recreate - DEV ONLY)
docker-reset-db:
    echo "Starting necessary services..." && \
    docker-compose up -d postgres && \
    echo "Waiting for PostgreSQL to be ready..." && \
    sleep 5 && \
    docker-compose exec postgres dropdb -U {{ dev_user }} {{ dev_db }} --if-exists && \
    docker-compose exec postgres createdb -U {{ dev_user }} {{ dev_db }} && \
    echo "Running migrations..." && \
    docker-compose run --rm migrate && \
    echo "Starting app for prefill..." && \
    docker-compose up -d app && \
    sleep 5 && \
    docker-compose exec app {{ python }} -m src.arbirich.prefill_database && \
    echo "✅ Database reset and initialization complete!"

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
