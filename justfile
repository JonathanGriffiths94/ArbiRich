# Justfile for ArbiRich bot
set dotenv-load

# Install dependencies using Poetry
install:
    poetry install --with dev --all-extras

# Run the bot
REDIS_CONTAINER := "redis-arbirich"

start-redis:
    docker run -d --rm --name {{REDIS_CONTAINER}} -p 6379:6379 redis:latest

stop-redis:
    docker stop {{REDIS_CONTAINER}}

run-bot:
    RUST_BACKTRACE=full poetry run python main.py

# A recipe that starts Redis, runs the bot, then stops Redis.
run: start-redis run-bot stop-redis


format_all:
    poetry run ruff check . --select I --fix # sort imports
    poetry run ruff format .

# Run linter for python
lint:
    poetry run ruff check src tests

# Run unit test
u_test *arguments:
  poetry run pytest tests/units {{arguments}}

# Run integration test
i_test *arguments:
  poetry run pytest tests/integrations {{arguments}}

# Run all jobs
all: u_test i_test format_all lint

# Clean up virtual environment and poetry lock file
clean:
    rm -rf .venv
    rm poetry.lock
    poetry install
