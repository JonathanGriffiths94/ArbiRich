# Justfile for ArbiRich bot

# Install dependencies using Poetry
install:
    poetry install

# Run the bot
run:
    poetry run python src/arbirich/main.py

# Run unit tests
u_test:
    poetry run pytest tests/units/

# Run integration tests
i_test:
    poetry run pytest tests/integrations/

# Format code with black
format:
    poetry run black src/

# Lint code with ruff
lint:
    poetry run ruff src/

# Run all jobs
all: u_test i_test format lint


# Clean up virtual environment and poetry lock file
clean:
    rm -rf .venv
    rm poetry.lock
    poetry install
