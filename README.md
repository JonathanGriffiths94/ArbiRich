# ArbiRich
ArbiRich – Automated Arbitrage, Maximum Profit. 💰🚀

## Overview

This repo contains a FastAPI application to perform crypto arbitrage trading.

## Contributing 

When contributing please install pre-commit hooks with: 

```sh
pre-commit install
```

This will run ruff linting/formatting upon committing to ensure common formatting/linting across the project.

## Getting Started

### Prerequisites

The project requires :

- Python 3.12
- [Poetry](https://python-poetry.org/)

### Installation

1. Clone the repository:

   ```sh
   git clone https://github.com/JonathanGriffiths94/ArbiRich.git
   cd ArbiRich
   ```

2. Install dependencies using Poetry.

   ```sh
   poetry install --with dev
   ```

3. Set up your environment variables:

   ```sh
   cp .env.example .env
   ```

## Database Management

The application uses SQLAlchemy to interact with the database:

- `database_service.py` - Provides a DatabaseService class with methods for all database operations
- `prefill_database.py` - Script to initialize the database with required reference data (exchanges, pairs, and strategies)

To initialize the database:

```bash
python -m src.arbirich.prefill_database
```

## Usage

Run the app with:

```sh
poetry run python main.py
```

You can access the Swagger App to play with the API at `http://0.0.0.0:8088/docs`
