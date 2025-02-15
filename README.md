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

## Usage

Run the app with:

```sh
poetry run python main.py
```

You can access the Swagger App to play with the API at `http://0.0.0.0:8088/docs`
