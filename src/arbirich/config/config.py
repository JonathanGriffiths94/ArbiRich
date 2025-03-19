import os

from dotenv import load_dotenv

from src.arbirich.services.exchange_configs import EXCHANGE_CONFIGS
from src.arbirich.services.strategies.strategy_configs import (
    get_active_strategies,
    get_unique_exchanges,
    get_unique_pairs,
)

load_dotenv()

# Redis configuration
REDIS_CONFIG = {
    "host": os.getenv("REDIS_HOST", "localhost"),
    "port": int(os.getenv("REDIS_PORT", "6379")),
    "db": int(os.getenv("REDIS_DB", "0")),
}

# Database configuration
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_USER = os.getenv("POSTGRES_USER", "arbiuser")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "arbipassword")
POSTGRES_DB = os.getenv("POSTGRES_DB", "arbidb")

DATABASE_URL = f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# Strategy selection - can be a single strategy name or a comma-separated list
STRATEGY_SELECTION = os.getenv("STRATEGY_SELECTION", "basic_arbitrage")

# Get active strategies based on selection
STRATEGIES = get_active_strategies(STRATEGY_SELECTION)

# Gather unique pairs from all active strategies
PAIRS = get_unique_pairs(STRATEGIES)

# Gather unique exchanges from all active strategies
EXCHANGES = get_unique_exchanges(STRATEGIES)

# Exchange configuration
EXCHANGE_CONFIGS = EXCHANGE_CONFIGS

# API configuration
API_PORT = int(os.getenv("API_PORT", "8088"))
API_HOST = os.getenv("API_HOST", "0.0.0.0")

# Debug mode
DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "t", "yes")

# Log level configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
