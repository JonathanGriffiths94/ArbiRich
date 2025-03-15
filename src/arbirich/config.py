import os
from typing import Any, Dict

from dotenv import load_dotenv

from src.arbirich.exchange_configs import EXCHANGE_CONFIGS

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

# Define all available strategies
ALL_STRATEGIES: Dict[str, Dict[str, Any]] = {
    "basic_arbitrage": {
        "starting_capital": 10000.0,
        "min_spread": 0.0001,  # 0.1%
        "threshold": 0.0001,  # Minimum spread to consider for arbitrage
        "exchanges": ["bybit", "cryptocom"],
        "pairs": [("BTC", "USDT")],
        "additional_info": {"description": "Basic arbitrage strategy looking for price differences between exchanges"},
    },
    "ETH_arbitrage": {
        "starting_capital": 5000.0,
        "min_spread": 0.002,  # 0.2%
        "threshold": 0.002,  # Higher threshold for ETH
        "exchanges": ["bybit", "cryptocom"],
        "pairs": [("ETH", "USDT")],
        "additional_info": {"description": "Specialized arbitrage strategy for ETH/USDT"},
    },
    "multi_asset_arbitrage": {
        "starting_capital": 20000.0,
        "min_spread": 0.0015,  # 0.15%
        "threshold": 0.0015,
        "exchanges": ["bybit", "cryptocom"],
        "pairs": [("BTC", "USDT"), ("ETH", "USDT"), ("SOL", "USDT")],
        "additional_info": {
            "description": "Multi-asset arbitrage strategy that monitors opportunities across several pairs"
        },
    },
}

# Parse strategy selection - can be a single strategy or multiple comma-separated strategies
strategy_names = [name.strip() for name in STRATEGY_SELECTION.split(",")]

# Create a dictionary of active strategies
STRATEGIES = {}
for name in strategy_names:
    if name in ALL_STRATEGIES:
        STRATEGIES[name] = ALL_STRATEGIES[name]
    else:
        print(f"Warning: Strategy '{name}' not found in ALL_STRATEGIES, ignoring.")

# If no valid strategies were selected, use the first available strategy
if not STRATEGIES:
    first_strategy = next(iter(ALL_STRATEGIES.keys()))
    print(f"No valid strategies selected, defaulting to '{first_strategy}'")
    STRATEGIES[first_strategy] = ALL_STRATEGIES[first_strategy]

# Gather unique pairs from all active strategies
PAIRS = []
for strategy in STRATEGIES.values():
    for pair in strategy["pairs"]:
        if pair not in PAIRS:
            PAIRS.append(pair)

# Gather unique exchanges from all active strategies
EXCHANGES = []
for strategy in STRATEGIES.values():
    for exchange in strategy["exchanges"]:
        if exchange not in EXCHANGES:
            EXCHANGES.append(exchange)

# Exchange configuration
EXCHANGE_CONFIGS = EXCHANGE_CONFIGS

# API configuration
API_PORT = int(os.getenv("API_PORT", "8088"))
API_HOST = os.getenv("API_HOST", "0.0.0.0")

# Debug mode
DEBUG = os.getenv("DEBUG", "false").lower() in ("true", "1", "t", "yes")

# Log level configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
