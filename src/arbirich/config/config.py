"""Configuration for the arbitrage system"""

import os

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database configuration
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/arbirich_db")

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# Construct REDIS_URL from the Redis settings
if REDIS_PASSWORD:
    REDIS_URL = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"
else:
    REDIS_URL = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}"

# Create REDIS_CONFIG dictionary for the RedisService class
REDIS_CONFIG = {"host": REDIS_HOST, "port": REDIS_PORT, "db": REDIS_DB, "password": REDIS_PASSWORD, "url": REDIS_URL}

# Exchanges configuration
ALL_EXCHANGES = {
    "bybit": {
        "name": "bybit",
        "api_rate_limit": 100,
        "trade_fees": 0.001,
        "rest_url": "https://api.bybit.com",
        "ws_url": "wss://stream.bybit.com/v5/public/spot",
        "delimiter": "",
        "withdrawal_fee": {"BTC": 0.0005, "ETH": 0.01, "USDT": 1.0},
        "api_response_time": 100,
        "mapping": {"USDT": "USDT"},
        "additional_info": {"connection_count": 1},
    },
    "cryptocom": {
        "name": "cryptocom",
        "api_rate_limit": 100,
        "trade_fees": 0.001,
        "rest_url": "https://api.crypto.com",
        "ws_url": "wss://stream.crypto.com/v2/market",
        "delimiter": "_",
        "withdrawal_fee": {"BTC": 0.0004, "ETH": 0.008, "USDT": 1.0},
        "api_response_time": 200,
        "mapping": {"USDT": "USDT"},
        "additional_info": {"connection_count": 1},
    },
    "binance": {
        "name": "binance",
        "api_rate_limit": 1200,
        "trade_fees": 0.001,
        "rest_url": "https://api.binance.com",
        "ws_url": "wss://stream.binance.com:9443/ws",
        "delimiter": "",
        "withdrawal_fee": {"BTC": 0.0005, "ETH": 0.005, "USDT": 1.0},
        "api_response_time": 50,
        "mapping": {"USDT": "USDT"},
        "additional_info": {"connection_count": 2},
    },
}

# Use only active exchanges for the primary configuration
EXCHANGES = {
    "bybit": ALL_EXCHANGES["bybit"],
    "cryptocom": ALL_EXCHANGES["cryptocom"],
}

# Trading pairs configuration
ALL_PAIRS = {
    "BTC-USDT": {
        "base_currency": "BTC",
        "quote_currency": "USDT",
    },
    "ETH-USDT": {
        "base_currency": "ETH",
        "quote_currency": "USDT",
    },
    "SOL-USDT": {
        "base_currency": "SOL",
        "quote_currency": "USDT",
    },
    "BNB-USDT": {
        "base_currency": "BNB",
        "quote_currency": "USDT",
    },
    "ETH-BTC": {
        "base_currency": "ETH",
        "quote_currency": "BTC",
    },
}

# Active trading pairs
PAIRS = [
    ("BTC", "USDT"),
    ("ETH", "USDT"),
    ("SOL", "USDT"),
    ("BNB", "USDT"),
    ("ETH", "BTC"),
]

# Strategy configurations
ALL_STRATEGIES = {
    "basic_arbitrage": {
        "type": "basic",
        "starting_capital": 10000.0,
        "min_spread": 0.0001,
        "threshold": 0.0001,
        "exchanges": ["bybit", "cryptocom"],
        "pairs": [("BTC", "USDT")],
        "additional_info": {
            "min_volume": 0.001,
            "max_slippage": 0.0005,
            "execution_delay": 0.1,
        },
    },
    "mid_price_arbitrage": {
        "type": "mid_price",
        "starting_capital": 5000.0,
        "min_spread": 0.0001,
        "threshold": 0.0001,
        "exchanges": ["bybit", "cryptocom"],
        "pairs": [("ETH", "USDT"), ("SOL", "USDT")],
        "additional_info": {
            "min_depth": 10,
            "max_slippage": 0.0003,
            "execution_delay": 0.2,
        },
    },
    "high_frequency_arbitrage": {
        "type": "hft",
        "starting_capital": 20000.0,
        "min_spread": 0.0001,  # Suitable threshold for HFT
        "threshold": 0.0001,
        "exchanges": ["bybit", "cryptocom"],  # More exchanges for HFT
        "pairs": [("BTC", "USDT"), ("ETH", "USDT"), ("SOL", "USDT")],
        "additional_info": {
            "min_volume": 0.0015,
            "max_slippage": 0.0002,
            "execution_delay": 0.05,  # Lower execution delay for HFT
            "price_change_threshold": 0.0001,  # Detect small price movements
            "history_size": 15,  # Track more price points
            "cooldown_seconds": 0.5,  # Shorter cooldown between trades
        },
    },
}

# Active strategies for trading
STRATEGIES = {
    "basic_arbitrage": ALL_STRATEGIES["basic_arbitrage"],
    "mid_price_arbitrage": ALL_STRATEGIES["mid_price_arbitrage"],
    "high_frequency_arbitrage": ALL_STRATEGIES["high_frequency_arbitrage"],
}


# Helper functions
def get_all_exchange_names():
    return list(ALL_EXCHANGES.keys())


def get_all_pair_symbols():
    return list(ALL_PAIRS.keys())


def get_all_strategy_names():
    return list(ALL_STRATEGIES.keys())


def get_exchange_config(exchange_name):
    """
    Get configuration for a specific exchange.

    Args:
        exchange_name: Name of the exchange

    Returns:
        Dict containing exchange configuration or None if not found
    """
    # Check in EXCHANGES first (active exchanges)
    if exchange_name in EXCHANGES:
        return EXCHANGES[exchange_name]

    # Then check in ALL_EXCHANGES
    if exchange_name in ALL_EXCHANGES:
        return ALL_EXCHANGES[exchange_name]

    # Not found
    return None


def get_pair_config(pair_symbol):
    """
    Get configuration for a specific trading pair.

    Args:
        pair_symbol: Symbol of the trading pair (e.g., 'BTC-USDT')

    Returns:
        Dict containing pair configuration or None if not found
    """
    # Check in ALL_PAIRS
    if pair_symbol in ALL_PAIRS:
        return ALL_PAIRS[pair_symbol]

    # Not found by symbol, try constructing from currencies
    for base, quote in PAIRS:
        constructed_symbol = f"{base}-{quote}"
        if constructed_symbol == pair_symbol:
            # If found in PAIRS but not in ALL_PAIRS, create a config
            return {"base_currency": base, "quote_currency": quote, "symbol": constructed_symbol}

    # Not found
    return None


def get_strategy_config(strategy_name):
    """
    Get configuration for a specific strategy.

    Args:
        strategy_name: Name of the strategy

    Returns:
        Dict containing strategy configuration or None if not found
    """
    # Check in STRATEGIES first (active strategies)
    if strategy_name in STRATEGIES:
        return STRATEGIES[strategy_name]

    # Then check in ALL_STRATEGIES
    if strategy_name in ALL_STRATEGIES:
        return ALL_STRATEGIES[strategy_name]

    # Not found
    return None
