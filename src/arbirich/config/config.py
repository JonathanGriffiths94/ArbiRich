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
        "mapping": {},
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
        "mapping": {},
        "additional_info": {"connection_count": 1},
    },
    # "binance": {
    #     "name": "binance",
    #     "api_rate_limit": 1200,
    #     "trade_fees": 0.001,
    #     "rest_url": "https://api.binance.com",
    #     "ws_url": "wss://stream.binance.com:9443/ws",
    #     "delimiter": "",
    #     "withdrawal_fee": {"BTC": 0.0005, "ETH": 0.005, "USDT": 1.0},
    #     "api_response_time": 50,
    #     "mapping": {"USDT": "USDT"},
    #     "additional_info": {"connection_count": 2},
    # },
}

# Use only active exchanges for the primary configuration
EXCHANGES = {
    "bybit": ALL_EXCHANGES["bybit"],
    "cryptocom": ALL_EXCHANGES["cryptocom"],
}

# Trading pairs configuration - Updated with mid-cap cryptos
ALL_PAIRS = {
    "ATOM-USDT": {
        "base_currency": "ATOM",
        "quote_currency": "USDT",
    },
    "LINK-USDT": {
        "base_currency": "LINK",
        "quote_currency": "USDT",
    },
    "AVAX-USDT": {
        "base_currency": "AVAX",
        "quote_currency": "USDT",
    },
    "MATIC-USDT": {
        "base_currency": "MATIC",
        "quote_currency": "USDT",
    },
    "DOT-USDT": {
        "base_currency": "DOT",
        "quote_currency": "USDT",
    },
    "NEAR-USDT": {
        "base_currency": "NEAR",
        "quote_currency": "USDT",
    },
    "FTM-USDT": {
        "base_currency": "FTM",
        "quote_currency": "USDT",
    },
    "ALGO-USDT": {
        "base_currency": "ALGO",
        "quote_currency": "USDT",
    },
}

# Trading pairs - Updated with mid-cap cryptos
PAIRS = [
    ("ATOM", "USDT"),  # Cosmos
    ("LINK", "USDT"),  # Chainlink
    ("AVAX", "USDT"),  # Avalanche
    ("MATIC", "USDT"),  # Polygon
    ("DOT", "USDT"),  # Polkadot
    ("NEAR", "USDT"),  # NEAR Protocol
    ("FTM", "USDT"),  # Fantom
    ("ALGO", "USDT"),  # Algorand
]

# Strategy configurations - Updated with new pairs
ALL_STRATEGIES = {
    "basic_arbitrage": {
        "type": "basic",
        "starting_capital": 10000.0,
        "min_spread": 0.0001,
        "threshold": 0.0001,
        "exchanges": ["bybit", "cryptocom"],
        "pairs": [("LINK", "USDT"), ("MATIC", "USDT")],  # Updated pairs
        "risk_management": {
            "max_position_size": 50.0,  # In USDT equivalent
            "max_daily_loss": 5.0,  # Percentage of capital
            "max_consecutive_losses": 3,
            "circuit_breaker_cooldown": 3600,  # 1 hour in seconds
            "scale_by_spread": True,
        },
        "execution": {
            "method": "parallel",  # Could be "parallel" or "staggered"
            "timeout": 3000,  # Timeout in milliseconds
            "retry_attempts": 2,
            "max_slippage": 0.0005,
        },
        "additional_info": {
            "min_volume": 10.0,  # Adjusted for mid-cap tokens
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
        "pairs": [("ATOM", "USDT"), ("DOT", "USDT")],  # Updated pairs
        "risk_management": {
            "max_position_size": 40.0,  # In USDT equivalent
            "max_daily_loss": 3.0,  # Percentage of capital
            "max_consecutive_losses": 2,
            "circuit_breaker_cooldown": 1800,  # 30 minutes in seconds
            "scale_by_spread": True,
        },
        "execution": {
            "method": "staggered",  # Staggered execution for more careful approach
            "timeout": 5000,  # Longer timeout for staggered execution
            "retry_attempts": 3,
            "max_slippage": 0.0003,
            "stagger_delay": 500,  # Milliseconds between legs
        },
        "additional_info": {
            "min_depth": 10,
            "max_slippage": 0.0003,
            "execution_delay": 0.2,
        },
    },
    "volume_adjusted_arbitrage": {
        "type": "volume_adjusted",
        "starting_capital": 15000.0,
        "min_spread": 0.0002,  # Higher spread requirement due to depth analysis
        "threshold": 0.0002,
        "target_volume": 100.0,  # Target volume in USDT equivalent for weighted calculation
        "min_depth_percentage": 0.7,  # Minimum % of target volume that must be available
        "exchanges": ["bybit", "cryptocom"],
        "pairs": [("AVAX", "USDT"), ("NEAR", "USDT"), ("FTM", "USDT"), ("ALGO", "USDT")],  # Updated pairs
        "risk_management": {
            "max_position_size": 60.0,  # In USDT equivalent
            "max_daily_loss": 4.0,
            "max_consecutive_losses": 3,
            "circuit_breaker_cooldown": 2700,  # 45 minutes in seconds
            "scale_by_spread": True,
            "exchange_risk_factors": {
                "bybit": 0.9,  # Risk factor for bybit (1.0 = full trust)
                "cryptocom": 0.8,  # Risk factor for crypto.com
            },
        },
        "execution": {"method": "parallel", "timeout": 4000, "retry_attempts": 2, "max_slippage": 0.0004},
        "additional_info": {
            "min_volume": 15.0,  # Adjusted for mid-cap tokens
            "liquidity_factor": 0.8,  # Reduce volume based on liquidity
            "depth_scaling": True,  # Scale position size based on order book depth
        },
    },
}

# Active strategies for trading
STRATEGIES = {
    "basic_arbitrage": ALL_STRATEGIES["basic_arbitrage"],
    "mid_price_arbitrage": ALL_STRATEGIES["mid_price_arbitrage"],
    "volume_adjusted_arbitrage": ALL_STRATEGIES["volume_adjusted_arbitrage"],
}

# Execution method configurations
EXECUTION_METHODS = {
    "parallel": {
        "timeout": 3000,  # Milliseconds
        "retry_attempts": 2,
        "cleanup_failed_trades": True,
    },
    "staggered": {
        "timeout": 5000,  # Milliseconds
        "retry_attempts": 3,
        "stagger_delay": 500,  # Milliseconds between trade legs
        "abort_on_first_failure": True,
    },
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
        pair_symbol: Symbol of the trading pair (e.g., 'ATOM-USDT')

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


def get_execution_method_config(method_name):
    """
    Get configuration for a specific execution method.

    Args:
        method_name: Name of the execution method

    Returns:
        Dict containing execution method configuration or None if not found
    """
    if method_name in EXECUTION_METHODS:
        return EXECUTION_METHODS[method_name]

    # Not found
    return None
