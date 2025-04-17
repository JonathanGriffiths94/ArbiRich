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
        "api_key": None,
        "api_secret": None,
        "enabled": True,
        "paper_trading": True,
        "fetch_fees_on_startup": True,
        "dynamic_fee_updates": True,
        "fee_update_interval": 3600,
        "api_endpoints": {
            "trading_fees": "/spot/v3/private/account",
            "withdrawal_fees": "/asset/v3/private/coin-info/query",
            "trading_rules": "/spot/v3/public/symbols",
            "symbols": "/spot/v3/public/symbols",
        },
        "api_timeouts": {"default": 5000, "fees": 10000, "trades": 3000, "orders": 3000},
        "rate_limits": {
            "public": {"requests_per_minute": 120},
            "private": {"requests_per_minute": 60},
            "orders": {"requests_per_minute": 30},
        },
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
        "api_key": None,
        "api_secret": None,
        "enabled": True,
        "paper_trading": True,
        "fetch_fees_on_startup": True,
        "dynamic_fee_updates": True,
        "fee_update_interval": 3600,
        "api_endpoints": {
            "trading_fees": "/api/v1/private/account",
            "withdrawal_fees": "/api/v1/private/coin-info",
            "trading_rules": "/api/v1/public/symbols",
            "symbols": "/api/v1/public/symbols",
        },
        "api_timeouts": {"default": 5000, "fees": 10000, "trades": 3000, "orders": 3000},
        "rate_limits": {
            "public": {"requests_per_minute": 100},
            "private": {"requests_per_minute": 50},
            "orders": {"requests_per_minute": 25},
        },
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
        "api_key": None,
        "api_secret": None,
        "enabled": True,
        "paper_trading": True,
        "fetch_fees_on_startup": True,
        "dynamic_fee_updates": True,
        "fee_update_interval": 3600,
        "api_endpoints": {
            "trading_fees": "/api/v3/account",
            "withdrawal_fees": "/sapi/v1/capital/config/getall",
            "trading_rules": "/api/v3/exchangeInfo",
            "symbols": "/api/v3/exchangeInfo",
        },
        "api_timeouts": {"default": 5000, "fees": 10000, "trades": 3000, "orders": 3000},
        "rate_limits": {
            "public": {"requests_per_minute": 1200},
            "private": {"requests_per_minute": 600},
            "orders": {"requests_per_minute": 100},
        },
    },
}

# Use only active exchanges for the primary configuration
EXCHANGES = {
    "bybit": ALL_EXCHANGES["bybit"],
    "cryptocom": ALL_EXCHANGES["cryptocom"],
}

# Trading pairs configuration - Updated with mid-cap cryptos
ALL_TRADING_PAIRS = {
    "ATOM-USDT": {
        "base_currency": "ATOM",
        "quote_currency": "USDT",
        "min_qty": 0.0,
        "max_qty": 0.0,
        "price_precision": 8,
        "qty_precision": 8,
        "min_notional": 0.0,
        "enabled": True,
    },
    "LINK-USDT": {
        "base_currency": "LINK",
        "quote_currency": "USDT",
        "min_qty": 0.0,
        "max_qty": 0.0,
        "price_precision": 8,
        "qty_precision": 8,
        "min_notional": 0.0,
        "enabled": True,
    },
    "AVAX-USDT": {
        "base_currency": "AVAX",
        "quote_currency": "USDT",
        "min_qty": 0.0,
        "max_qty": 0.0,
        "price_precision": 8,
        "qty_precision": 8,
        "min_notional": 0.0,
        "enabled": True,
    },
    "MATIC-USDT": {
        "base_currency": "MATIC",
        "quote_currency": "USDT",
        "min_qty": 0.0,
        "max_qty": 0.0,
        "price_precision": 8,
        "qty_precision": 8,
        "min_notional": 0.0,
        "enabled": True,
    },
    "DOT-USDT": {
        "base_currency": "DOT",
        "quote_currency": "USDT",
        "min_qty": 0.0,
        "max_qty": 0.0,
        "price_precision": 8,
        "qty_precision": 8,
        "min_notional": 0.0,
        "enabled": True,
    },
    "NEAR-USDT": {
        "base_currency": "NEAR",
        "quote_currency": "USDT",
        "min_qty": 0.0,
        "max_qty": 0.0,
        "price_precision": 8,
        "qty_precision": 8,
        "min_notional": 0.0,
        "enabled": True,
    },
    "FTM-USDT": {
        "base_currency": "FTM",
        "quote_currency": "USDT",
        "min_qty": 0.0,
        "max_qty": 0.0,
        "price_precision": 8,
        "qty_precision": 8,
        "min_notional": 0.0,
        "enabled": True,
    },
    "ALGO-USDT": {
        "base_currency": "ALGO",
        "quote_currency": "USDT",
        "min_qty": 0.0,
        "max_qty": 0.0,
        "price_precision": 8,
        "qty_precision": 8,
        "min_notional": 0.0,
        "enabled": True,
    },
}

# Active trading pairs - Updated with mid-cap cryptos
TRADING_PAIRS = [
    ("ATOM", "USDT"),  # Cosmos
    ("LINK", "USDT"),  # Chainlink
    ("AVAX", "USDT"),  # Avalanche
    ("MATIC", "USDT"),  # Polygon
    ("DOT", "USDT"),  # Polkadot
    ("NEAR", "USDT"),  # NEAR Protocol
    ("FTM", "USDT"),  # Fantom
    ("ALGO", "USDT"),  # Algorand
]

# Map strategy types to default risk profiles
STRATEGY_RISK_PROFILES = {
    "basic": "moderate",
    "mid_price": "conservative",
    "vwap": "aggressive",
    "liquidity_adjusted": "market_maker",
}

# Map strategy types to default execution methods
STRATEGY_EXECUTION_METHODS = {
    "basic": "parallel",
    "mid_price": "staggered",
    "vwap": "risk_aware",
    "liquidity_adjusted": "risk_aware",
}

# Execution methods
EXECUTION_METHODS = {
    "parallel": {
        "name": "parallel",
        "description": "Execute trades simultaneously across exchanges",
        "type": "parallel",
        "method": "parallel",
        "retry_attempts": 2,
        "cleanup_failed_trades": True,
        "stagger_delay": 500,
        "abort_on_first_failure": True,
        "leg_order": "buy_first",
        "timeout": 3000,
        "retry_delay": 200,
        "max_slippage": 0.0005,
        "max_concurrent_trades": 3,
        "execution_timeout": 10.0,
    },
    "staggered": {
        "name": "staggered",
        "description": "Execute trades sequentially with configurable delays",
        "type": "staggered",
        "method": "staggered",
        "retry_attempts": 3,
        "cleanup_failed_trades": True,
        "stagger_delay": 500,
        "abort_on_first_failure": True,
        "leg_order": "buy_first",
        "timeout": 5000,
        "retry_delay": 200,
        "max_slippage": 0.0005,
        "execution_order": "buy_first",
        "delay_between_orders": 0.5,
    },
    "risk_aware": {
        "name": "risk_aware",
        "description": "Risk-aware execution with additional validations",
        "type": "risk_aware",
        "method": "risk_aware",
        "retry_attempts": 2,
        "cleanup_failed_trades": True,
        "stagger_delay": 500,
        "abort_on_first_failure": True,
        "leg_order": "buy_first",
        "timeout": 3000,
        "retry_delay": 200,
        "max_slippage": 0.0005,
        "base_execution_type": "parallel",
        "max_slippage_tolerance": 0.001,
        "risk_check_timeout": 1.0,
        "enable_pre_trade_validation": True,
        "enable_post_trade_analysis": True,
    },
}

# Strategy configurations - Updated with new pairs and execution methods
ALL_STRATEGIES = {
    "basic_arbitrage": {
        "type": "basic",
        "name": "basic_arbitrage",
        "starting_capital": 10000.0,
        "min_spread": 0.0001,
        "threshold": 0.0001,
        "exchanges": ["bybit", "cryptocom"],
        "pairs": [("LINK", "USDT")],
        "risk_profile": "moderate",  # Reference to risk profile
        "execution_method": "parallel",  # Reference to execution method
        "execution_params": {  # Optional overrides
            "timeout": 3000,
            "retry_attempts": 2,
            "max_slippage": 0.0005,
        },
        "additional_info": {
            "min_volume": 10.0,
            "execution_delay": 0.1,
        },
    },
    "mid_price_arbitrage": {
        "type": "mid_price",
        "name": "mid_price_arbitrage",
        "starting_capital": 5000.0,
        "min_spread": 0.0001,
        "threshold": 0.0001,
        "exchanges": ["bybit", "cryptocom"],
        "pairs": [("ATOM", "USDT"), ("DOT", "USDT")],
        "risk_profile": "conservative",  # Reference to risk profile
        "execution_method": "staggered",  # Reference to execution method
        "execution_params": {  # Optional overrides
            "timeout": 5000,
            "retry_attempts": 3,
            "max_slippage": 0.0003,
            "stagger_delay": 500,
        },
        "additional_info": {
            "min_depth": 10,
            "execution_delay": 0.2,
        },
    },
    "vwap_arbitrage": {
        "type": "vwap",
        "name": "vwap_arbitrage",
        "starting_capital": 15000.0,
        "min_spread": 0.0002,
        "threshold": 0.0002,
        "target_volume": 100.0,
        "min_depth_percentage": 0.7,
        "exchanges": ["bybit", "cryptocom"],
        "pairs": [("AVAX", "USDT"), ("NEAR", "USDT"), ("FTM", "USDT"), ("ALGO", "USDT")],
        "risk_profile": "aggressive",  # Reference to risk profile
        "execution_method": "risk_aware",  # Reference to execution method
        "execution_params": {  # Optional overrides
            "timeout": 4000,
            "retry_attempts": 2,
            "max_slippage": 0.0004,
        },
        "additional_info": {
            "min_volume": 15.0,
            "liquidity_factor": 0.8,
            "depth_scaling": True,
        },
    },
    "liquidity_adjusted_arbitrage": {
        "type": "liquidity_adjusted",
        "name": "liquidity_adjusted_arbitrage",
        "starting_capital": 20000.0,
        "min_spread": 0.0015,
        "threshold": 0.0015,
        "target_volume": 100.0,
        "min_depth_percentage": 0.7,
        "slippage_factor": 0.5,
        "liquidity_multiplier": 1.5,
        "dynamic_volume_adjust": True,
        "max_slippage": 0.0006,
        "opportunity_timeout": 1.5,
        "exchanges": ["bybit", "cryptocom"],
        "pairs": [
            ("AVAX", "USDT"),
            ("NEAR", "USDT"),
            ("FTM", "USDT"),
        ],
        "risk_profile": "market_maker",  # Reference to risk profile
        "execution_method": "risk_aware",  # Reference to execution method
        "execution_params": {  # Optional overrides
            "timeout": 3500,
            "retry_attempts": 2,
            "max_slippage": 0.0006,
        },
        "additional_info": {
            "min_volume": 15.0,
            "liquidity_factor": 0.85,
            "depth_scaling": True,
        },
    },
}

# Active strategies for trading
STRATEGIES = {
    "basic_arbitrage": ALL_STRATEGIES["basic_arbitrage"],
    "mid_price_arbitrage": ALL_STRATEGIES["mid_price_arbitrage"],
    "vwap_arbitrage": ALL_STRATEGIES["vwap_arbitrage"],
    "liquidity_adjusted_arbitrage": ALL_STRATEGIES["liquidity_adjusted_arbitrage"],
}

# Add metrics tracking configuration
METRICS_CONFIG = {
    "tracking_enabled": True,
    "tracking_interval": 300,  # 5 minutes
    "metrics_retention_days": 30,
    "performance_tracking": {
        "track_fees": True,
        "track_execution_time": True,
        "track_market_conditions": True,
    },
    "alert_thresholds": {
        "max_drawdown_percentage": 10.0,
        "min_win_rate": 40.0,
        "max_consecutive_losses": 5,
    },
}

# Risk profiles for different strategy types
RISK_PROFILES = {
    "conservative": {
        "name": "conservative",
        "description": "Low risk profile with small position sizes and tight controls",
        "max_position_size_percentage": 2.0,  # 2% of capital per trade
        "max_drawdown_percentage": 5.0,  # 5% max drawdown
        "max_exposure_per_asset_percentage": 10.0,  # 10% max per asset
        "circuit_breaker_conditions": {
            "max_consecutive_losses": 2,
            "cooldown_period": 3600,  # 1 hour cooldown
            "daily_loss_limit": 3.0,  # 3% daily loss limit
            "max_trades_per_hour": 10,
        },
        "metrics_thresholds": {
            "max_drawdown_percentage": 5.0,
            "min_win_rate": 50.0,
            "max_consecutive_losses": 3,
        },
    },
    "moderate": {
        "name": "moderate",
        "description": "Balanced risk profile for steady returns",
        "max_position_size_percentage": 5.0,  # 5% of capital per trade
        "max_drawdown_percentage": 10.0,  # 10% max drawdown
        "max_exposure_per_asset_percentage": 20.0,  # 20% max per asset
        "circuit_breaker_conditions": {
            "max_consecutive_losses": 3,
            "cooldown_period": 1800,  # 30 min cooldown
            "daily_loss_limit": 5.0,  # 5% daily loss limit
            "max_trades_per_hour": 20,
        },
    },
    "aggressive": {
        "name": "aggressive",
        "description": "High risk profile for maximum returns",
        "max_position_size_percentage": 10.0,  # 10% of capital per trade
        "max_drawdown_percentage": 20.0,  # 20% max drawdown
        "max_exposure_per_asset_percentage": 40.0,  # 40% max per asset
        "circuit_breaker_conditions": {
            "max_consecutive_losses": 5,
            "cooldown_period": 900,  # 15 min cooldown
            "daily_loss_limit": 8.0,  # 8% daily loss limit
            "max_trades_per_hour": 30,
        },
    },
    "market_maker": {
        "name": "market_maker",
        "description": "Specialized profile for market making strategies",
        "max_position_size_percentage": 15.0,  # 15% of capital per trade
        "max_drawdown_percentage": 15.0,  # 15% max drawdown
        "max_exposure_per_asset_percentage": 50.0,  # 50% max per asset
        "circuit_breaker_conditions": {
            "max_consecutive_losses": 4,
            "cooldown_period": 300,  # 5 min cooldown
            "daily_loss_limit": 6.0,  # 6% daily loss limit
            "max_trades_per_hour": 100,  # Higher frequency trading
            "max_spread_multiplier": 2.0,  # Maximum spread multiplier
        },
    },
}


# Helper functions
def get_all_exchange_names():
    """Get list of all exchange names"""
    return list(ALL_EXCHANGES.keys())


def get_all_pair_symbols():
    """Get list of all trading pair symbols"""
    return list(ALL_TRADING_PAIRS.keys())


def get_exchange_config(exchange_name):
    """Get configuration for a specific exchange"""
    if exchange_name in EXCHANGES:
        return EXCHANGES[exchange_name]
    return ALL_EXCHANGES.get(exchange_name)


def get_pair_config(pair_symbol):
    """Get configuration for a specific trading pair"""
    if pair_symbol in ALL_TRADING_PAIRS:
        return ALL_TRADING_PAIRS[pair_symbol]

    for base, quote in TRADING_PAIRS:
        constructed_symbol = f"{base}-{quote}"
        if constructed_symbol == pair_symbol:
            return {"base_currency": base, "quote_currency": quote, "symbol": constructed_symbol}
    return None


def get_strategy_config(strategy_name):
    """Get configuration for a specific strategy"""
    if strategy_name in STRATEGIES:
        return STRATEGIES[strategy_name]
    return ALL_STRATEGIES.get(strategy_name)


def get_execution_method_config(method_name):
    """Get configuration for a specific execution method"""
    return EXECUTION_METHODS.get(method_name, EXECUTION_METHODS["parallel"])


def get_strategy_params(strategy_name):
    """Extract strategy parameters suitable for database StrategyParameters entry"""
    strategy = get_strategy_config(strategy_name)
    if not strategy:
        return None

    params = {
        "min_spread": strategy.get("min_spread", 0.001),
        "threshold": strategy.get("threshold", 0.001),
    }

    if "execution_params" in strategy:
        if "max_slippage" in strategy["execution_params"]:
            params["max_slippage"] = strategy["execution_params"]["max_slippage"]
        if "timeout" in strategy["execution_params"]:
            params["max_execution_time_ms"] = strategy["execution_params"]["timeout"]

    if "additional_info" in strategy:
        if "min_volume" in strategy["additional_info"]:
            params["min_volume"] = strategy["additional_info"]["min_volume"]

    additional = {k: v for k, v in strategy.items() if k not in ["name", "min_spread", "threshold"] and k not in params}
    if additional:
        params["additional_parameters"] = additional

    return params


def get_risk_profile_for_strategy(strategy_name):
    """Extract risk profile parameters for a strategy"""
    strategy = get_strategy_config(strategy_name)
    if not strategy:
        return None

    risk_profile_name = strategy.get("risk_profile", STRATEGY_RISK_PROFILES.get(strategy["type"], "moderate"))
    risk_profile = RISK_PROFILES.get(risk_profile_name, RISK_PROFILES["moderate"]).copy()
    risk_profile["name"] = f"{strategy_name}_risk_profile"

    return risk_profile


def get_execution_strategy_for_strategy(strategy_name):
    """Extract execution strategy parameters for a strategy"""
    strategy = get_strategy_config(strategy_name)
    if not strategy:
        return None

    execution_method_name = strategy.get(
        "execution_method", STRATEGY_EXECUTION_METHODS.get(strategy["type"], "parallel")
    )
    execution_config = EXECUTION_METHODS[execution_method_name].copy()

    if "execution_params" in strategy:
        execution_config.update(strategy["execution_params"])

    execution_config["name"] = f"{strategy_name}_{execution_config['method']}_execution"
    return execution_config


def get_risk_profile(profile_name: str) -> dict:
    """Get a risk profile configuration by name"""
    return RISK_PROFILES.get(profile_name, RISK_PROFILES["moderate"])


def get_metrics_config():
    """Get metrics tracking configuration"""
    return METRICS_CONFIG


def get_all_strategy_names():
    """Get list of all strategy names"""
    return list(ALL_STRATEGIES.keys())
