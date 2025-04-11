# ArbiRich Configuration System

This document explains the new consolidated configuration system for the ArbiRich arbitrage trading platform.


## Usage

```python
# Import the consolidated configuration
from src.arbirich.config.consolidated_config import (
    # Database configuration
    DATABASE_URL, DB_HOST, DB_PORT,
    
    # Redis configuration
    REDIS_CONFIG, REDIS_URL,
    
    # Application settings
    DEBUG, LOG_LEVEL,
    
    # Exchange and strategy access
    ALL_EXCHANGES, ALL_PAIRS, ALL_STRATEGIES,
    
    # Utility functions
    get_exchange_config, get_pair_config, get_strategy_config,
    get_active_exchanges, get_active_pairs, get_active_strategies,
)

# Examples
# Get an exchange configuration
binance_config = get_exchange_config("binance")

# Get all active strategies
active_strategies = get_active_strategies()

# Get a specific strategy configuration
strategy_config = get_strategy_config("basic_arbitrage")
```

## Configuration Structure

### Environment Variables

The system uses environment variables with fallbacks to default values:

```bash
# Database
POSTGRES_DB=arbirich_db
POSTGRES_USER=arbiuser
POSTGRES_PASSWORD=postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0

# Web/API
WEB_HOST=0.0.0.0
WEB_PORT=8080
API_HOST=0.0.0.0
API_PORT=8088

# Application
DEBUG=false
LOG_LEVEL=INFO
STRATEGY_SELECTION=basic_arbitrage
```

### Exchanges

Exchanges are defined in the `ALL_EXCHANGES` dictionary with all necessary properties:

```python
ALL_EXCHANGES = {
    "binance": {
        "name": "binance",
        "api_rate_limit": 1200,
        "trade_fees": 0.0010,
        "rest_url": "https://api.binance.com",
        "ws_url": "wss://stream.binance.com/ws",
        "delimiter": "",
        "is_active": False,
        "withdrawal_fee": { ... },
        "api_response_time": 150,
        "mapping": { ... },
        "additional_info": { ... }
    },
    # More exchanges...
}
```

### Trading Pairs

Trading pairs are defined in the `ALL_PAIRS` dictionary:

```python
ALL_PAIRS = {
    "BTC-USDT": {
        "base_currency": "BTC",
        "quote_currency": "USDT",
        "symbol": "BTC-USDT",
        "is_active": False
    },
    # More pairs...
}
```

### Strategies

Strategies are defined in the `ALL_STRATEGIES` dictionary with detailed configuration:

```python
ALL_STRATEGIES = {
    "basic_arbitrage": {
        "name": "basic_arbitrage",
        "type": "basic",
        "starting_capital": 10000.0,
        "min_spread": 0.0015,
        "exchanges": ["bybit", "cryptocom"],
        "pairs": [("BTC", "USDT"), ("ETH", "USDT")],
        "is_active": False,
        "additional_info": { ... }
    },
    # More strategies...
}
```

## Utility Functions

The module provides several utility functions for accessing configuration:

- `get_all_exchange_names()` - Get a list of all exchange names
- `get_exchange_config(exchange_name)` - Get configuration for a specific exchange
- `get_active_exchanges()` - Get all active exchanges
- `get_all_pair_symbols()` - Get a list of all trading pair symbols 
- `get_pair_config(pair_symbol)` - Get configuration for a specific trading pair
- `get_active_pairs()` - Get all active trading pairs
- `get_all_strategy_names()` - Get a list of all strategy names
- `get_strategy_config(strategy_name)` - Get configuration for a specific strategy
- `get_active_strategies(selection)` - Get active strategies, optionally filtered by name
- `get_unique_pairs_for_active_strategies()` - Get unique pairs used in active strategies
- `get_unique_exchanges_for_active_strategies()` - Get unique exchanges used in active strategies
