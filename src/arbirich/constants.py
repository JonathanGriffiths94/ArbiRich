"""
Constants used throughout the ArbiRich application.
Centralizing constants helps maintain consistency across the codebase.
"""

# Redis channel names
ORDER_BOOK_CHANNEL = "order_book"
TRADE_OPPORTUNITIES_CHANNEL = "trade_opportunities"
TRADE_EXECUTIONS_CHANNEL = "trade_executions"
DEBUG_CHANNEL = "debug"

# Redis key prefixes
ORDER_BOOK_KEY_PREFIX = "order_book:"
TRADE_OPPORTUNITY_KEY_PREFIX = "trade_opportunity:"
TRADE_EXECUTION_KEY_PREFIX = "trade_execution:"
DEDUP_KEY_PREFIX = "dedup:"

# Time constants (in seconds)
OPPORTUNITY_TTL = 300  # How long to keep opportunities in Redis
DEBOUNCE_TTL = 10  # Debounce time for similar opportunities
HEALTH_CHECK_INTERVAL = 30  # How often to check Redis health

# Operational constants
MAX_RETRY_ATTEMPTS = 3
RECONNECT_DELAY = 5
DEFAULT_BATCH_SIZE = 10

# Numeric constants
PRICE_PRECISION = 8  # Decimal places for price
VOLUME_PRECISION = 8  # Decimal places for volume

# Status codes
STATUS_SUCCESS = "success"
STATUS_ERROR = "error"
STATUS_PENDING = "pending"
STATUS_EXECUTING = "executing"
STATUS_COMPLETED = "completed"
STATUS_CANCELED = "canceled"
STATUS_EXPIRED = "expired"

# Log levels as constants
LOG_DEBUG = "DEBUG"
LOG_INFO = "INFO"
LOG_WARNING = "WARNING"
LOG_ERROR = "ERROR"
LOG_CRITICAL = "CRITICAL"

# Default strategy name
DEFAULT_STRATEGY = "basic_arbitrage"

# Exchange specific constants
# Minimum order sizes (in base asset)
MIN_ORDER_SIZE = {
    "bybit": {
        "BTC-USDT": 0.001,
        "ETH-USDT": 0.01,
        "SOL-USDT": 0.1,
    },
    "cryptocom": {
        "BTC-USDT": 0.0001,
        "ETH-USDT": 0.01,
        "SOL-USDT": 0.1,
    },
}

# Fee rates by exchange (as decimal, e.g., 0.001 = 0.1%)
FEE_RATES = {
    "bybit": 0.001,  # 0.1%
    "cryptocom": 0.001,  # 0.1%
}
