# Redis channel names
TRADE_OPPORTUNITIES_CHANNEL = "trade_opportunities"
TRADE_EXECUTIONS_CHANNEL = "trade_executions"
ORDER_BOOK_CHANNEL = "order_books"

# Database table names
EXCHANGES_TABLE = "exchanges"
TRADING_PAIRS_TABLE = "trading_pairs"  # Using consistent naming with the schema
STRATEGIES_TABLE = "strategies"
TRADE_OPPORTUNITIES_TABLE = "trade_opportunities"
TRADE_EXECUTIONS_TABLE = "trade_executions"
STRATEGY_METRICS_TABLE = "strategy_metrics"
STRATEGY_TRADING_PAIR_METRICS_TABLE = "strategy_trading_pair_metrics"
STRATEGY_TRADING_EXCHANGE_METRICS_TABLE = "strategy_trading_exchange_metrics"

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

# Status constants
STATUS_ACTIVE = "active"
STATUS_INACTIVE = "inactive"
STATUS_ERROR = "error"
STATUS_PENDING = "pending"
STATUS_COMPLETE = "complete"

# Trading constants
MIN_ORDER_SIZE = 0.001  # Minimum order size for most exchanges
MAX_SLIPPAGE_PERCENT = 0.5  # Maximum allowed slippage (0.5%)
MAX_EXECUTION_RETRIES = 3  # Maximum number of execution retry attempts
EXECUTION_TIMEOUT_MS = 10000  # Timeout for executions (10 seconds)

# Log levels as constants
LOG_DEBUG = "DEBUG"
LOG_INFO = "INFO"
LOG_WARNING = "WARNING"
LOG_ERROR = "ERROR"
LOG_CRITICAL = "CRITICAL"
