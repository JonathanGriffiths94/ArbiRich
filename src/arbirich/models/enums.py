import enum
from typing import Any, Dict, List, Optional, Tuple


# ------------------ Channel Enums ------------------
class ChannelName(str, enum.Enum):
    """Redis channel names for pub/sub communication"""

    TRADE_OPPORTUNITIES = "trade_opportunities"
    TRADE_EXECUTIONS = "trade_executions"
    ORDER_BOOK = "order_book"

    @classmethod
    def list(cls) -> List[str]:
        return [e.value for e in cls]


# ------------------ Table Enums ------------------
class TableName(str, enum.Enum):
    """Database table names"""

    EXCHANGES = "exchanges"
    TRADING_PAIRS = "trading_pairs"  # Updated to match new naming
    STRATEGIES = "strategies"
    TRADE_OPPORTUNITIES = "trade_opportunities"
    TRADE_EXECUTIONS = "trade_executions"
    STRATEGY_METRICS = "strategy_metrics"
    STRATEGY_TRADING_PAIR_METRICS = "strategy_trading_pair_metrics"
    STRATEGY_EXCHANGE_METRICS = "strategy_exchange_metrics"

    @classmethod
    def list(cls) -> List[str]:
        return [e.value for e in cls]


# ------------------ Redis Key Prefix Enums ------------------
class RedisKeyPrefix(str, enum.Enum):
    """Redis key prefixes for different data types"""

    ORDER_BOOK = "order_book:"
    TRADE_OPPORTUNITY = "trade_opportunity:"
    TRADE_EXECUTION = "trade_execution:"
    DEDUP = "dedup:"

    @classmethod
    def list(cls) -> List[str]:
        return [e.value for e in cls]


# ------------------ Status Enums ------------------
class Status(str, enum.Enum):
    """General status constants"""

    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"
    PENDING = "pending"
    COMPLETE = "complete"

    @classmethod
    def list(cls) -> List[str]:
        return [e.value for e in cls]


# ------------------ Log Level Enums ------------------
class LogLevel(str, enum.Enum):
    """Log levels"""

    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"

    @classmethod
    def list(cls) -> List[str]:
        return [e.value for e in cls]


# ------------------ Strategy Enums ------------------
class StrategyType(str, enum.Enum):
    """Types of trading strategies"""

    BASIC = "basic"
    MID_PRICE = "mid_price"
    HFT = "hft"
    STATISTICAL = "statistical"
    MACHINE_LEARNING = "ml"
    GRID_TRADING = "grid"
    MARKET_MAKING = "market_making"
    MEAN_REVERSION = "mean_reversion"
    TREND_FOLLOWING = "trend_following"

    @classmethod
    def list(cls) -> List[str]:
        return [e.value for e in cls]

    @classmethod
    def get_config(cls, strategy_type: str) -> Optional[Dict[str, Any]]:
        """Get the configuration for a strategy type from global config"""
        from src.arbirich.config import get_strategy_config

        return get_strategy_config(strategy_type)


# ------------------ Exchange Enums ------------------
class ExchangeType(str, enum.Enum):
    """Supported exchanges"""

    BYBIT = "bybit"
    CRYPTOCOM = "cryptocom"
    BINANCE = "binance"
    COINBASE = "coinbase"
    KRAKEN = "kraken"
    KUCOIN = "kucoin"
    HUOBI = "huobi"
    BITFINEX = "bitfinex"

    @classmethod
    def list(cls) -> List[str]:
        return [e.value for e in cls]

    @classmethod
    def active(cls) -> List[str]:
        """Get list of active exchanges from config"""
        from src.arbirich.config.config import get_all_exchange_names

        return get_all_exchange_names()

    @classmethod
    def get_config(cls, exchange_name: str) -> Optional[Dict[str, Any]]:
        """Get the configuration for an exchange from global config"""
        from src.arbirich.config import get_exchange_config

        return get_exchange_config(exchange_name)


# ------------------ Pair Enums ------------------
class PairType(str, enum.Enum):
    """Common trading pairs"""

    BTC_USDT = "BTC-USDT"
    ETH_USDT = "ETH-USDT"
    SOL_USDT = "SOL-USDT"
    BNB_USDT = "BNB-USDT"
    ETH_BTC = "ETH-BTC"
    XRP_USDT = "XRP-USDT"
    ADA_USDT = "ADA-USDT"
    DOT_USDT = "DOT-USDT"
    DOGE_USDT = "DOGE-USDT"
    AVAX_USDT = "AVAX-USDT"

    @classmethod
    def list(cls) -> List[str]:
        return [e.value for e in cls]

    @classmethod
    def active(cls) -> List[str]:
        """Get list of active pairs from config"""
        from src.arbirich.config.config import get_all_pair_symbols

        return get_all_pair_symbols()

    @classmethod
    def get_currencies(cls, pair_value: str) -> Tuple[str, str]:
        """Get base and quote currencies from a pair value"""
        if pair_value not in cls.list() and "-" in pair_value:
            # Handle custom pairs not defined in the enum
            base, quote = pair_value.split("-")
            return base, quote

        if pair_value not in cls.list():
            raise ValueError(f"Invalid pair value: {pair_value}")

        base, quote = pair_value.split("-")
        return base, quote

    @classmethod
    def get_config(cls, pair_symbol: str) -> Optional[Dict[str, Any]]:
        """Get the configuration for a pair from global config"""
        from src.arbirich.config import get_pair_config

        return get_pair_config(pair_symbol)


# ------------------ Order Enums ------------------
class OrderType(str, enum.Enum):
    """Types of orders"""

    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"

    @classmethod
    def list(cls) -> List[str]:
        return [e.value for e in cls]


class OrderSide(str, enum.Enum):
    """Order sides"""

    BUY = "buy"
    SELL = "sell"

    @classmethod
    def list(cls) -> List[str]:
        return [e.value for e in cls]


# ------------------ Trade Execution Enums ------------------
class TradeStatus(str, enum.Enum):
    """Status of trade executions"""

    PENDING = "pending"
    EXECUTED = "executed"
    FAILED = "failed"
    CANCELED = "canceled"
    PARTIAL = "partial"

    @classmethod
    def list(cls) -> List[str]:
        return [e.value for e in cls]


# ------------------ Time Constants ------------------
class TimeConstants:
    """Time-related constants in seconds"""

    OPPORTUNITY_TTL = 300  # How long to keep opportunities in Redis
    DEBOUNCE_TTL = 10  # Debounce time for similar opportunities
    HEALTH_CHECK_INTERVAL = 30  # How often to check Redis health
    RECONNECT_DELAY = 5  # Delay before reconnection attempts


# ------------------ Operational Constants ------------------
class OperationalConstants:
    """General operational constants"""

    MAX_RETRY_ATTEMPTS = 3
    DEFAULT_BATCH_SIZE = 10
    PRICE_PRECISION = 8  # Decimal places for price
    VOLUME_PRECISION = 8  # Decimal places for volume
    MIN_ORDER_SIZE = 0.001  # Minimum order size for most exchanges
    MAX_SLIPPAGE_PERCENT = 0.5  # Maximum allowed slippage (0.5%)
    MAX_EXECUTION_RETRIES = 3  # Maximum number of execution retry attempts
    EXECUTION_TIMEOUT_MS = 10000  # Timeout for executions (10 seconds)
    ORDER_BOOK_DEPTH = 10  # Number of order book levels to fetch
    ORDER_BOOK_UPDATE_INTERVAL = 5  # Update interval for order book data
