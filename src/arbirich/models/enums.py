"""
Enumeration classes for the ArbiRich application.
All enums and constants are consolidated in this file to prevent circular imports.
"""

from enum import Enum


class StrEnum(str, Enum):
    """Base class for string enumerations."""

    def __str__(self) -> str:
        return str(self.value)


class OrderSide(StrEnum):
    """Order side (buy/sell)."""

    BUY = "buy"
    SELL = "sell"


class OrderType(StrEnum):
    """Order type (market/limit/etc)."""

    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"


class TradeStatus(StrEnum):
    """Status of a trade execution."""

    PENDING = "pending"
    EXECUTED = "executed"
    PARTIALLY_EXECUTED = "partially_executed"
    FAILED = "failed"
    CANCELED = "canceled"
    REJECTED = "rejected"


class Status(str, Enum):
    """General status enum for various components."""

    ACTIVE = "active"
    INACTIVE = "inactive"
    ERROR = "error"
    PENDING = "pending"
    WARNING = "warning"
    COMPLETE = "complete"


class ExchangeType(StrEnum):
    """Type of exchange."""

    BINANCE = "binance"
    COINBASE = "coinbase"
    KRAKEN = "kraken"
    KUCOIN = "kucoin"
    HUOBI = "huobi"
    BYBIT = "bybit"
    BITFINEX = "bitfinex"
    OKX = "okx"
    GEMINI = "gemini"
    CRYPTOCOM = "cryptocom"


class StrategyType(StrEnum):
    """Type of trading strategy."""

    ARBITRAGE = "arbitrage"
    MARKET_MAKING = "market_making"
    TRIANGULAR = "triangular"
    STATISTICAL = "statistical"
    BASIC = "basic"
    MID_PRICE = "mid_price"
    VWAP = "vwap"
    LIQUIDITY_ADJUSTED = "liquidity_adjusted"


class PairType(StrEnum):
    """Type of trading pair."""

    SPOT = "spot"
    FUTURES = "futures"
    MARGIN = "margin"
    OPTIONS = "options"


class LogLevel(str, Enum):
    """Log levels."""

    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class RedisKeyPrefix(str, Enum):
    """Redis key prefixes for different data types."""

    ORDER_BOOK = "order_book"
    TRADE_OPPORTUNITY = "trade_opportunity"
    TRADE_EXECUTION = "trade_execution"
    DEDUP = "dedup"
    BALANCE = "balance"
    SESSION = "session"
    LOCK = "lock"


class ChannelName(str, Enum):
    """WebSocket channel names."""

    TRADE_OPPORTUNITIES = "trade_opportunities"
    TRADE_EXECUTIONS = "trade_executions"
    ORDER_BOOK = "order_book"
    STATUS_UPDATES = "status_updates"
    OPPORTUNITY = "opportunity"
    EXECUTION = "execution"
    STATS = "stats"
    BROADCAST = "broadcast"


class TableName(str, Enum):
    """Database table names."""

    EXCHANGES = "exchanges"
    TRADING_PAIRS = "trading_pairs"
    TRADE_OPPORTUNITIES = "trade_opportunities"
    TRADE_EXECUTIONS = "trade_executions"
    STRATEGIES = "strategies"
    STRATEGY_METRICS = "strategy_metrics"
    STRATEGY_TRADING_PAIR_METRICS = "strategy_trading_pair_metrics"
    STRATEGY_EXCHANGE_METRICS = "strategy_exchange_metrics"
    STRATEGY_TYPES = "strategy_types"
    STRATEGY_PARAMETERS = "strategy_parameters"
    STRATEGY_TYPE_PARAMETERS = "strategy_type_parameters"
    STRATEGY_EXCHANGE_PAIR_MAPPINGS = "strategy_exchange_pair_mappings"
    STRATEGY_EXECUTION_MAPPING = "strategy_execution_mapping"
    RISK_PROFILES = "risk_profiles"
    EXECUTION_STRATEGIES = "execution_strategies"
    SYSTEM_HEALTH_CHECKS = "system_health_checks"


class OperationalConstants:
    """Operational constants for the application."""

    MAX_RETRY_ATTEMPTS = 3
    DEFAULT_BATCH_SIZE = 100
    PRICE_PRECISION = 8
    VOLUME_PRECISION = 8
    MIN_ORDER_SIZE = 10.0
    MAX_SLIPPAGE_PERCENT = 0.1
    MAX_EXECUTION_RETRIES = 2
    EXECUTION_TIMEOUT_MS = 5000


class TimeConstants:
    """Time-related constants for the application."""

    OPPORTUNITY_TTL = 60  # seconds
    DEBOUNCE_TTL = 1.0  # seconds
    HEALTH_CHECK_INTERVAL = 30  # seconds
    RECONNECT_DELAY = 5  # seconds
