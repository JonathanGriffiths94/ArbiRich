"""
Consolidated model imports to simplify importing models throughout the application.
This module imports all models from the various model files.
"""

from typing import Callable, Dict, Type

# Import API models
from src.arbirich.models.api.requests import (
    ExchangeCreateRequest,
    RiskProfileCreateRequest,
    StrategyCreateRequest,
    TradingPairCreateRequest,
    TradingStopRequest,
)
from src.arbirich.models.api.responses import (
    ChartData,
    ChartDataset,
    DashboardStats,
    HealthResponse,
    StatusResponse,
    TradingStatusResponse,
)

# Import base models
from src.arbirich.models.base import (
    BaseModel,
    IdentifiableModel,
    StatusAwareModel,
    TimestampedModel,
)

# Import configuration models
from src.arbirich.models.config import (
    AppConfig,
    ExchangeConfig,
    ExecutionConfig,
    RiskConfig,
    StrategyConfig,
    TradingPairConfig,
)

# Import DB models (optional, typically accessed through repositories instead)
from src.arbirich.models.db.base import Base

# Import all enumerations
from src.arbirich.models.enums import (
    ChannelName,
    ExchangeType,
    LogLevel,
    OperationalConstants,
    OrderSide,
    OrderType,
    PairType,
    RedisKeyPrefix,
    Status,
    StrategyType,
    TableName,
    TimeConstants,
    TradeStatus,
)

# Import exchange models
from src.arbirich.models.exchange import (
    AccountBalances,
    CandlestickData,
    Exchange,
    ExchangeBalance,
    ExchangeInfo,
    ExchangeOrder,
    OrderResponse,
    OrderStatus,
    SymbolInfo,
    TickerData,
    TradeExecutionResult,
    WebsocketMessage,
)

# Import execution models
from src.arbirich.models.execution import (
    ExecutionResult,
    ExecutionStrategy,
    Order,
    TradeExecution,
    TradeOpportunity,
)
from src.arbirich.models.metrics import StrategyExchangeMetrics, StrategyMetrics, StrategyTradingPairMetrics

# Import system health model
from src.arbirich.models.models import SystemHealthCheck

# Import order book models
from src.arbirich.models.order_book import (
    MarketDepth,
    MarketDepthLevel,
    OrderBookState,
    OrderBookUpdate,
    OrderLevel,
    market_depth_to_order_book,
    order_book_to_market_depth,
)

# Import risk models
from src.arbirich.models.risk import (
    RiskAssessment,
    RiskProfile,
)
from src.arbirich.models.strategy import (
    Strategy,
    StrategyExchangePairMapping,
    StrategyExecutionMapping,
    StrategyParameters,
    StrategyTypeModel,  # Strategy type model class
    StrategyTypeParameters,
)

# Import trading models
from src.arbirich.models.trading import (
    OrderBook,
    OrderBookEntry,
    TradingPair,
)

# Create registries for different types of models
model_registry: Dict[str, Type] = {
    # Enum models
    "ChannelName": ChannelName,
    "ExchangeType": ExchangeType,
    "LogLevel": LogLevel,
    "OrderSide": OrderSide,
    "OrderType": OrderType,
    "PairType": PairType,
    "RedisKeyPrefix": RedisKeyPrefix,
    "Status": Status,
    "StrategyType": StrategyType,
    "TableName": TableName,
    "TradeStatus": TradeStatus,
    "OperationalConstants": OperationalConstants,
    "TimeConstants": TimeConstants,
    # Base models
    "BaseModel": BaseModel,
    "IdentifiableModel": IdentifiableModel,
    "StatusAwareModel": StatusAwareModel,
    "TimestampedModel": TimestampedModel,
    # Config models
    "AppConfig": AppConfig,
    "ExecutionConfig": ExecutionConfig,
    "ExchangeConfig": ExchangeConfig,
    "RiskConfig": RiskConfig,
    "StrategyConfig": StrategyConfig,
    "TradingPairConfig": TradingPairConfig,
    # Exchange models
    "AccountBalances": AccountBalances,
    "CandlestickData": CandlestickData,
    "Exchange": Exchange,
    "ExchangeBalance": ExchangeBalance,
    "ExchangeInfo": ExchangeInfo,
    "ExchangeOrder": ExchangeOrder,
    "OrderResponse": OrderResponse,
    "OrderStatus": OrderStatus,
    "SymbolInfo": SymbolInfo,
    "TickerData": TickerData,
    "TradeExecutionResult": TradeExecutionResult,
    "WebsocketMessage": WebsocketMessage,
    # Trading models
    "OrderBook": OrderBook,
    "OrderBookEntry": OrderBookEntry,
    "TradingPair": TradingPair,
    # Order book models
    "MarketDepth": MarketDepth,
    "MarketDepthLevel": MarketDepthLevel,
    "OrderBookState": OrderBookState,
    "OrderBookUpdate": OrderBookUpdate,
    "OrderLevel": OrderLevel,
    # Execution models
    "ExecutionResult": ExecutionResult,
    "ExecutionStrategy": ExecutionStrategy,
    "Order": Order,
    "TradeExecution": TradeExecution,
    "TradeOpportunity": TradeOpportunity,
    # Risk models
    "RiskAssessment": RiskAssessment,
    "RiskProfile": RiskProfile,
    # Strategy models
    "Strategy": Strategy,
    "StrategyExchangeMetrics": StrategyExchangeMetrics,
    "StrategyExchangePairMapping": StrategyExchangePairMapping,
    "StrategyExecutionMapping": StrategyExecutionMapping,
    "StrategyMetrics": StrategyMetrics,
    "StrategyParameters": StrategyParameters,
    "StrategyTradingPairMetrics": StrategyTradingPairMetrics,
    "StrategyTypeModel": StrategyTypeModel,  # Strategy type model class
    "StrategyTypeParameters": StrategyTypeParameters,
    # API models - Requests
    "ExchangeCreateRequest": ExchangeCreateRequest,
    "RiskProfileCreateRequest": RiskProfileCreateRequest,
    "StrategyCreateRequest": StrategyCreateRequest,
    "TradingPairCreateRequest": TradingPairCreateRequest,
    "TradingStopRequest": TradingStopRequest,
    # API models - Responses
    "ChartData": ChartData,
    "ChartDataset": ChartDataset,
    "DashboardStats": DashboardStats,
    "HealthResponse": HealthResponse,
    "StatusResponse": StatusResponse,
    "TradingStatusResponse": TradingStatusResponse,
    # Database models
    "Base": Base,
    # System health models
    "SystemHealthCheck": SystemHealthCheck,
}

# Register utility functions
utility_registry: Dict[str, Callable] = {
    "market_depth_to_order_book": market_depth_to_order_book,
    "order_book_to_market_depth": order_book_to_market_depth,
}

# Export all models
__all__ = list(model_registry.keys()) + list(utility_registry.keys())
