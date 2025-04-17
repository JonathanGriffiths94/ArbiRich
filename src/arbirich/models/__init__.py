"""
Consolidated model imports to simplify importing models throughout the application.
This module imports all models from the various model files.
"""

from typing import Callable, Dict, Type

from arbirich.models.base import BaseModel, IdentifiableModel, StatusAwareModel, TimestampedModel
from arbirich.models.configs import (
    AppConfig,
    BaseComponentConfig,
    BaseExecutionConfig,
    BaseStrategyConfig,
    BasicStrategyConfig,
    DetectionComponentConfig,
    ExchangeConfig,
    ExecutionComponentConfig,
    IngestionComponentConfig,
    LiquidityAdjustedStrategyConfig,
    MidPriceStrategyConfig,
    ReportingComponentConfig,
    RiskConfig,
    TradingPairConfig,
    VWAPStrategyConfig,
)
from arbirich.models.exchange import (
    AccountBalances,
    CandlestickData,
    Exchange,
    ExchangeBalance,
    ExchangeInfo,
    ExchangeOrder,
    OrderResponse,
    SymbolInfo,
    TickerData,
    TradeExecutionResult,
    WebsocketMessage,
)
from arbirich.models.trading_pair import TradingPair
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
from src.arbirich.models.db.base import Base
from src.arbirich.models.enums import (
    ChannelName,
    ExchangeType,
    LogLevel,
    OperationalConstants,
    OrderSide,
    OrderStatus,
    OrderType,
    PairType,
    RedisKeyPrefix,
    Status,
    StrategyType,
    TableName,
    TimeConstants,
)
from src.arbirich.models.metrics import StrategyExchangeMetrics, StrategyMetrics, StrategyTradingPairMetrics
from src.arbirich.models.order_book import (
    MarketDepth,
    MarketDepthLevel,
    OrderBookState,
    OrderBookUpdate,
    OrderLevel,
    market_depth_to_order_book,
    order_book_to_market_depth,
)
from src.arbirich.models.risk import (
    RiskAssessment,
    RiskProfile,
)
from src.arbirich.models.strategy import (
    Strategy,
    StrategyExchangePairMapping,
    StrategyExecutionMapping,
    StrategyParameters,
    StrategyTypeModel,
    StrategyTypeParameters,
)
from src.arbirich.models.trading import (
    Order,
    TradeExecution,
    TradeOpportunity,
    TradeRequest,
    TradeResponse,
    TradeResultReference,
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
    "OrderStatus": OrderStatus,
    "OperationalConstants": OperationalConstants,
    "TimeConstants": TimeConstants,
    # Base models
    "BaseModel": BaseModel,
    "IdentifiableModel": IdentifiableModel,
    "StatusAwareModel": StatusAwareModel,
    "TimestampedModel": TimestampedModel,
    # Config models
    "AppConfig": AppConfig,
    "ExecutionConfig": BaseExecutionConfig,
    "ExchangeConfig": ExchangeConfig,
    "RiskConfig": RiskConfig,
    "BaseStrategyConfig": BaseStrategyConfig,
    "BasicStrategyConfig": BasicStrategyConfig,
    "MidPriceStrategyConfig": MidPriceStrategyConfig,
    "VWAPStrategyConfig": VWAPStrategyConfig,
    "BaseComponentConfig": BaseComponentConfig,
    "ExecutionComponentConfig": ExecutionComponentConfig,
    "LiquidityAdjustedStrategyConfig": LiquidityAdjustedStrategyConfig,
    "DetectionComponentConfig": DetectionComponentConfig,
    "IngestionComponentConfig": IngestionComponentConfig,
    "ReportingComponentConfig": ReportingComponentConfig,
    "TradingPairConfig": TradingPairConfig,
    # Exchange models
    "AccountBalances": AccountBalances,
    "CandlestickData": CandlestickData,
    "Exchange": Exchange,
    "ExchangeBalance": ExchangeBalance,
    "ExchangeInfo": ExchangeInfo,
    "ExchangeOrder": ExchangeOrder,
    "OrderResponse": OrderResponse,
    "SymbolInfo": SymbolInfo,
    "TickerData": TickerData,
    "TradeExecutionResult": TradeExecutionResult,
    "WebsocketMessage": WebsocketMessage,
    # Trading pair models
    "TradingPair": TradingPair,
    # Order book models
    "MarketDepth": MarketDepth,
    "MarketDepthLevel": MarketDepthLevel,
    "OrderBookState": OrderBookState,
    "OrderBookUpdate": OrderBookUpdate,
    "OrderLevel": OrderLevel,
    "Order": Order,
    # Trade models
    "TradeRequest": TradeRequest,
    "TradeResponse": TradeResponse,
    "TradeResultReference": TradeResultReference,
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
    "StrategyTypeModel": StrategyTypeModel,
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
}

# Register utility functions
utility_registry: Dict[str, Callable] = {
    "market_depth_to_order_book": market_depth_to_order_book,
    "order_book_to_market_depth": order_book_to_market_depth,
}

# Export all models
__all__ = list(model_registry.keys()) + list(utility_registry.keys())
