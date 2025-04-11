import hashlib
import json
import logging
import time
import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, computed_field
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, Numeric, String, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

logger = logging.getLogger(__name__)

# Create base class for SQLAlchemy models
Base = declarative_base()


# Base models for common API operations
class Exchange(BaseModel):
    id: Optional[int] = None
    name: str
    api_rate_limit: Optional[int] = None
    trade_fees: Optional[float] = None
    rest_url: Optional[str] = None
    ws_url: Optional[str] = None
    delimiter: Optional[str] = None
    withdrawal_fee: Optional[Dict[str, Any]] = None
    api_response_time: Optional[int] = None
    mapping: Optional[Dict[str, Any]] = None
    additional_info: Optional[Dict[str, Any]] = None
    is_active: bool = False  # Default to inactive
    created_at: Optional[datetime] = None

    class Config:
        from_attributes = True

    def to_db_dict(self) -> dict:
        """Convert to a dictionary suitable for database insertion"""
        return {
            "id": self.id,
            "name": self.name,
            "api_rate_limit": self.api_rate_limit,
            "trade_fees": self.trade_fees,
            "rest_url": self.rest_url,
            "ws_url": self.ws_url,
            "delimiter": self.delimiter,
            "withdrawal_fee": self.withdrawal_fee,
            "api_response_time": self.api_response_time,
            "mapping": self.mapping,
            "additional_info": json.dumps(self.additional_info) if self.additional_info else None,
            "is_active": self.is_active,
            "created_at": self.created_at,
        }


# Update the Pair model to ensure symbol is always set:
class Pair(BaseModel):
    id: Optional[int] = None
    base_currency: str
    quote_currency: str
    symbol: Optional[str] = None
    is_active: bool = False  # Default to inactive

    class Config:
        from_attributes = True

    @property
    def get_symbol(self) -> str:
        """Return a standard symbol representation (e.g., BTC-USDT)"""
        return f"{self.base_currency}-{self.quote_currency}"

    def __init__(self, **data):
        super().__init__(**data)
        # Always set symbol if it's not provided or is None
        if not self.symbol:
            self.symbol = self.get_symbol

    def to_db_dict(self) -> dict:
        """Convert to a dictionary suitable for database insertion"""
        # Ensure symbol is set before returning
        if not self.symbol:
            self.symbol = self.get_symbol

        return {
            "id": self.id,
            "base_currency": self.base_currency,
            "quote_currency": self.quote_currency,
            "symbol": self.symbol,
            "is_active": self.is_active,
        }


class Strategy(BaseModel):
    id: Optional[int] = None
    name: str
    starting_capital: float
    min_spread: float
    additional_info: Optional[Dict[str, Any]] = None
    total_profit: float = 0
    total_loss: float = 0
    net_profit: float = 0
    trade_count: int = 0
    start_timestamp: Optional[datetime] = None
    last_updated: Optional[datetime] = None
    is_active: bool = False  # Default to inactive

    # Properly defined metrics fields with consistent naming
    latest_metrics: Optional[Any] = Field(default=None, exclude=True)
    metrics: Optional[List[Any]] = Field(default_factory=list, exclude=True)

    class Config:
        from_attributes = True
        arbitrary_types_allowed = True  # Allow storage of arbitrary types


class Order(BaseModel):
    price: float
    quantity: float


class OrderBookUpdate(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    exchange: str
    symbol: str
    bids: Dict[float, float] = Field(default_factory=dict)  # Changed from List[Order] to Dict[float, float]
    asks: Dict[float, float] = Field(default_factory=dict)  # Changed from List[Order] to Dict[float, float]
    timestamp: float
    sequence: Optional[int] = None  # Added sequence number for tracking updates

    @computed_field
    def hash(self) -> str:
        # Updated to work with Dict format
        bids_json = json.dumps(self.bids, sort_keys=True)
        asks_json = json.dumps(self.asks, sort_keys=True)
        combined = bids_json + asks_json
        return hashlib.sha256(combined.encode("utf-8")).hexdigest()

    # Add helper methods from the new model
    def get_best_bid(self) -> Optional[Order]:
        """Get the highest bid price and quantity"""
        if not self.bids:
            return None
        best_price = max(self.bids.keys())
        return Order(price=best_price, quantity=self.bids[best_price])

    def get_best_ask(self) -> Optional[Order]:
        """Get the lowest ask price and quantity"""
        if not self.asks:
            return None
        best_price = min(self.asks.keys())
        return Order(price=best_price, quantity=self.asks[best_price])

    def get_mid_price(self) -> Optional[float]:
        """Get the mid price between best bid and best ask"""
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()

        if best_bid and best_ask:
            return (best_bid.price + best_ask.price) / 2
        return None

    def get_spread(self) -> Optional[float]:
        """Get the bid-ask spread"""
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()

        if best_bid and best_ask:
            return best_ask.price - best_bid.price
        return None

    def get_spread_percentage(self) -> Optional[float]:
        """Get the bid-ask spread as a percentage of the mid price"""
        spread = self.get_spread()
        mid_price = self.get_mid_price()

        if spread is not None and mid_price is not None:
            return (spread / mid_price) * 100
        return None


class OrderBookState(BaseModel):
    symbols: Dict[str, Dict[str, OrderBookUpdate]] = Field(default_factory=dict)

    @property
    def prices(self) -> set:
        result = set()
        for asset, exch_dict in self.symbols.items():
            for order_book in exch_dict.values():
                for order in order_book.bids:
                    result.add(order.price)
        return result

    def __str__(self):
        return f"OrderBookState(symbols={self.symbols})"


# Core domain models
class TradeOpportunity(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    strategy: str  # Changed from strategy_id to match schema
    pair: str  # Changed from pair_id to match schema
    buy_exchange: str  # Changed from buy_exchange_id to match schema
    sell_exchange: str  # Changed from sell_exchange_id to match schema
    buy_price: float
    sell_price: float
    spread: float = Field(..., description="Difference between best ask and best bid")
    volume: float
    opportunity_timestamp: float = Field(default_factory=time.time)

    class Config:
        from_attributes = True

    def __str__(self):
        return (
            f"TradeOpportunity(id={self.id}, pair={self.pair}, "
            f"buy_exchange={self.buy_exchange}, sell_exchange={self.sell_exchange}, "
            f"buy_price={self.buy_price}, sell_price={self.sell_price}, "
            f"spread={self.spread}, volume={self.volume}, "
            f"opportunity_timestamp={self.opportunity_timestamp})"
        )

    def to_db_dict(self) -> dict:
        """Convert to a dictionary suitable for database insertion"""
        return {
            "id": uuid.UUID(self.id),
            "strategy": self.strategy,
            "pair": self.pair,
            "buy_exchange": self.buy_exchange,
            "sell_exchange": self.sell_exchange,
            "buy_price": self.buy_price,
            "sell_price": self.sell_price,
            "spread": self.spread,
            "volume": self.volume,
            "opportunity_timestamp": datetime.fromtimestamp(self.opportunity_timestamp),
        }

    @property
    def opportunity_key(self) -> str:
        """Generate a unique key for this opportunity"""
        return f"{self.strategy}:{self.pair}:{self.buy_exchange}:{self.sell_exchange}"


class TradeExecution(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    strategy: str  # Changed from strategy_id to match schema
    pair: str  # Changed from pair_id to match schema
    buy_exchange: str  # Changed from buy_exchange_id to match schema
    sell_exchange: str  # Changed from sell_exchange_id to match schema
    executed_buy_price: float
    executed_sell_price: float
    spread: float
    volume: float
    execution_timestamp: float
    execution_id: Optional[str] = None
    opportunity_id: Optional[str] = None

    class Config:
        from_attributes = True

    def __str__(self):
        return (
            f"TradeExecution(id={self.id}, pair={self.pair}, "
            f"buy_exchange={self.buy_exchange}, sell_exchange={self.sell_exchange}, "
            f"executed_buy_price={self.executed_buy_price}, "
            f"executed_sell_price={self.executed_sell_price}, spread={self.spread}, "
            f"volume={self.volume}, execution_timestamp={self.execution_timestamp})"
        )

    def to_db_dict(self) -> dict:
        """Convert to a dictionary suitable for database insertion"""
        db_dict = {}

        # Handle ID conversion safely
        try:
            db_dict["id"] = uuid.UUID(self.id)
        except ValueError:
            # For test data or non-UUID IDs, generate a new UUID
            db_dict["id"] = uuid.uuid4()

        # Copy all other fields directly
        db_dict.update(
            {
                "strategy": self.strategy,
                "pair": self.pair,
                "buy_exchange": self.buy_exchange,
                "sell_exchange": self.sell_exchange,
                "executed_buy_price": self.executed_buy_price,
                "executed_sell_price": self.executed_sell_price,
                "spread": self.spread,
                "volume": self.volume,
                "execution_timestamp": datetime.fromtimestamp(self.execution_timestamp),
                "execution_id": self.execution_id,
            }
        )

        # Handle opportunity_id conversion safely
        if self.opportunity_id:
            try:
                db_dict["opportunity_id"] = uuid.UUID(self.opportunity_id)
            except ValueError:
                # Log a warning but continue
                logger.warning(f"Invalid opportunity_id format: {self.opportunity_id}")
                # You can either set it to None or generate a random UUID
                db_dict["opportunity_id"] = None

        return db_dict


class StrategyMetrics(Base):
    """Strategy performance metrics."""

    __tablename__ = "strategy_metrics"

    id = Column(Integer, primary_key=True)
    strategy_id = Column(Integer, ForeignKey("strategies.id"), nullable=False)
    period_start = Column(DateTime, nullable=False)
    period_end = Column(DateTime, nullable=False)

    # Performance metrics
    win_count = Column(Integer, nullable=False, default=0)
    loss_count = Column(Integer, nullable=False, default=0)
    win_rate = Column(Numeric(5, 2), nullable=False, default=0)  # Percentage

    # Financial metrics
    gross_profit = Column(Numeric(18, 2), nullable=False, default=0)
    gross_loss = Column(Numeric(18, 2), nullable=False, default=0)
    net_profit = Column(Numeric(18, 2), nullable=False, default=0)
    profit_factor = Column(Numeric(8, 4), nullable=False, default=0)  # gross_profit / gross_loss

    # Risk metrics
    max_drawdown = Column(Numeric(18, 2), nullable=False, default=0)
    max_drawdown_percentage = Column(Numeric(5, 2), nullable=False, default=0)
    avg_profit_per_trade = Column(Numeric(18, 2), nullable=False, default=0)
    avg_loss_per_trade = Column(Numeric(18, 2), nullable=False, default=0)
    risk_reward_ratio = Column(Numeric(8, 4), nullable=False, default=0)

    # Volume metrics
    total_volume = Column(Numeric(18, 8), nullable=False, default=0)
    avg_volume_per_trade = Column(Numeric(18, 8), nullable=False, default=0)

    # Time metrics
    avg_hold_time_seconds = Column(Integer, nullable=False, default=0)

    # Market condition metrics
    market_volatility = Column(Numeric(8, 4), nullable=True)
    correlation_to_market = Column(Numeric(5, 2), nullable=True)  # -100 to 100

    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    strategy = relationship("StrategyDB", back_populates="metrics", foreign_keys=[strategy_id])
    trading_pair_metrics = relationship(
        "StrategyTradingPairMetrics", back_populates="strategy_metrics", cascade="all, delete-orphan"
    )
    exchange_metrics = relationship(
        "StrategyExchangeMetrics", back_populates="strategy_metrics", cascade="all, delete-orphan"
    )


class StrategyTradingPairMetrics(Base):
    """Strategy performance metrics by trading pair."""

    __tablename__ = "strategy_trading_pair_metrics"

    id = Column(Integer, primary_key=True)
    strategy_metrics_id = Column(Integer, ForeignKey("strategy_metrics.id"), nullable=False)
    trading_pair_id = Column(Integer, ForeignKey("pairs.id"), nullable=False)  # Updated to match your schema

    trade_count = Column(Integer, nullable=False, default=0)
    net_profit = Column(Numeric(18, 2), nullable=False, default=0)
    win_rate = Column(Numeric(5, 2), nullable=False, default=0)

    created_at = Column(DateTime, default=func.now())

    # Relationships
    strategy_metrics = relationship("StrategyMetrics", back_populates="trading_pair_metrics")
    trading_pair = relationship("PairDB")  # Updated to match your model names


class StrategyExchangeMetrics(Base):
    """Strategy performance metrics by exchange."""

    __tablename__ = "strategy_exchange_metrics"

    id = Column(Integer, primary_key=True)
    strategy_metrics_id = Column(Integer, ForeignKey("strategy_metrics.id"), nullable=False)
    exchange_id = Column(Integer, ForeignKey("exchanges.id"), nullable=False)

    trade_count = Column(Integer, nullable=False, default=0)
    net_profit = Column(Numeric(18, 2), nullable=False, default=0)
    win_rate = Column(Numeric(5, 2), nullable=False, default=0)

    created_at = Column(DateTime, default=func.now())

    # Relationships
    strategy_metrics = relationship("StrategyMetrics", back_populates="exchange_metrics")
    exchange = relationship("ExchangeDB")  # Updated to match your model names


# Add SQLAlchemy models for tables that need relationships
class PairDB(Base):
    """Database model for trading pairs."""

    __tablename__ = "pairs"

    id = Column(Integer, primary_key=True)
    base_currency = Column(String, nullable=False)
    quote_currency = Column(String, nullable=False)
    symbol = Column(String, nullable=False, unique=True)
    is_active = Column(Boolean, nullable=False, default=False)  # Added is_active with default false

    # Relationships
    trading_pair_metrics = relationship("StrategyTradingPairMetrics", back_populates="trading_pair")

    def __repr__(self):
        return f"<Pair(id={self.id}, symbol={self.symbol}, is_active={self.is_active})>"


class ExchangeDB(Base):
    """Database model for exchanges."""

    __tablename__ = "exchanges"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    api_rate_limit = Column(Integer, nullable=True)
    trade_fees = Column(Numeric(18, 8), nullable=True)
    rest_url = Column(String, nullable=True)
    ws_url = Column(String, nullable=True)
    delimiter = Column(String, nullable=True)
    withdrawal_fee = Column(String, nullable=True)  # JSON stored as string
    api_response_time = Column(Integer, nullable=True)
    mapping = Column(String, nullable=True)  # JSON stored as string
    additional_info = Column(String, nullable=True)  # JSON stored as string
    is_active = Column(Boolean, nullable=False, default=False)  # Added is_active with default false
    created_at = Column(DateTime, default=func.now())

    # Relationships
    exchange_metrics = relationship("StrategyExchangeMetrics", back_populates="exchange")

    def __repr__(self):
        return f"<Exchange(id={self.id}, name={self.name}, is_active={self.is_active})>"


# Update SQLAlchemy Strategy class with relationship to metrics
class StrategyDB(Base):
    """Database model for Strategy."""

    __tablename__ = "strategies"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    starting_capital = Column(Numeric(18, 2), nullable=False)
    min_spread = Column(Numeric(18, 8), nullable=False)
    is_active = Column(Boolean, default=False)  # Updated to default false
    # Other columns
    additional_info = Column(String, nullable=True)
    total_profit = Column(Numeric(18, 2), nullable=False, default=0)
    total_loss = Column(Numeric(18, 2), nullable=False, default=0)
    net_profit = Column(Numeric(18, 2), nullable=False, default=0)
    trade_count = Column(Integer, nullable=False, default=0)
    start_timestamp = Column(DateTime, default=func.now())
    last_updated = Column(DateTime, default=func.now(), onupdate=func.now())

    # Add relationship to metrics - update to match StrategyMetrics relationship
    metrics = relationship("StrategyMetrics", back_populates="strategy", foreign_keys=[StrategyMetrics.strategy_id])

    def __repr__(self):
        return f"<Strategy(id={self.id}, name={self.name}, is_active={self.is_active})>"
