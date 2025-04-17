"""
Order book models for the ArbiRich application.
This module contains models related to order book updates and state.
"""

import hashlib
import json
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import Field

from src.arbirich.models.base import BaseModel, IdentifiableModel


class OrderLevel(BaseModel):
    """A single level in an order book (price and quantity)."""

    price: float
    quantity: float


class OrderBookUpdate(IdentifiableModel):
    """An update to an order book."""

    exchange: str
    symbol: str
    bids: Dict[float, float] = Field(default_factory=dict)
    asks: Dict[float, float] = Field(default_factory=dict)
    timestamp: float

    # Fields from DB schema
    exchange_id: Optional[int] = None
    trading_pair_id: Optional[int] = None
    sequence: Optional[int] = None
    hash_value: Optional[str] = None  # Renamed from hash to avoid conflict

    # Change from computed_field to a regular method to avoid conflict
    def compute_hash(self) -> str:
        """Compute a hash of the order book state."""
        bids_json = json.dumps(self.bids, sort_keys=True)
        asks_json = json.dumps(self.asks, sort_keys=True)
        combined = bids_json + asks_json
        return hashlib.sha256(combined.encode("utf-8")).hexdigest()

    # Get hash_value or compute it if not available
    def get_hash(self) -> str:
        """Get the cached hash value or compute a new one."""
        if self.hash_value:
            return self.hash_value
        return self.compute_hash()

    def get_best_bid(self) -> Optional[OrderLevel]:
        """Get the highest bid price and quantity."""
        if not self.bids:
            return None
        best_price = max(self.bids.keys())
        return OrderLevel(price=best_price, quantity=self.bids[best_price])

    def get_best_ask(self) -> Optional[OrderLevel]:
        """Get the lowest ask price and quantity."""
        if not self.asks:
            return None
        best_price = min(self.asks.keys())
        return OrderLevel(price=best_price, quantity=self.asks[best_price])

    def get_mid_price(self) -> Optional[float]:
        """Get the mid price between best bid and best ask."""
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        if not best_bid or not best_ask:
            return None
        return (best_bid.price + best_ask.price) / 2

    def get_spread(self) -> Optional[float]:
        """Get the bid-ask spread."""
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        if not best_bid or not best_ask:
            return None
        return best_ask.price - best_bid.price

    def get_spread_percentage(self) -> Optional[float]:
        """Get the bid-ask spread as a percentage of the mid price."""
        spread = self.get_spread()
        mid_price = self.get_mid_price()
        if spread is None or mid_price is None or mid_price == 0:
            return None
        return (spread / mid_price) * 100


class TradeOpportunity(IdentifiableModel):
    strategy: str
    pair: str
    buy_exchange: str
    sell_exchange: str
    buy_price: float
    sell_price: float
    spread: float
    volume: float
    opportunity_timestamp: float = Field(default_factory=lambda: datetime.now().timestamp())
    opportunity_key: Optional[str] = None


class TradeExecution(IdentifiableModel):
    opportunity_id: Optional[str] = None
    strategy: str
    pair: str
    buy_exchange: str
    sell_exchange: str
    executed_buy_price: float
    executed_sell_price: float
    spread: float
    volume: float
    profit: Optional[float] = None
    status: str = "completed"
    execution_timestamp: float = Field(default_factory=lambda: datetime.now().timestamp())
    buy_execution_result_id: Optional[str] = None
    sell_execution_result_id: Optional[str] = None


class MarketDepthLevel(BaseModel):
    """Single level in an order book."""

    price: float
    amount: float


class MarketDepth(BaseModel):
    """Full market depth data with bids and asks."""

    exchange: str
    symbol: str
    bids: List[MarketDepthLevel]
    asks: List[MarketDepthLevel]
    timestamp: float = Field(default_factory=time.time)
    raw_data: Optional[Dict[str, Any]] = None


class OrderBookState(BaseModel):
    """Collection of order books indexed by symbol and exchange."""

    symbols: Dict[str, Dict[str, OrderBookUpdate]] = Field(default_factory=dict)
    strategy: Optional[str] = None
    threshold: float = 0.001

    @property
    def prices(self) -> set:
        """Get all unique prices in the order books."""
        result = set()
        for asset, exch_dict in self.symbols.items():
            for exchange, order_book in exch_dict.items():
                for price in list(order_book.bids.keys()) + list(order_book.asks.keys()):
                    result.add(price)
        return result


class ExecutionResult(IdentifiableModel):
    """Result of an order execution."""

    exchange: str
    symbol: str
    side: str  # buy or sell
    order_id: str
    price: float
    quantity: float
    filled_quantity: float = 0.0
    status: str  # pending, filled, partially_filled, canceled, rejected
    timestamp: float = Field(default_factory=time.time)
    fee: Optional[float] = None
    fee_currency: Optional[str] = None
    error_message: Optional[str] = None
    raw_response: Optional[Dict[str, Any]] = None

    # Fields from DB schema
    exchange_id: Optional[int] = None
    trading_pair_id: Optional[int] = None
    strategy_id: Optional[int] = None


class ExecutionStrategy(IdentifiableModel):
    """Model representing a trading/execution strategy configuration."""

    name: str
    description: Optional[str] = None
    parameters: Dict[str, Any] = Field(default_factory=dict)
    symbols: List[str] = Field(default_factory=list)
    exchanges: List[str] = Field(default_factory=list)
    is_active: bool = True
    created_at: float = Field(default_factory=time.time)
    updated_at: Optional[float] = None

    # Configuration parameters commonly used in trading strategies
    threshold: Optional[float] = None  # Minimum profit threshold
    max_position_size: Optional[float] = None  # Maximum position size
    risk_factor: float = 1.0  # Risk adjustment factor
    fee_rate: float = 0.0  # Expected fee rate
    slippage: float = 0.0  # Expected slippage

    # Fields from DB schema
    user_id: Optional[int] = None


class Order(IdentifiableModel):
    """Model representing a trading order."""

    exchange: str
    symbol: str
    order_id: Optional[str] = None  # Exchange-assigned order ID
    client_order_id: Optional[str] = None  # Client-assigned order ID
    side: str  # "buy" or "sell"
    order_type: str  # "market", "limit", etc.
    price: Optional[float] = None  # Required for limit orders
    quantity: float
    time_in_force: Optional[str] = None  # "GTC", "IOC", "FOK", etc.
    status: str = "new"  # "new", "open", "filled", "canceled", "rejected"
    created_at: float = Field(default_factory=time.time)
    updated_at: Optional[float] = None
    filled_quantity: float = 0.0
    average_price: Optional[float] = None

    # Fields from DB schema
    exchange_id: Optional[int] = None
    trading_pair_id: Optional[int] = None
    strategy_id: Optional[int] = None
    user_id: Optional[int] = None


# Conversion functions
def order_book_to_market_depth(order_book: OrderBookUpdate) -> MarketDepth:
    """Convert an OrderBookUpdate to a MarketDepth model."""
    bids = [MarketDepthLevel(price=price, amount=amount) for price, amount in order_book.bids.items()]
    asks = [MarketDepthLevel(price=price, amount=amount) for price, amount in order_book.asks.items()]

    # Sort bids in descending order and asks in ascending order
    bids.sort(key=lambda x: x.price, reverse=True)
    asks.sort(key=lambda x: x.price)

    return MarketDepth(
        exchange=order_book.exchange, symbol=order_book.symbol, bids=bids, asks=asks, timestamp=order_book.timestamp
    )


def market_depth_to_order_book(market_depth: MarketDepth) -> OrderBookUpdate:
    """Convert a MarketDepth to an OrderBookUpdate model."""
    bids_dict = {level.price: level.amount for level in market_depth.bids}
    asks_dict = {level.price: level.amount for level in market_depth.asks}

    return OrderBookUpdate(
        exchange=market_depth.exchange,
        symbol=market_depth.symbol,
        bids=bids_dict,
        asks=asks_dict,
        timestamp=market_depth.timestamp,
    )
