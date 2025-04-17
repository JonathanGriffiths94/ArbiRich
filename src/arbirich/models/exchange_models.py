"""
Exchange-specific models for standardized data handling across connectors.

These models define common data structures for responses from exchange APIs,
ensuring consistent typing and validation across different exchange connectors.
"""

import logging
import time
import uuid
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, computed_field, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from arbirich.models.db.schema import exchanges as exchanges_table
from arbirich.models.db.schema import trading_pairs as trading_pairs_table
from src.arbirich.models.enums import ExchangeType, OrderSide, OrderType, TradeStatus

if TYPE_CHECKING:
    from src.arbirich.models.order_book import OrderBookUpdate

logger = logging.getLogger(__name__)


# Base models for common API operations
class Exchange(BaseModel):
    """Comprehensive exchange model combining database schema and config"""

    id: Optional[int] = None
    name: str
    api_rate_limit: Optional[int] = None
    trade_fees: Optional[Decimal] = None
    rest_url: Optional[str] = None
    ws_url: Optional[str] = None
    delimiter: Optional[str] = None
    withdrawal_fee: Dict[str, float] = Field(default_factory=dict)
    api_response_time: Optional[int] = None
    mapping: Dict[str, str] = Field(default_factory=dict)
    additional_info: Dict[str, Any] = Field(default_factory=dict)
    is_active: bool = False
    created_at: Optional[datetime] = None

    # Fields from ExchangeConfig but not in database schema
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    enabled: bool = True
    paper_trading: bool = True

    # Reference to exchange type enum
    @property
    def exchange_type(self) -> Optional[ExchangeType]:
        """Get the corresponding enum value for this exchange"""
        try:
            return ExchangeType(self.name.lower())
        except ValueError:
            logger.warning(f"No matching ExchangeType for {self.name}")
            return None

    class Config:
        from_attributes = True

    def to_db_dict(self) -> dict:
        """Convert to a dictionary suitable for exchanges table"""
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
            "additional_info": self.additional_info,
            "is_active": self.is_active,
            "created_at": self.created_at,
        }

    @classmethod
    def from_config(cls, config_model) -> "Exchange":
        """Create an Exchange from an ExchangeConfig"""
        return cls(
            name=config_model.name,
            api_rate_limit=config_model.api_rate_limit,
            trade_fees=config_model.trade_fees,
            rest_url=config_model.rest_url,
            ws_url=config_model.ws_url,
            delimiter=config_model.delimiter,
            withdrawal_fee=config_model.withdrawal_fee,
            api_response_time=config_model.api_response_time,
            mapping=config_model.mapping,
            additional_info=config_model.additional_info,
            is_active=config_model.enabled,
            api_key=config_model.api_key,
            api_secret=config_model.api_secret,
            enabled=config_model.enabled,
            paper_trading=config_model.paper_trading,
        )

    @classmethod
    async def get_by_name(cls, db_session: AsyncSession, name: str) -> Optional["Exchange"]:
        """Get exchange by name."""
        query = select(exchanges_table).where(exchanges_table.c.name == name)
        result = await db_session.execute(query)
        row = result.fetchone()
        if not row:
            return None
        return cls(**row._mapping)

    @classmethod
    async def get_all_active(cls, db_session: AsyncSession) -> List["Exchange"]:
        """Get all active exchanges."""
        query = select(exchanges_table).where(exchanges_table.c.is_active)
        result = await db_session.execute(query)
        return [cls(**row._mapping) for row in result.fetchall()]

    def format_symbol(self, pair: Union["TradingPair", str]) -> str:
        """
        Format a trading pair symbol according to exchange requirements.
        Uses the exchange's delimiter and any symbol mappings.
        """
        if isinstance(pair, TradingPair):
            symbol = pair.symbol
        else:
            symbol = pair

        # Split symbol into base and quote currencies if it contains '-'
        if "-" in symbol:
            base, quote = symbol.split("-")

            # Apply any mappings from the exchange configuration
            base = self.mapping.get(base, base)
            quote = self.mapping.get(quote, quote)

            # Format with the exchange's delimiter
            if self.delimiter:
                return f"{base}{self.delimiter}{quote}"
            return f"{base}{quote}"

        # If symbol doesn't contain '-', assume it's already formatted
        return symbol

    def normalize_symbol(self, exchange_symbol: str) -> str:
        """
        Convert an exchange-specific symbol to the standard format (BASE-QUOTE).
        """
        # Try to find a matching symbol in the mapping (reverse lookup)
        reverse_mapping = {v: k for k, v in self.mapping.items()}

        # If delimiter is present, split by delimiter
        if self.delimiter and self.delimiter in exchange_symbol:
            base, quote = exchange_symbol.split(self.delimiter)
            base = reverse_mapping.get(base, base)
            quote = reverse_mapping.get(quote, quote)
            return f"{base}-{quote}"

        # Try to match known currency pairs
        for std_symbol in self.mapping.values():
            if std_symbol.lower() == exchange_symbol.lower():
                return std_symbol

        # If we can't determine base/quote, return as is
        return exchange_symbol


class TickerData(BaseModel):
    """
    Standard ticker data received from exchanges.
    """

    symbol: str
    bid: float
    ask: float
    bid_size: Optional[float] = None
    ask_size: Optional[float] = None
    last: Optional[float] = None
    volume: Optional[float] = None
    timestamp: float = Field(default_factory=time.time)
    exchange: str

    @computed_field
    def spread(self) -> float:
        """Calculate the spread between bid and ask prices."""
        return self.ask - self.bid

    @computed_field
    def spread_percentage(self) -> float:
        """Calculate the spread as a percentage of the mid price."""
        mid = (self.ask + self.bid) / 2
        return (self.ask - self.bid) / mid * 100 if mid > 0 else 0


class ExchangeBalance(BaseModel):
    """
    Account balance for a specific currency.
    """

    currency: str
    available: float
    locked: Optional[float] = 0.0
    total: Optional[float] = None

    @field_validator("total", mode="before")
    @classmethod
    def calculate_total(cls, v: Optional[float], info: Dict[str, Any]) -> float:
        """Calculate total balance if not provided"""
        if v is not None:
            return v
        return info.data.get("available", 0.0) + info.data.get("locked", 0.0)


class AccountBalances(BaseModel):
    """
    Account balances for all currencies on an exchange.
    """

    exchange: str
    balances: Dict[str, ExchangeBalance]
    timestamp: float = Field(default_factory=time.time)
    raw_response: Optional[Dict[str, Any]] = None


class OrderStatus(str, Enum):
    """
    Standardized order statuses across exchanges.
    """

    OPEN = "open"
    PARTIAL = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    UNKNOWN = "unknown"


class ExchangeOrder(BaseModel):
    """
    Order information from an exchange.
    """

    id: str
    exchange: str
    symbol: str
    side: OrderSide
    order_type: OrderType
    price: Optional[float] = None
    amount: float
    filled: float = 0
    remaining: float = Field(...)
    status: OrderStatus = OrderStatus.OPEN
    timestamp: float = Field(default_factory=time.time)
    average_price: Optional[float] = None
    fees: Optional[Dict[str, float]] = None
    raw_response: Optional[Dict[str, Any]] = None

    @field_validator("remaining", mode="before")
    @classmethod
    def calculate_remaining(cls, v: Optional[float], info: Dict[str, Any]) -> float:
        """Calculate remaining amount if not provided."""
        if v is not None:
            return v
        return info.data.get("amount", 0.0) - info.data.get("filled", 0.0)


class OrderResponse(BaseModel):
    """
    Response from creating, cancelling or querying an order.
    """

    success: bool
    order: Optional[ExchangeOrder] = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    timestamp: float = Field(default_factory=time.time)
    raw_response: Optional[Dict[str, Any]] = None


class CandlestickData(BaseModel):
    """
    Single candlestick (OHLCV) data.
    """

    exchange: str
    symbol: str
    timestamp: float
    open: float
    high: float
    low: float
    close: float
    volume: float
    trades: Optional[int] = None
    interval: str  # e.g., '1m', '5m', '1h', '1d'


class MarketDepthLevel(BaseModel):
    """
    Single level in an order book.
    """

    price: float
    amount: float


class MarketDepth(BaseModel):
    """
    Full market depth data with bids and asks.
    """

    exchange: str
    symbol: str
    bids: List[MarketDepthLevel]
    asks: List[MarketDepthLevel]
    timestamp: float = Field(default_factory=time.time)
    raw_data: Optional[Dict[str, Any]] = None


class OrderBookEntry(BaseModel):
    """Represents a single entry in an order book (price and quantity pair)."""

    price: Decimal
    quantity: Decimal


class OrderBook(BaseModel):
    """Represents an order book snapshot for a trading pair on an exchange."""

    exchange: str
    symbol: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    bids: List[OrderBookEntry] = Field(default_factory=list)
    asks: List[OrderBookEntry] = Field(default_factory=list)

    def get_best_bid(self) -> Optional[OrderBookEntry]:
        """Get the highest bid in the order book."""
        if not self.bids:
            return None
        return max(self.bids, key=lambda bid: bid.price)

    def get_best_ask(self) -> Optional[OrderBookEntry]:
        """Get the lowest ask in the order book."""
        if not self.asks:
            return None
        return min(self.asks, key=lambda ask: ask.price)

    def get_mid_price(self) -> Optional[Decimal]:
        """Calculate the mid-price between best bid and best ask."""
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        if not best_bid or not best_ask:
            return None
        return (best_bid.price + best_ask.price) / 2


class TradingPair(BaseModel):
    """Trading pair model combining database schema and config"""

    id: Optional[int] = None
    base_currency: str
    quote_currency: str
    symbol: str
    is_active: bool = False

    # Fields from TradingPairConfig but not in database schema
    min_qty: float = 0.0
    max_qty: float = 0.0
    price_precision: int = 8
    qty_precision: int = 8
    min_notional: float = 0.0
    enabled: bool = True

    class Config:
        from_attributes = True

    def to_db_dict(self) -> dict:
        """Convert to a dictionary suitable for trading_pairs table"""
        return {
            "id": self.id,
            "base_currency": self.base_currency,
            "quote_currency": self.quote_currency,
            "symbol": self.symbol,
            "is_active": self.is_active,
        }

    @classmethod
    def from_config(cls, config_model) -> "TradingPair":
        """Create a TradingPair from a TradingPairConfig"""
        return cls(
            base_currency=config_model.base_currency,
            quote_currency=config_model.quote_currency,
            symbol=config_model.symbol,
            is_active=config_model.enabled,
            min_qty=config_model.min_qty,
            max_qty=config_model.max_qty,
            price_precision=config_model.price_precision,
            qty_precision=config_model.qty_precision,
            min_notional=config_model.min_notional,
            enabled=config_model.enabled,
        )

    @classmethod
    async def get_by_symbol(cls, db_session: AsyncSession, symbol: str) -> Optional["TradingPair"]:
        """Get trading pair by symbol."""
        query = select(trading_pairs_table).where(trading_pairs_table.c.symbol == symbol)
        result = await db_session.execute(query)
        row = result.fetchone()
        if not row:
            return None
        return cls(**row._mapping)

    @classmethod
    async def get_all_active(cls, db_session: AsyncSession) -> List["TradingPair"]:
        """Get all active trading pairs."""
        query = select(trading_pairs_table).where(trading_pairs_table.c.is_active)
        result = await db_session.execute(query)
        return [cls(**row._mapping) for row in result.fetchall()]

    @classmethod
    async def get_by_currency(cls, db_session: AsyncSession, currency: str) -> List["TradingPair"]:
        """
        Get all trading pairs (active or inactive) that include the specified currency
        as either the base or quote currency.

        Args:
            db_session: The database session
            currency: The currency code to search for

        Returns:
            List of trading pairs containing the specified currency
        """
        query = select(trading_pairs_table).where(
            (trading_pairs_table.c.base_currency == currency) | (trading_pairs_table.c.quote_currency == currency)
        )
        result = await db_session.execute(query)
        return [cls(**row._mapping) for row in result.fetchall()]


class TradeExecutionResult(BaseModel):
    """
    Result of a trade execution.
    """

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    exchange: str
    symbol: str
    side: OrderSide
    order_type: OrderType
    requested_amount: float
    executed_amount: float
    price: Optional[float] = None
    average_price: Optional[float] = None
    status: TradeStatus
    timestamp: float = Field(default_factory=time.time)
    fees: Optional[Dict[str, float]] = None
    trade_id: Optional[str] = None
    order_id: Optional[str] = None
    error: Optional[str] = None
    raw_response: Optional[Dict[str, Any]] = None


class ExchangeInfo(BaseModel):
    """
    Exchange information including capabilities and status.
    """

    name: str
    trading_pairs: List[str]
    timeframes: Optional[List[str]] = None
    has_websocket: bool = False
    has_rest: bool = True
    has_private_api: bool = False
    rate_limit: Optional[int] = None
    status: str = "online"
    url: Optional[str] = None
    version: Optional[str] = None
    description: Optional[str] = None


class SymbolInfo(BaseModel):
    """
    Information about a trading symbol/pair.
    """

    exchange: str
    symbol: str
    base_asset: str
    quote_asset: str
    min_price: Optional[float] = None
    max_price: Optional[float] = None
    price_precision: int = 8
    min_amount: Optional[float] = None
    max_amount: Optional[float] = None
    amount_precision: int = 8
    min_notional: Optional[float] = None
    fee_rate: Optional[float] = None
    is_active: bool = True
    additional_info: Optional[Dict[str, Any]] = None


class WebsocketMessage(BaseModel):
    """
    Message received from a websocket connection.
    """

    exchange: str
    type: str  # 'ticker', 'orderbook', 'trade', etc.
    data: Dict[str, Any]
    timestamp: float = Field(default_factory=time.time)
    raw_message: Optional[str] = None


# Conversion functions
def order_book_update_to_market_depth(order_book: "OrderBookUpdate") -> MarketDepth:
    """
    Convert an OrderBookUpdate to a MarketDepth model.

    Args:
        order_book: The OrderBookUpdate instance

    Returns:
        A MarketDepth instance with the same data
    """
    bids = [MarketDepthLevel(price=price, amount=amount) for price, amount in order_book.bids.items()]
    asks = [MarketDepthLevel(price=price, amount=amount) for price, amount in order_book.asks.items()]

    # Sort bids in descending order and asks in ascending order
    bids.sort(key=lambda x: x.price, reverse=True)
    asks.sort(key=lambda x: x.price)

    return MarketDepth(
        exchange=order_book.exchange, symbol=order_book.symbol, bids=bids, asks=asks, timestamp=order_book.timestamp
    )


def market_depth_to_order_book_update(market_depth: MarketDepth) -> "OrderBookUpdate":
    """
    Convert a MarketDepth to an OrderBookUpdate model.

    Args:
        market_depth: The MarketDepth instance

    Returns:
        An OrderBookUpdate instance with the same data
    """
    from src.arbirich.models.order_book import OrderBookUpdate

    bids_dict = {level.price: level.amount for level in market_depth.bids}
    asks_dict = {level.price: level.amount for level in market_depth.asks}

    return OrderBookUpdate(
        id=str(uuid.uuid4()),
        exchange=market_depth.exchange,
        symbol=market_depth.symbol,
        bids=bids_dict,
        asks=asks_dict,
        timestamp=market_depth.timestamp,
    )
