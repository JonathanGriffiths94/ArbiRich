"""
Exchange-specific models for standardized data handling across connectors.

These models define common data structures for responses from exchange APIs,
ensuring consistent typing and validation across different exchange connectors.
"""

import time
import uuid
from datetime import datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from pydantic import Field, computed_field
from sqlalchemy.ext.asyncio import AsyncSession

from src.arbirich.models.base import BaseModel, IdentifiableModel, StatusAwareModel, TimestampedModel
from src.arbirich.models.enums import ExchangeType, OrderSide, OrderType, TradeStatus

if TYPE_CHECKING:
    from src.arbirich.models.exchange_models import TradingPair


class Exchange(StatusAwareModel, TimestampedModel):
    """Exchange model with configuration and status."""

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

    # Fields from DB schema
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    paper_trading: bool = True
    has_websocket: bool = False
    has_rest: bool = True
    has_private_api: bool = False
    status: str = "online"
    version: Optional[str] = None
    description: Optional[str] = None

    # Fields not in DB but used in app
    enabled: bool = True

    @property
    def exchange_type(self) -> Optional[ExchangeType]:
        """Get the corresponding enum value for this exchange."""
        try:
            return ExchangeType(self.name.lower())
        except ValueError:
            return None

    @classmethod
    def from_config(cls, config_model) -> "Exchange":
        """Create an Exchange from an ExchangeConfig."""
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
        """Get exchange by name from database."""
        # This method would use the appropriate DB access code
        # Implementation would be in a repository class
        pass

    @classmethod
    async def get_all_active(cls, db_session: AsyncSession) -> List["Exchange"]:
        """Get all active exchanges from database."""
        # This method would use the appropriate DB access code
        # Implementation would be in a repository class
        pass

    def format_symbol(self, pair: Union["TradingPair", str]) -> str:
        """
        Format a trading pair symbol according to exchange requirements.
        Uses the exchange's delimiter and any symbol mappings.
        """
        if hasattr(pair, "symbol"):
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
    """Standard ticker data received from exchanges."""

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
    """Account balance for a specific currency."""

    currency: str
    available: float
    locked: Optional[float] = 0.0
    total: Optional[float] = None

    def model_post_init(self, __context) -> None:
        """Calculate total balance if not provided."""
        if self.total is None:
            self.total = self.available + self.locked


class AccountBalances(BaseModel):
    """Account balances for all currencies on an exchange."""

    exchange: str
    balances: Dict[str, ExchangeBalance]
    timestamp: float = Field(default_factory=time.time)
    raw_response: Optional[Dict[str, Any]] = None


class OrderStatus(str):
    """Standardized order statuses across exchanges."""

    OPEN = "open"
    PARTIAL = "partially_filled"
    FILLED = "filled"
    CANCELED = "canceled"
    REJECTED = "rejected"
    EXPIRED = "expired"
    UNKNOWN = "unknown"


class ExchangeOrder(BaseModel):
    """Order information from an exchange."""

    id: str
    exchange: str
    symbol: str
    side: OrderSide
    order_type: OrderType
    price: Optional[float] = None
    amount: float
    filled: float = 0.0
    remaining: Optional[float] = None
    status: str = OrderStatus.OPEN
    timestamp: float = Field(default_factory=time.time)
    average_price: Optional[float] = None
    fees: Optional[Dict[str, float]] = None
    raw_response: Optional[Dict[str, Any]] = None

    def model_post_init(self, __context) -> None:
        """Calculate remaining amount if not provided."""
        if self.remaining is None:
            self.remaining = self.amount - self.filled


class OrderResponse(BaseModel):
    """Response from creating, cancelling or querying an order."""

    success: bool
    order: Optional[ExchangeOrder] = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    timestamp: float = Field(default_factory=time.time)
    raw_response: Optional[Dict[str, Any]] = None


class CandlestickData(BaseModel):
    """Single candlestick (OHLCV) data."""

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


class ExchangeInfo(BaseModel):
    """Exchange information including capabilities and status."""

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
    """Information about a trading symbol/pair."""

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
    """Message received from a websocket connection."""

    exchange: str
    type: str  # 'ticker', 'orderbook', 'trade', etc.
    data: Dict[str, Any]
    timestamp: float = Field(default_factory=time.time)
    raw_message: Optional[str] = None


class TradeExecutionResult(IdentifiableModel):
    """Result of a trade execution."""

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

    # Fields from DB schema
    strategy_id: Optional[int] = None
    trade_execution_id: Optional[str] = None
    exchange_id: Optional[int] = None
    trading_pair_id: Optional[int] = None
