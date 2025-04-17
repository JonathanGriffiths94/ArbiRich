"""
Trading models for the ArbiRich application.
This module contains models related to trading pairs and order books.
"""

from datetime import datetime
from decimal import Decimal
from typing import List, Optional

from pydantic import Field
from sqlalchemy.ext.asyncio import AsyncSession

from src.arbirich.models.base import BaseModel, StatusAwareModel


class TradingPair(StatusAwareModel):
    """Trading pair model combining database schema and config."""

    id: Optional[int] = None
    base_currency: str
    quote_currency: str
    symbol: str
    is_active: bool = False

    # Fields from schema
    min_qty: Decimal = Decimal("0.0")
    max_qty: Decimal = Decimal("0.0")
    price_precision: int = 8
    qty_precision: int = 8
    min_notional: Decimal = Decimal("0.0")
    enabled: bool = True

    def __init__(self, **data):
        # Pre-process to ensure symbol is set
        if "symbol" not in data or data["symbol"] is None:
            if "base_currency" in data and "quote_currency" in data:
                data["symbol"] = f"{data['base_currency']}-{data['quote_currency']}"
        super().__init__(**data)

    @classmethod
    def from_config(cls, config_model) -> "TradingPair":
        """Create a TradingPair from a TradingPairConfig."""
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
        # This would be implemented in a repository class
        pass

    @classmethod
    async def get_all_active(cls, db_session: AsyncSession) -> List["TradingPair"]:
        """Get all active trading pairs."""
        # This would be implemented in a repository class
        pass

    @classmethod
    async def get_by_currency(cls, db_session: AsyncSession, currency: str) -> List["TradingPair"]:
        """
        Get all trading pairs that include the specified currency
        as either the base or quote currency.
        """
        # This would be implemented in a repository class
        pass


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

    # Fields for DB schema
    exchange_id: Optional[int] = None
    trading_pair_id: Optional[int] = None
    sequence: Optional[int] = None
    hash_value: Optional[str] = None  # Renamed from hash to avoid conflict

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

    def get_spread(self) -> Optional[Decimal]:
        """Get the bid-ask spread."""
        best_bid = self.get_best_bid()
        best_ask = self.get_best_ask()
        if not best_bid or not best_ask:
            return None
        return best_ask.price - best_bid.price

    def get_spread_percentage(self) -> Optional[Decimal]:
        """Get the bid-ask spread as a percentage of the mid price."""
        spread = self.get_spread()
        mid_price = self.get_mid_price()
        if spread is None or mid_price is None or mid_price == 0:
            return None
        return (spread / mid_price) * 100
