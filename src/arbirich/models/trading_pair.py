from datetime import datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional

from pydantic import Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.arbirich.models.base import StatusAwareModel
from src.arbirich.models.db.schema import trading_pairs as trading_pairs_table
from src.arbirich.models.enums import TradingPairStatus


class TradingPair(StatusAwareModel):
    """Trading pair model combining database schema and config."""

    id: Optional[int] = None
    base_currency: str
    quote_currency: str
    symbol: str
    is_active: bool = False
    status: str = TradingPairStatus.INACTIVE
    status_reason: Optional[str] = None
    status_changed_at: Optional[datetime] = None

    # Fields from schema
    min_qty: Decimal = Decimal("0.0")
    max_qty: Decimal = Decimal("0.0")
    price_precision: int = 8
    qty_precision: int = 8
    min_notional: Decimal = Decimal("0.0")
    enabled: bool = True

    # Additional config fields
    strategy_specific_settings: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict, description="Strategy-specific settings for this trading pair"
    )
    supported_strategy_types: List[str] = Field(
        default_factory=list, description="List of strategy types supported for this pair"
    )
    exchange_settings: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict, description="Exchange-specific settings for this pair"
    )
    execution_constraints: Dict[str, Any] = Field(
        default_factory=dict, description="Execution constraints for this pair"
    )

    class Config:
        from_attributes = True

    def __init__(self, **data):
        # Pre-process to ensure symbol is set
        if "symbol" not in data or data["symbol"] is None:
            if "base_currency" in data and "quote_currency" in data:
                data["symbol"] = f"{data['base_currency']}-{data['quote_currency']}"
        super().__init__(**data)

    def to_db_dict(self) -> dict:
        """Convert to a dictionary suitable for trading_pairs table"""
        return {
            "id": self.id,
            "base_currency": self.base_currency,
            "quote_currency": self.quote_currency,
            "symbol": self.symbol,
            "is_active": self.is_active,
            "status": self.status,
            "status_reason": self.status_reason,
            "status_changed_at": self.status_changed_at,
        }

    @classmethod
    def from_config(cls, config_model) -> "TradingPair":
        """Create a TradingPair from a TradingPairConfig"""
        return cls(
            base_currency=config_model.base_currency,
            quote_currency=config_model.quote_currency,
            symbol=config_model.symbol
            if config_model.symbol
            else f"{config_model.base_currency}-{config_model.quote_currency}",
            is_active=config_model.enabled,
            min_qty=config_model.min_qty,
            max_qty=config_model.max_qty,
            price_precision=config_model.price_precision,
            qty_precision=config_model.qty_precision,
            min_notional=config_model.min_notional,
            enabled=config_model.enabled,
            strategy_specific_settings=config_model.strategy_specific_settings,
            supported_strategy_types=config_model.supported_strategy_types,
            exchange_settings=config_model.exchange_settings,
            execution_constraints=config_model.execution_constraints,
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
        Get all trading pairs that include the specified currency
        as either the base or quote currency.
        """
        query = select(trading_pairs_table).where(
            (trading_pairs_table.c.base_currency == currency) | (trading_pairs_table.c.quote_currency == currency)
        )
        result = await db_session.execute(query)
        return [cls(**row._mapping) for row in result.fetchall()]
