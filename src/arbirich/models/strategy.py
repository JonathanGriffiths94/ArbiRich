"""
Strategy models for the ArbiRich application.
This module contains models related to trading strategies and metrics.
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import Field

from src.arbirich.models.base import BaseModel
from src.arbirich.models.enums import StrategyType as EnumStrategyType
from src.arbirich.models.risk import RiskProfile


class StrategyParameters(BaseModel):
    """Parameters for a strategy."""

    id: Optional[int] = None
    strategy_id: int
    min_spread: float = 0.0001
    threshold: float = 0.0001
    max_slippage: Optional[float] = None
    min_volume: Optional[float] = None
    max_execution_time_ms: Optional[int] = None
    additional_parameters: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class StrategyTypeParameters(BaseModel):
    """Type-specific parameters for a strategy."""

    id: Optional[int] = None
    strategy_id: Optional[int] = None
    target_volume: Optional[float] = None
    min_depth: Optional[int] = None
    min_depth_percentage: Optional[float] = None
    parameters: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class StrategyTypeModel(BaseModel):
    """Model representing a strategy type."""

    id: Optional[int] = None
    name: str
    description: Optional[str] = None
    implementation_class: str
    created_at: Optional[datetime] = None

    @property
    def strategy_type_enum(self) -> Optional[EnumStrategyType]:
        """Get the corresponding enum value for this strategy type."""
        try:
            return EnumStrategyType(self.name)
        except ValueError:
            return None

    @classmethod
    def from_enum(cls, enum_value: EnumStrategyType, **kwargs) -> "StrategyTypeModel":
        """Create a StrategyTypeModel from an enum value."""
        return cls(name=enum_value.value, implementation_class=f"{enum_value.value.title()}Strategy", **kwargs)


class StrategyExecutionMapping(BaseModel):
    """Mapping between a strategy and execution strategy."""

    id: Optional[int] = None
    strategy_id: int
    execution_strategy_id: int
    is_active: bool = True
    priority: int = 100
    created_at: Optional[datetime] = None


class StrategyExchangePairMapping(BaseModel):
    """Mapping between a strategy, exchange and trading pair."""

    id: Optional[int] = None
    strategy_id: int
    exchange_id: int
    trading_pair_id: int
    is_active: bool = True
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    # Additional fields for convenience (not stored in database)
    exchange_name: Optional[str] = None
    pair_symbol: Optional[str] = None
    base_currency: Optional[str] = None
    quote_currency: Optional[str] = None


class Strategy(BaseModel):
    """A trading strategy."""

    id: Optional[int] = None
    name: str
    description: Optional[str] = None
    strategy_type_id: Optional[int] = None
    risk_profile_id: Optional[int] = None
    starting_capital: float
    total_profit: float = 0
    total_loss: float = 0
    net_profit: float = 0
    trade_count: int = 0
    start_timestamp: Optional[datetime] = None
    last_updated: Optional[datetime] = None
    is_active: bool = False
    created_by: Optional[str] = None

    # Common parameters (could be moved to parameters)
    min_spread: Optional[float] = None
    threshold: Optional[float] = None
    max_slippage: Optional[float] = None
    min_volume: Optional[float] = None

    # Relationships
    parameters: Optional[StrategyParameters] = None
    type_parameters: Optional[StrategyTypeParameters] = None
    risk_profile: Optional[RiskProfile] = None
    strategy_type: Optional[StrategyTypeModel] = None
    execution_mappings: List[StrategyExecutionMapping] = Field(default_factory=list)
    exchange_pair_mappings: List[StrategyExchangePairMapping] = Field(default_factory=list)

    additional_info: Optional[Dict[str, Any]] = None

    # Metrics (not stored directly in the strategy table)
    latest_metrics: Optional[Any] = Field(default=None, exclude=True)
    metrics: Optional[List[Any]] = Field(default_factory=list, exclude=True)

    def to_db_dict(self) -> dict:
        """Convert to a dictionary suitable for strategies table."""
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "strategy_type_id": self.strategy_type_id,
            "risk_profile_id": self.risk_profile_id,
            "starting_capital": self.starting_capital,
            "total_profit": self.total_profit,
            "total_loss": self.total_loss,
            "net_profit": self.net_profit,
            "trade_count": self.trade_count,
            "start_timestamp": self.start_timestamp,
            "last_updated": self.last_updated,
            "is_active": self.is_active,
            "created_by": self.created_by,
            "additional_info": json.dumps(self.additional_info) if self.additional_info else None,
        }
