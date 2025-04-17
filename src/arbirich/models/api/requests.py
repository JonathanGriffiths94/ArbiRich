"""
API request models for the ArbiRich application.
These models are used to validate and structure the requests to API endpoints.
"""

from typing import Dict, List, Optional

from pydantic import Field

from src.arbirich.models.base import BaseModel


class TradingStopRequest(BaseModel):
    """Request model for stopping trading operations."""

    emergency: bool = False


class ExchangeCreateRequest(BaseModel):
    """Request model for creating a new exchange."""

    name: str
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    rest_url: Optional[str] = None
    ws_url: Optional[str] = None
    delimiter: str = ""
    trade_fees: float = 0.001
    api_rate_limit: int = 100
    paper_trading: bool = True
    enabled: bool = True


class TradingPairCreateRequest(BaseModel):
    """Request model for creating a new trading pair."""

    base_currency: str
    quote_currency: str
    min_qty: float = 0.0
    max_qty: float = 0.0
    price_precision: int = 8
    qty_precision: int = 8
    min_notional: float = 0.0
    enabled: bool = True


class StrategyCreateRequest(BaseModel):
    """Request model for creating a new strategy."""

    name: str
    type: str
    description: Optional[str] = None
    starting_capital: float = 10000.0
    min_spread: float = 0.0001
    threshold: float = 0.0001
    exchanges: List[str] = Field(default_factory=list)
    pairs: List[str] = Field(default_factory=list)
    risk_profile_id: Optional[int] = None
    enabled: bool = True
    additional_info: Optional[Dict] = None


class RiskProfileCreateRequest(BaseModel):
    """Request model for creating a new risk profile."""

    name: str
    description: Optional[str] = None
    max_position_size_percentage: float = 10.0
    max_drawdown_percentage: float = 5.0
    max_exposure_per_asset_percentage: float = 20.0
    circuit_breaker_conditions: Optional[Dict] = None
