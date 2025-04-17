import time
import uuid
from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import UUID

from pydantic import Field, validator

from src.arbirich.models.base import BaseModel, TradeExecutionModel, TradeOpportunityModel, UUIDModel
from src.arbirich.models.enums import OrderSide, OrderType


class TradeRequest(BaseModel):
    """
    Model representing a trade request to be sent to an exchange.
    Contains all necessary information to execute a trade.
    """

    exchange: str
    symbol: str
    side: OrderSide
    price: Decimal = Field(..., description="Trade price")
    amount: Decimal = Field(..., description="Trade amount/quantity")
    order_type: OrderType = Field(OrderType.LIMIT, description="Type of order (market, limit, etc.)")
    strategy: str = Field(..., description="Strategy identifier that generated this trade")
    execution_id: UUID = Field(..., description="Unique identifier for this trade execution")

    # Optional fields
    client_order_id: Optional[str] = Field(None, description="Client-assigned order ID")
    time_in_force: Optional[str] = Field(None, description="Time in force setting (GTC, IOC, FOK)")
    leverage: Optional[int] = Field(None, description="Leverage for margin/futures trading")
    stop_price: Optional[Decimal] = Field(None, description="Stop price for stop/stop-limit orders")
    post_only: bool = Field(False, description="Whether the order should be post-only")
    reduce_only: bool = Field(False, description="Whether the order should only reduce position")

    # For internal tracking
    request_timestamp: float = Field(default_factory=time.time)
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

    class Config:
        """Configuration for the TradeRequest model."""

        validate_assignment = True
        arbitrary_types_allowed = True
        json_encoders = {Decimal: lambda v: float(v), UUID: lambda v: str(v)}

    @validator("client_order_id", pre=True, always=False)
    def set_client_order_id(cls, v):
        """Generate a client order ID if not provided."""
        if v is None:
            return f"arbi_{uuid.uuid4().hex[:16]}"
        return v


class Order(UUIDModel):
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


class TradeResultReference(BaseModel):
    """Reference to a trade execution result with side information."""

    result_id: str
    side: str  # 'buy' or 'sell'


class TradeResponse(BaseModel):
    """
    Response model for trade execution.
    """

    success: bool
    exchange: str
    symbol: str
    side: OrderSide
    order_type: OrderType
    price: Optional[Decimal] = None
    amount: Optional[Decimal] = None
    filled_amount: Optional[Decimal] = None
    order_id: Optional[str] = None
    client_order_id: Optional[str] = None
    status: str
    timestamp: float = Field(default_factory=time.time)
    error: Optional[str] = None
    error_code: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        """Configuration for the TradeResponse model."""

        validate_assignment = True
        arbitrary_types_allowed = True
        json_encoders = {Decimal: lambda v: float(v)}


class TradeOpportunity(TradeOpportunityModel):
    """A trading opportunity identified by a strategy."""

    strategy: str
    pair: str
    buy_exchange: str
    sell_exchange: str
    buy_price: float
    sell_price: float
    spread: float
    volume: float
    opportunity_key: Optional[str] = None

    # Fields from DB schema
    strategy_id: Optional[int] = None
    trading_pair_id: Optional[int] = None
    buy_exchange_id: Optional[int] = None
    sell_exchange_id: Optional[int] = None


class TradeExecution(TradeExecutionModel):
    """Model for a trade execution."""

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

    # Fields from DB schema
    # References to execution results through the mapping table
    result_refs: List[TradeResultReference] = Field(default_factory=list)
    strategy_id: Optional[int] = None
    trading_pair_id: Optional[int] = None
    buy_exchange_id: Optional[int] = None
    sell_exchange_id: Optional[int] = None
    execution_time_ms: Optional[int] = None
    success: bool = False
    partial: bool = False
    error: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
