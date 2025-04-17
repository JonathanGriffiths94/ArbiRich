import datetime
from typing import Optional

from src.arbirich.models.base import BaseModel


class StrategyMetrics(BaseModel):
    id: Optional[int] = None
    strategy_id: int
    period_start: datetime.datetime
    period_end: datetime.datetime
    win_count: int = 0
    loss_count: int = 0
    win_rate: float = 0.0
    gross_profit: float = 0.0
    gross_loss: float = 0.0
    net_profit: float = 0.0
    profit_factor: float = 0.0
    max_drawdown: float = 0.0
    max_drawdown_percentage: float = 0.0
    avg_profit_per_trade: float = 0.0
    avg_loss_per_trade: float = 0.0
    risk_reward_ratio: float = 0.0
    total_volume: float = 0.0
    avg_volume_per_trade: float = 0.0
    avg_hold_time_seconds: int = 0
    market_volatility: Optional[float] = None
    correlation_to_market: Optional[float] = None
    created_at: Optional[datetime.datetime] = None
    updated_at: Optional[datetime.datetime] = None


class StrategyTradingPairMetrics(BaseModel):
    id: Optional[int] = None
    strategy_metrics_id: int
    trading_pair_id: int
    trade_count: int = 0
    net_profit: float = 0.0
    win_rate: float = 0.0
    created_at: Optional[datetime.datetime] = None

    # Display fields
    trading_pair: Optional[str] = None
    total_profit: Optional[float] = None
    success_rate: Optional[float] = None
    average_profit: Optional[float] = None
    execution_count: Optional[int] = None


class StrategyExchangeMetrics(BaseModel):
    id: Optional[int] = None
    strategy_metrics_id: int
    exchange_id: int
    trade_count: int = 0
    net_profit: float = 0.0
    win_rate: float = 0.0
    created_at: Optional[datetime.datetime] = None

    # Display fields
    exchange_name: Optional[str] = None
    total_profit: Optional[float] = None
    success_rate: Optional[float] = None
    average_profit: Optional[float] = None
    execution_count: Optional[int] = None


class StrategyMarketConditionPerformance(BaseModel):
    id: Optional[int] = None
    strategy_id: int
    market_condition: str
    exchange_id: int
    trading_pair_id: int
    trade_count: int = 0
    profit: float = 0.0
    win_rate: float = 0.0
    avg_profit_per_trade: float = 0.0
    created_at: Optional[datetime.datetime] = None
    updated_at: Optional[datetime.datetime] = None
