from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class HealthResponse(BaseModel):
    api: str
    redis: str
    database: str


class StatusResponse(BaseModel):
    status: str
    version: str
    timestamp: str
    environment: str
    components: Dict[str, str]
    processes: List[Dict[str, str]]
    active_strategies: List[str]
    active_pairs: List[str]
    active_exchanges: List[str]
    active_trading: bool
    trading_start_time: Optional[str] = None
    trading_stop_time: Optional[str] = None
    trading_stop_reason: Optional[str] = None
    trading_stop_emergency: Optional[bool] = None


class TradingStopRequest(BaseModel):
    emergency: bool = False
    reason: Optional[str] = None


class TradingStatusResponse(BaseModel):
    success: bool
    message: str
    status: Optional[str] = None
    overall: Optional[bool] = None
    components: Optional[Dict[str, str]] = None
    data: Optional[Dict[str, Any]] = None


class ChartDataset(BaseModel):
    label: str
    data: List[float]
    borderColor: str
    backgroundColor: str
    fill: bool = False


class ChartData(BaseModel):
    labels: List[str]
    datasets: List[ChartDataset]


class DashboardStats(BaseModel):
    total_profit: float = 0.0
    total_trades: int = 0
    win_rate: float = 0.0
    executions_24h: int = 0
    opportunities_count: int = 0
    active_strategies: int = 0
