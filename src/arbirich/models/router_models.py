from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class StatusResponse(BaseModel):
    """Response model for the status endpoint."""

    status: str
    version: str
    timestamp: str
    environment: str
    components: Dict[str, str] = {}


class HealthResponse(BaseModel):
    """Response model for the health check endpoint."""

    api: str
    redis: str
    database: str


class TradingStatusResponse(BaseModel):
    """Standard response with status and message"""

    success: bool
    message: str
    data: Optional[Dict[str, Any]] = None


class DashboardStats(BaseModel):
    """Dashboard statistics model."""

    total_profit: float = 0.0
    total_trades: int = 0
    win_rate: float = 0.0
    executions_24h: int = 0
    opportunities_count: int = 0
    active_strategies: int = 0


class ChartDataset(BaseModel):
    """Chart dataset model."""

    label: str
    data: List[float]
    borderColor: str
    backgroundColor: str
    fill: bool = False
    tension: float = 0.4


class ChartData(BaseModel):
    """Chart data model."""

    labels: List[str]
    datasets: List[ChartDataset]


class TradingStopRequest(BaseModel):
    """Request model for stopping trading operations."""

    emergency: bool = False
