import datetime
from typing import Dict, Optional

from sqlalchemy import JSON, Boolean, DateTime, Integer, String
from sqlalchemy.orm import mapped_column

from arbirich.models.base import BaseModel


class SystemHealthCheck(BaseModel):
    """System health check model that combines both DB and API needs."""

    id: Optional[int] = None
    timestamp: datetime.datetime
    database_healthy: bool
    redis_healthy: bool
    exchanges: Dict[str, str]  # Status of each exchange as {"exchange_name": "status"}
    overall_health: bool
    details: Optional[str] = None

    # DB representation fields
    @classmethod
    def get_db_columns(cls):
        return {
            "id": mapped_column(Integer, primary_key=True, nullable=False),
            "timestamp": mapped_column(DateTime, nullable=False),
            "database_healthy": mapped_column(Boolean, nullable=False),
            "redis_healthy": mapped_column(Boolean, nullable=False),
            "exchanges": mapped_column(JSON, nullable=False),
            "overall_health": mapped_column(Boolean, nullable=False),
            "details": mapped_column(String, nullable=True),
        }

    class Config:
        from_attributes = True
        schema_extra = {
            "example": {
                "timestamp": "2023-01-01T00:00:00",
                "database_healthy": True,
                "redis_healthy": True,
                "exchanges": {"binance": "healthy", "bybit": "healthy"},
                "overall_health": True,
                "details": "All systems operational",
            }
        }
