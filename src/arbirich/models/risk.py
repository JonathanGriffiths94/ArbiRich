"""
Risk-related models for the ArbiRich application.
This module contains models related to risk management.
"""

from datetime import datetime
from typing import Any, Dict, Optional

from src.arbirich.models.base import BaseModel


class RiskProfile(BaseModel):
    """Risk profile settings for a strategy."""

    id: Optional[int] = None
    name: Optional[str] = None
    description: Optional[str] = None
    max_position_size_percentage: float = 10.0  # % of total capital
    max_drawdown_percentage: float = 5.0  # % of capital
    max_exposure_per_asset_percentage: float = 20.0  # % of capital per asset
    circuit_breaker_conditions: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class RiskAssessment(BaseModel):
    """Result of a risk assessment for a potential trade."""

    is_acceptable: bool
    risk_score: float  # 0-100, higher means more risky
    max_position_size: float
    reasons: Dict[str, Any] = {}
    recommendations: Dict[str, Any] = {}
