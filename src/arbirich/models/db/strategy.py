"""
SQLAlchemy models for strategies in the ArbiRich application.
"""

import json

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, Numeric, String, func
from sqlalchemy.orm import relationship

from src.arbirich.models.db.base import Base


class StrategyType(Base):
    """Database model for strategy types."""

    __tablename__ = "strategy_types"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    description = Column(String, nullable=True)
    implementation_class = Column(String, nullable=False)
    created_at = Column(DateTime, default=func.now())

    # Relationships
    strategies = relationship("Strategy", back_populates="strategy_type")

    def __repr__(self):
        return f"<StrategyType(id={self.id}, name={self.name})>"


class RiskProfile(Base):
    """Database model for risk profiles."""

    __tablename__ = "risk_profiles"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    description = Column(String, nullable=True)
    max_position_size_percentage = Column(Numeric(5, 2))
    max_drawdown_percentage = Column(Numeric(5, 2))
    max_exposure_per_asset_percentage = Column(Numeric(5, 2))
    circuit_breaker_conditions = Column(String)  # JSON stored as string
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    strategies = relationship("Strategy", back_populates="risk_profile")

    def __repr__(self):
        return f"<RiskProfile(id={self.id}, name={self.name})>"

    def to_dict(self):
        """Convert risk profile to dictionary."""
        result = {c.name: getattr(self, c.name) for c in self.__table__.columns}

        # Convert JSON strings to dictionaries
        if result["circuit_breaker_conditions"]:
            try:
                result["circuit_breaker_conditions"] = json.loads(result["circuit_breaker_conditions"])
            except json.JSONDecodeError:
                result["circuit_breaker_conditions"] = {}

        return result


class Strategy(Base):
    """Database model for Strategy."""

    __tablename__ = "strategies"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    description = Column(String, nullable=True)
    strategy_type_id = Column(Integer, ForeignKey("strategy_types.id"), nullable=False)
    risk_profile_id = Column(Integer, ForeignKey("risk_profiles.id"), nullable=False)
    starting_capital = Column(Numeric(18, 2), nullable=False)
    is_active = Column(Boolean, default=False)
    additional_info = Column(String, nullable=True)  # JSON stored as string
    total_profit = Column(Numeric(18, 2), nullable=False, default=0)
    total_loss = Column(Numeric(18, 2), nullable=False, default=0)
    net_profit = Column(Numeric(18, 2), nullable=False, default=0)
    trade_count = Column(Integer, nullable=False, default=0)
    start_timestamp = Column(DateTime, default=func.now())
    last_updated = Column(DateTime, default=func.now(), onupdate=func.now())
    created_by = Column(String, nullable=True)

    # Relationships
    strategy_type = relationship("StrategyType", back_populates="strategies")
    risk_profile = relationship("RiskProfile", back_populates="strategies")
    parameters = relationship("StrategyParameters", back_populates="strategy", uselist=False)
    type_parameters = relationship("StrategyTypeParameters", back_populates="strategy", uselist=False)
    metrics = relationship("StrategyMetrics", back_populates="strategy", cascade="all, delete-orphan")
    execution_mappings = relationship(
        "StrategyExecutionMapping", back_populates="strategy", cascade="all, delete-orphan"
    )
    exchange_pair_mappings = relationship(
        "StrategyExchangePairMapping", back_populates="strategy", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<Strategy(id={self.id}, name={self.name}, is_active={self.is_active})>"

    def to_dict(self):
        """Convert strategy to dictionary."""
        result = {c.name: getattr(self, c.name) for c in self.__table__.columns}

        # Convert JSON strings to dictionaries
        if result["additional_info"]:
            try:
                result["additional_info"] = json.loads(result["additional_info"])
            except json.JSONDecodeError:
                result["additional_info"] = {}

        return result


class StrategyParameters(Base):
    """Database model for strategy parameters."""

    __tablename__ = "strategy_parameters"

    id = Column(Integer, primary_key=True)
    strategy_id = Column(Integer, ForeignKey("strategies.id"), nullable=False)
    min_spread = Column(Numeric(18, 8), nullable=False)
    threshold = Column(Numeric(18, 8), nullable=False)
    max_slippage = Column(Numeric(18, 8))
    min_volume = Column(Numeric(18, 8))
    max_execution_time_ms = Column(Integer)
    additional_parameters = Column(String)  # JSON stored as string
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    strategy = relationship("Strategy", back_populates="parameters")

    def __repr__(self):
        return f"<StrategyParameters(id={self.id}, strategy_id={self.strategy_id})>"

    def to_dict(self):
        """Convert strategy parameters to dictionary."""
        result = {c.name: getattr(self, c.name) for c in self.__table__.columns}

        # Convert JSON strings to dictionaries
        if result["additional_parameters"]:
            try:
                result["additional_parameters"] = json.loads(result["additional_parameters"])
            except json.JSONDecodeError:
                result["additional_parameters"] = {}

        return result


class StrategyTypeParameters(Base):
    """Database model for strategy type parameters."""

    __tablename__ = "strategy_type_parameters"

    id = Column(Integer, primary_key=True)
    strategy_id = Column(Integer, ForeignKey("strategies.id"), nullable=False)
    target_volume = Column(Numeric(18, 8), nullable=True)
    min_depth = Column(Integer, nullable=True)
    min_depth_percentage = Column(Numeric(5, 2), nullable=True)
    parameters = Column(String, nullable=True)  # JSON stored as string
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    strategy = relationship("Strategy", back_populates="type_parameters")

    def __repr__(self):
        return f"<StrategyTypeParameters(id={self.id}, strategy_id={self.strategy_id})>"

    def to_dict(self):
        """Convert strategy type parameters to dictionary."""
        result = {c.name: getattr(self, c.name) for c in self.__table__.columns}

        # Convert JSON strings to dictionaries
        if result["parameters"]:
            try:
                result["parameters"] = json.loads(result["parameters"])
            except json.JSONDecodeError:
                result["parameters"] = {}

        return result


class StrategyMetrics(Base):
    """Database model for strategy metrics."""

    __tablename__ = "strategy_metrics"

    id = Column(Integer, primary_key=True)
    strategy_id = Column(Integer, ForeignKey("strategies.id"), nullable=False)
    period_start = Column(DateTime, nullable=False)
    period_end = Column(DateTime, nullable=False)

    win_count = Column(Integer, nullable=False, default=0)
    loss_count = Column(Integer, nullable=False, default=0)
    win_rate = Column(Numeric(5, 2), nullable=False, default=0)

    gross_profit = Column(Numeric(18, 2), nullable=False, default=0)
    gross_loss = Column(Numeric(18, 2), nullable=False, default=0)
    net_profit = Column(Numeric(18, 2), nullable=False, default=0)
    profit_factor = Column(Numeric(8, 4), nullable=False, default=0)

    max_drawdown = Column(Numeric(18, 2), nullable=False, default=0)
    max_drawdown_percentage = Column(Numeric(5, 2), nullable=False, default=0)
    avg_profit_per_trade = Column(Numeric(18, 2), nullable=False, default=0)
    avg_loss_per_trade = Column(Numeric(18, 2), nullable=False, default=0)
    risk_reward_ratio = Column(Numeric(8, 4), nullable=False, default=0)

    total_volume = Column(Numeric(18, 8), nullable=False, default=0)
    avg_volume_per_trade = Column(Numeric(18, 8), nullable=False, default=0)

    avg_hold_time_seconds = Column(Integer, nullable=False, default=0)

    market_volatility = Column(Numeric(8, 4), nullable=True)
    correlation_to_market = Column(Numeric(5, 2), nullable=True)

    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    strategy = relationship("Strategy", back_populates="metrics")
    trading_pair_metrics = relationship(
        "StrategyTradingPairMetrics", back_populates="strategy_metrics", cascade="all, delete-orphan"
    )
    exchange_metrics = relationship(
        "StrategyExchangeMetrics", back_populates="strategy_metrics", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"<StrategyMetrics(id={self.id}, strategy_id={self.strategy_id})>"


class StrategyTradingPairMetrics(Base):
    """Database model for strategy trading pair metrics."""

    __tablename__ = "strategy_trading_pair_metrics"

    id = Column(Integer, primary_key=True)
    strategy_metrics_id = Column(Integer, ForeignKey("strategy_metrics.id"), nullable=False)
    trading_pair_id = Column(Integer, ForeignKey("trading_pairs.id"), nullable=False)

    trade_count = Column(Integer, nullable=False, default=0)
    net_profit = Column(Numeric(18, 2), nullable=False, default=0)
    win_rate = Column(Numeric(5, 2), nullable=False, default=0)

    created_at = Column(DateTime, default=func.now())

    # Relationships
    strategy_metrics = relationship("StrategyMetrics", back_populates="trading_pair_metrics")
    trading_pair = relationship("TradingPair", back_populates="strategy_pair_metrics")

    def __repr__(self):
        return f"<StrategyTradingPairMetrics(id={self.id}, strategy_metrics_id={self.strategy_metrics_id})>"


class StrategyExchangeMetrics(Base):
    """Database model for strategy exchange metrics."""

    __tablename__ = "strategy_exchange_metrics"

    id = Column(Integer, primary_key=True)
    strategy_metrics_id = Column(Integer, ForeignKey("strategy_metrics.id"), nullable=False)
    exchange_id = Column(Integer, ForeignKey("exchanges.id"), nullable=False)

    trade_count = Column(Integer, nullable=False, default=0)
    net_profit = Column(Numeric(18, 2), nullable=False, default=0)
    win_rate = Column(Numeric(5, 2), nullable=False, default=0)

    created_at = Column(DateTime, default=func.now())

    # Relationships
    strategy_metrics = relationship("StrategyMetrics", back_populates="exchange_metrics")
    exchange = relationship("Exchange", back_populates="strategy_exchange_metrics")

    def __repr__(self):
        return f"<StrategyExchangeMetrics(id={self.id}, strategy_metrics_id={self.strategy_metrics_id})>"
