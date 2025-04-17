"""
SQLAlchemy models for execution in the ArbiRich application.
"""

import json
import uuid

from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, Numeric, String, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from src.arbirich.models.db.base import Base
from src.arbirich.models.enums import OrderType, TradeStatus


class ExecutionStrategy(Base):
    """Database model for execution strategies."""

    __tablename__ = "execution_strategies"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    description = Column(String, nullable=True)
    timeout = Column(Integer, nullable=False)
    retry_attempts = Column(Integer, nullable=False, default=2)
    parameters = Column(String, nullable=True)  # JSON stored as string
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    strategy_mappings = relationship("StrategyExecutionMapping", back_populates="execution_strategy")

    def __repr__(self):
        return f"<ExecutionStrategy(id={self.id}, name={self.name})>"

    def to_dict(self):
        """Convert execution strategy to dictionary."""
        result = {c.name: getattr(self, c.name) for c in self.__table__.columns}

        # Convert JSON strings to dictionaries
        if result["parameters"]:
            try:
                result["parameters"] = json.loads(result["parameters"])
            except json.JSONDecodeError:
                result["parameters"] = {}

        return result


class StrategyExecutionMapping(Base):
    """Database model for strategy-execution mappings."""

    __tablename__ = "strategy_execution_mapping"

    id = Column(Integer, primary_key=True)
    strategy_id = Column(Integer, ForeignKey("strategies.id"), nullable=False)
    execution_strategy_id = Column(Integer, ForeignKey("execution_strategies.id"), nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    priority = Column(Integer, nullable=False, default=100)
    created_at = Column(DateTime, default=func.now())

    # Relationships
    strategy = relationship("Strategy", back_populates="execution_mappings")
    execution_strategy = relationship("ExecutionStrategy", back_populates="strategy_mappings")

    def __repr__(self):
        return f"<StrategyExecutionMapping(id={self.id}, strategy_id={self.strategy_id})>"


class StrategyExchangePairMapping(Base):
    """Database model for strategy-exchange-pair mappings."""

    __tablename__ = "strategy_exchange_pair_mappings"

    id = Column(Integer, primary_key=True)
    strategy_id = Column(Integer, ForeignKey("strategies.id"), nullable=False)
    exchange_id = Column(Integer, ForeignKey("exchanges.id"), nullable=False)
    trading_pair_id = Column(Integer, ForeignKey("trading_pairs.id"), nullable=False)
    is_active = Column(Boolean, nullable=False, default=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    strategy = relationship("Strategy", back_populates="exchange_pair_mappings")
    exchange = relationship("Exchange", back_populates="exchange_pair_mappings")
    trading_pair = relationship("TradingPair", back_populates="exchange_pair_mappings")

    def __repr__(self):
        return f"<StrategyExchangePairMapping(id={self.id}, strategy_id={self.strategy_id})>"


class TradeOpportunity(Base):
    """Database model for trade opportunities."""

    __tablename__ = "trade_opportunities"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    strategy_id = Column(Integer, ForeignKey("strategies.id"), nullable=False)
    trading_pair_id = Column(Integer, ForeignKey("trading_pairs.id"), nullable=False)
    buy_exchange_id = Column(Integer, ForeignKey("exchanges.id"), nullable=False)
    sell_exchange_id = Column(Integer, ForeignKey("exchanges.id"), nullable=False)
    buy_price = Column(Numeric(18, 8), nullable=False)
    sell_price = Column(Numeric(18, 8), nullable=False)
    spread = Column(Numeric(18, 8), nullable=False)
    volume = Column(Numeric(18, 8), nullable=False)
    opportunity_timestamp = Column(DateTime, default=func.now())

    # Relationships
    strategy = relationship("Strategy", foreign_keys=[strategy_id])
    trading_pair = relationship("TradingPair", foreign_keys=[trading_pair_id])
    buy_exchange = relationship("Exchange", foreign_keys=[buy_exchange_id])
    sell_exchange = relationship("Exchange", foreign_keys=[sell_exchange_id])
    executions = relationship("TradeExecution", back_populates="opportunity")

    def __repr__(self):
        return f"<TradeOpportunity(id={self.id}, strategy_id={self.strategy_id})>"

    def to_dict(self):
        """Convert trade opportunity to dictionary."""
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class TradeExecution(Base):
    """Database model for trade executions."""

    __tablename__ = "trade_executions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    opportunity_id = Column(UUID(as_uuid=True), ForeignKey("trade_opportunities.id"), nullable=True)
    strategy_id = Column(Integer, ForeignKey("strategies.id"), nullable=False)
    trading_pair_id = Column(Integer, ForeignKey("trading_pairs.id"), nullable=False)
    buy_exchange_id = Column(Integer, ForeignKey("exchanges.id"), nullable=False)
    sell_exchange_id = Column(Integer, ForeignKey("exchanges.id"), nullable=False)
    executed_buy_price = Column(Numeric(18, 8), nullable=False)
    executed_sell_price = Column(Numeric(18, 8), nullable=False)
    spread = Column(Numeric(18, 8), nullable=False)
    volume = Column(Numeric(18, 8), nullable=False)
    profit = Column(Numeric(18, 8), nullable=False, default=0)
    execution_time_ms = Column(Integer, nullable=False, default=0)
    success = Column(Boolean, nullable=False, default=False)
    partial = Column(Boolean, nullable=False, default=False)
    error = Column(String, nullable=True)
    details = Column(String, nullable=True)  # JSON stored as string
    execution_timestamp = Column(DateTime, default=func.now())

    # Relationships
    opportunity = relationship("TradeOpportunity", back_populates="executions")
    strategy = relationship("Strategy", foreign_keys=[strategy_id])
    trading_pair = relationship("TradingPair", foreign_keys=[trading_pair_id])
    buy_exchange = relationship("Exchange", foreign_keys=[buy_exchange_id])
    sell_exchange = relationship("Exchange", foreign_keys=[sell_exchange_id])
    orders = relationship("Order", back_populates="trade_execution")

    def __repr__(self):
        return f"<TradeExecution(id={self.id}, strategy_id={self.strategy_id})>"

    def to_dict(self):
        """Convert trade execution to dictionary."""
        result = {c.name: getattr(self, c.name) for c in self.__table__.columns}

        # Convert JSON strings to dictionaries
        if result["details"]:
            try:
                result["details"] = json.loads(result["details"])
            except json.JSONDecodeError:
                result["details"] = {}

        return result


class Order(Base):
    """Database model for orders."""

    __tablename__ = "orders"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    trade_execution_id = Column(UUID(as_uuid=True), ForeignKey("trade_executions.id"), nullable=True)
    exchange_id = Column(Integer, ForeignKey("exchanges.id"), nullable=False)
    trading_pair_id = Column(Integer, ForeignKey("trading_pairs.id"), nullable=False)
    symbol = Column(String, nullable=False)
    side = Column(String, nullable=False)  # OrderSide enum value
    type = Column(String, nullable=False, default=OrderType.MARKET.value)  # OrderType enum value
    price = Column(Numeric(18, 8), nullable=True)
    quantity = Column(Numeric(18, 8), nullable=False)
    filled_quantity = Column(Numeric(18, 8), nullable=False, default=0)
    status = Column(String, nullable=False, default=TradeStatus.PENDING.value)  # TradeStatus enum value
    exchange_order_id = Column(String, nullable=True)
    error_message = Column(String, nullable=True)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    trade_execution = relationship("TradeExecution", back_populates="orders")
    exchange = relationship("Exchange", foreign_keys=[exchange_id])
    trading_pair = relationship("TradingPair", foreign_keys=[trading_pair_id])

    def __repr__(self):
        return f"<Order(id={self.id}, exchange_id={self.exchange_id}, side={self.side})>"

    def to_dict(self):
        """Convert order to dictionary."""
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
