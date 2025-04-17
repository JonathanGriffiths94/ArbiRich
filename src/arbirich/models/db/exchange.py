"""
SQLAlchemy models for exchanges in the ArbiRich application.
"""

import json

from sqlalchemy import Boolean, Column, DateTime, Integer, Numeric, String, func
from sqlalchemy.orm import relationship

from src.arbirich.models.db.base import Base


class Exchange(Base):
    """Database model for exchanges."""

    __tablename__ = "exchanges"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    api_rate_limit = Column(Integer, nullable=True)
    trade_fees = Column(Numeric(18, 8), nullable=True)
    rest_url = Column(String, nullable=True)
    ws_url = Column(String, nullable=True)
    delimiter = Column(String, nullable=True)
    withdrawal_fee = Column(String, nullable=True)  # JSON stored as string
    api_response_time = Column(Integer, nullable=True)
    mapping = Column(String, nullable=True)  # JSON stored as string
    additional_info = Column(String, nullable=True)  # JSON stored as string
    is_active = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime, default=func.now())

    # Relationships
    strategy_exchange_metrics = relationship("StrategyExchangeMetrics", back_populates="exchange")
    exchange_pair_mappings = relationship("StrategyExchangePairMapping", back_populates="exchange")

    def __repr__(self):
        return f"<Exchange(id={self.id}, name={self.name}, is_active={self.is_active})>"

    def to_dict(self):
        """Convert exchange to dictionary."""
        result = {c.name: getattr(self, c.name) for c in self.__table__.columns}

        # Convert JSON strings to dictionaries
        for field in ["withdrawal_fee", "mapping", "additional_info"]:
            if result[field]:
                try:
                    result[field] = json.loads(result[field])
                except json.JSONDecodeError:
                    result[field] = {}

        return result

    @classmethod
    def from_dict(cls, data):
        """Create an Exchange from a dictionary."""
        # Process JSON fields
        for field in ["withdrawal_fee", "mapping", "additional_info"]:
            if field in data and isinstance(data[field], dict):
                data[field] = json.dumps(data[field])

        return cls(**data)


class TradingPair(Base):
    """Database model for trading pairs."""

    __tablename__ = "trading_pairs"

    id = Column(Integer, primary_key=True)
    base_currency = Column(String, nullable=False)
    quote_currency = Column(String, nullable=False)
    symbol = Column(String, nullable=False, unique=True)
    is_active = Column(Boolean, nullable=False, default=False)

    # Relationships
    strategy_pair_metrics = relationship("StrategyTradingPairMetrics", back_populates="trading_pair")
    exchange_pair_mappings = relationship("StrategyExchangePairMapping", back_populates="trading_pair")

    def __repr__(self):
        return f"<TradingPair(id={self.id}, symbol={self.symbol}, is_active={self.is_active})>"

    def to_dict(self):
        """Convert trading pair to dictionary."""
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

    @classmethod
    def from_dict(cls, data):
        """Create a TradingPair from a dictionary."""
        return cls(**data)
