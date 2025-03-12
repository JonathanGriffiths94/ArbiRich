import uuid

from sqlalchemy import (
    JSON,
    TIMESTAMP,
    Column,
    ForeignKey,
    Integer,
    Numeric,
    String,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Exchange(Base):
    __tablename__ = "exchanges"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String, nullable=False, unique=True)
    api_rate_limit = Column(Integer)
    trade_fees = Column(Numeric(18, 8))
    additional_info = Column(JSON)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))


class TradingPair(Base):
    __tablename__ = "trading_pairs"
    id = Column(Integer, primary_key=True, autoincrement=True)
    base_currency = Column(String, nullable=False)
    quote_currency = Column(String, nullable=False)

    __table_args__ = (
        UniqueConstraint("base_currency", "quote_currency", name="uix_base_quote"),
    )


class TradeOpportunity(Base):
    __tablename__ = "trade_opportunities"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    trading_pair_id = Column(Integer, ForeignKey("trading_pairs.id"))
    buy_exchange_id = Column(Integer, ForeignKey("exchanges.id"))
    sell_exchange_id = Column(Integer, ForeignKey("exchanges.id"))
    buy_price = Column(Numeric(18, 8), nullable=False)
    sell_price = Column(Numeric(18, 8), nullable=False)
    spread = Column(Numeric(18, 8), nullable=False)
    volume = Column(Numeric(18, 8), nullable=False)
    opportunity_timestamp = Column(TIMESTAMP, nullable=False)


class TradeExecution(Base):
    __tablename__ = "trade_executions"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    trading_pair_id = Column(Integer, ForeignKey("trading_pairs.id"))
    buy_exchange_id = Column(Integer, ForeignKey("exchanges.id"))
    sell_exchange_id = Column(Integer, ForeignKey("exchanges.id"))
    executed_buy_price = Column(Numeric(18, 8), nullable=False)
    executed_sell_price = Column(Numeric(18, 8), nullable=False)
    spread = Column(Numeric(18, 8), nullable=False)
    volume = Column(Numeric(18, 8), nullable=False)
    execution_timestamp = Column(TIMESTAMP, nullable=False)
    execution_id = Column(String)
    opportunity_id = Column(UUID(as_uuid=True), ForeignKey("trade_opportunities.id"))
