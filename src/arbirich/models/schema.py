import uuid

from sqlalchemy import (
    JSON,
    TIMESTAMP,
    Column,
    ForeignKey,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import UUID

metadata = MetaData()

exchanges = Table(
    "exchanges",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String, nullable=False, unique=True),
    Column("api_rate_limit", Integer),
    Column("trade_fees", Numeric(18, 8)),
    Column("rest_url", String),
    Column("ws_url", String),
    Column("delimiter", String),
    Column("withdrawal_fee", JSON),
    Column("api_response_time", Integer),
    Column("mapping", JSON),
    Column("additional_info", JSON),
    Column("created_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP")),
)

pairs = Table(
    "pairs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("base_currency", String, nullable=False),
    Column("quote_currency", String, nullable=False),
    Column("symbol", String, nullable=False, unique=True),
    UniqueConstraint("base_currency", "quote_currency", name="uix_pair_base_quote"),
)

strategies = Table(
    "strategies",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String, nullable=False, unique=True),
    Column("starting_capital", Numeric(18, 2), nullable=False),
    Column("min_spread", Numeric(18, 8), nullable=False),
    Column("additional_info", JSON),
    Column("total_profit", Numeric(18, 2), nullable=False, server_default=text("0")),
    Column("total_loss", Numeric(18, 2), nullable=False, server_default=text("0")),
    Column("net_profit", Numeric(18, 2), nullable=False, server_default=text("0")),
    Column("trade_count", Integer, nullable=False, server_default=text("0")),
    Column("start_timestamp", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP")),
    Column(
        "last_updated",
        TIMESTAMP,
        server_default=text("CURRENT_TIMESTAMP"),
        onupdate=text("CURRENT_TIMESTAMP"),
    ),
)

trade_opportunities = Table(
    "trade_opportunities",
    metadata,
    Column("id", UUID, primary_key=True, default=uuid.uuid4),
    Column("strategy", String, ForeignKey("strategies.name")),
    Column("pair", String, ForeignKey("pairs.symbol")),
    Column("buy_exchange", String, ForeignKey("exchanges.name")),
    Column("sell_exchange", String, ForeignKey("exchanges.name")),
    Column("buy_price", Numeric(18, 8), nullable=False),
    Column("sell_price", Numeric(18, 8), nullable=False),
    Column("spread", Numeric(18, 8), nullable=False),
    Column("volume", Numeric(18, 8), nullable=False),
    Column("opportunity_timestamp", TIMESTAMP, nullable=False),
)

trade_executions = Table(
    "trade_executions",
    metadata,
    Column("id", UUID, primary_key=True, default=uuid.uuid4),
    Column("strategy", String, ForeignKey("strategies.name")),
    Column("pair", String, ForeignKey("pairs.symbol")),
    Column("buy_exchange", String, ForeignKey("exchanges.name")),
    Column("sell_exchange", String, ForeignKey("exchanges.name")),
    Column("executed_buy_price", Numeric(18, 8), nullable=False),
    Column("executed_sell_price", Numeric(18, 8), nullable=False),
    Column("spread", Numeric(18, 8), nullable=False),
    Column("volume", Numeric(18, 8), nullable=False),
    Column("execution_timestamp", TIMESTAMP, nullable=False),
    Column("execution_id", String),
    Column("opportunity_id", UUID, ForeignKey("trade_opportunities.id")),
)
