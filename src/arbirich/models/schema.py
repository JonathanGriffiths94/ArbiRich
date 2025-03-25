import uuid

from sqlalchemy import (
    JSON,
    TIMESTAMP,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    MetaData,
    Numeric,
    String,
    Table,
    UniqueConstraint,
    func,
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
    Column("is_active", Boolean, nullable=False, server_default=text("false")),
    Column("created_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP")),
)

pairs = Table(
    "pairs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("base_currency", String, nullable=False),
    Column("quote_currency", String, nullable=False),
    Column("symbol", String, nullable=False, unique=True),
    Column("is_active", Boolean, nullable=False, server_default=text("false")),
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
    Column("is_active", Boolean, nullable=False, server_default=text("false")),  # Updated default to false
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

# New tables for strategy metrics
strategy_metrics = Table(
    "strategy_metrics",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("strategy_id", Integer, ForeignKey("strategies.id"), nullable=False),
    Column("period_start", DateTime, nullable=False),
    Column("period_end", DateTime, nullable=False),
    # Performance metrics
    Column("win_count", Integer, nullable=False, server_default=text("0")),
    Column("loss_count", Integer, nullable=False, server_default=text("0")),
    Column("win_rate", Numeric(5, 2), nullable=False, server_default=text("0")),
    # Financial metrics
    Column("gross_profit", Numeric(18, 2), nullable=False, server_default=text("0")),
    Column("gross_loss", Numeric(18, 2), nullable=False, server_default=text("0")),
    Column("net_profit", Numeric(18, 2), nullable=False, server_default=text("0")),
    Column("profit_factor", Numeric(8, 4), nullable=False, server_default=text("0")),
    # Risk metrics
    Column("max_drawdown", Numeric(18, 2), nullable=False, server_default=text("0")),
    Column("max_drawdown_percentage", Numeric(5, 2), nullable=False, server_default=text("0")),
    Column("avg_profit_per_trade", Numeric(18, 2), nullable=False, server_default=text("0")),
    Column("avg_loss_per_trade", Numeric(18, 2), nullable=False, server_default=text("0")),
    Column("risk_reward_ratio", Numeric(8, 4), nullable=False, server_default=text("0")),
    # Volume metrics
    Column("total_volume", Numeric(18, 8), nullable=False, server_default=text("0")),
    Column("avg_volume_per_trade", Numeric(18, 8), nullable=False, server_default=text("0")),
    # Time metrics
    Column("avg_hold_time_seconds", Integer, nullable=False, server_default=text("0")),
    # Market condition metrics
    Column("market_volatility", Numeric(8, 4), nullable=True),
    Column("correlation_to_market", Numeric(5, 2), nullable=True),
    Column("created_at", DateTime, server_default=func.now()),
    Column("updated_at", DateTime, server_default=func.now(), onupdate=func.now()),
)

strategy_trading_pair_metrics = Table(
    "strategy_trading_pair_metrics",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("strategy_metrics_id", Integer, ForeignKey("strategy_metrics.id"), nullable=False),
    Column("trading_pair_id", Integer, ForeignKey("pairs.id"), nullable=False),  # Updated to match your schema
    Column("trade_count", Integer, nullable=False, server_default=text("0")),
    Column("net_profit", Numeric(18, 2), nullable=False, server_default=text("0")),
    Column("win_rate", Numeric(5, 2), nullable=False, server_default=text("0")),
    Column("created_at", DateTime, server_default=func.now()),
)

strategy_exchange_metrics = Table(
    "strategy_exchange_metrics",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("strategy_metrics_id", Integer, ForeignKey("strategy_metrics.id"), nullable=False),
    Column("exchange_id", Integer, ForeignKey("exchanges.id"), nullable=False),
    Column("trade_count", Integer, nullable=False, server_default=text("0")),
    Column("net_profit", Numeric(18, 2), nullable=False, server_default=text("0")),
    Column("win_rate", Numeric(5, 2), nullable=False, server_default=text("0")),
    Column("created_at", DateTime, server_default=func.now()),
)
