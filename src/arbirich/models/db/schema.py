import uuid

from sqlalchemy import (
    JSON,
    TIMESTAMP,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
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

# Core entity tables
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
    Column("api_key", String),
    Column("api_secret", String),
    Column("paper_trading", Boolean, server_default=text("true")),
    Column("has_websocket", Boolean, server_default=text("false")),
    Column("has_rest", Boolean, server_default=text("true")),
    Column("has_private_api", Boolean, server_default=text("false")),
    Column("status", String, server_default=text("'online'")),
    Column("version", String),
    Column("description", String),
)

trading_pairs = Table(
    "trading_pairs",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("base_currency", String, nullable=False),
    Column("quote_currency", String, nullable=False),
    Column("symbol", String, nullable=False, unique=True),
    Column("is_active", Boolean, nullable=False, server_default=text("false")),
    Column("min_qty", Numeric(18, 8), server_default=text("0")),
    Column("max_qty", Numeric(18, 8), server_default=text("0")),
    Column("price_precision", Integer, server_default=text("8")),
    Column("qty_precision", Integer, server_default=text("8")),
    Column("min_notional", Numeric(18, 8), server_default=text("0")),
    Column("enabled", Boolean, server_default=text("true")),
    Column("status", String, server_default=text("'inactive'")),
    Column("status_reason", String),
    Column("status_changed_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP")),
    UniqueConstraint("base_currency", "quote_currency", name="uix_pair_base_quote"),
)

# Strategy component tables
strategy_types = Table(
    "strategy_types",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String, nullable=False, unique=True),  # 'basic', 'mid_price', 'volume_adjusted', etc.
    Column("description", String, nullable=True),
    Column("implementation_class", String, nullable=False),  # Class name for implementation
    Column("created_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP")),
)

risk_profiles = Table(
    "risk_profiles",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String, nullable=False, unique=True),  # 'Conservative', 'Moderate', 'Aggressive', etc.
    Column("description", String, nullable=True),
    Column("max_position_size_percentage", Numeric(5, 2)),  # % of total capital
    Column("max_drawdown_percentage", Numeric(5, 2)),
    Column("max_exposure_per_asset_percentage", Numeric(5, 2)),
    Column("circuit_breaker_conditions", JSON),  # Rules for auto-shutdown
    Column("created_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP")),
)

execution_methods = Table(
    "execution_methods",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String, nullable=False, unique=True),  # 'Parallel', 'Sequential', 'Staged', etc.
    Column("description", String, nullable=True),
    Column("timeout", Integer, nullable=False),  # Timeout in milliseconds
    Column("retry_attempts", Integer, nullable=False, server_default=text("2")),
    Column("parameters", JSON, nullable=True),  # Execution-specific parameters
    Column("created_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP")),
)

# Main strategy table
strategies = Table(
    "strategies",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String, nullable=False, unique=True),
    Column("description", String, nullable=True),
    Column("strategy_type_id", Integer, ForeignKey("strategy_types.id"), nullable=False),
    Column("risk_profile_id", Integer, ForeignKey("risk_profiles.id"), nullable=False),
    Column("starting_capital", Numeric(18, 2), nullable=False),
    Column("total_profit", Numeric(18, 2), nullable=False, server_default=text("0")),
    Column("total_loss", Numeric(18, 2), nullable=False, server_default=text("0")),
    Column("net_profit", Numeric(18, 2), nullable=False, server_default=text("0")),
    Column("trade_count", Integer, nullable=False, server_default=text("0")),
    Column("start_timestamp", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP")),
    Column("last_updated", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP")),
    Column("is_active", Boolean, nullable=False, server_default=text("false")),
    Column("created_by", String, nullable=True),
)

# Configuration tables
strategy_parameters = Table(
    "strategy_parameters",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("strategy_id", Integer, ForeignKey("strategies.id"), nullable=False),
    Column("min_spread", Numeric(18, 8), nullable=False),  # Minimum profitable spread
    Column("threshold", Numeric(18, 8), nullable=False),  # Threshold value for decision making
    Column("max_slippage", Numeric(18, 8)),  # Maximum slippage tolerance
    Column("min_volume", Numeric(18, 8)),  # Minimum trade size
    Column("max_execution_time_ms", Integer),  # Maximum time allowed for execution
    Column("additional_parameters", JSON, nullable=True),  # Flexible parameters storage
    Column("created_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP")),
    UniqueConstraint("strategy_id", name="uix_strategy_parameters"),
)

strategy_type_parameters = Table(
    "strategy_type_parameters",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("strategy_id", Integer, ForeignKey("strategies.id"), nullable=False),
    Column("target_volume", Numeric(18, 8), nullable=True),  # For volume-adjusted strategies
    Column("min_depth", Integer, nullable=True),  # For mid-price strategies
    Column("min_depth_percentage", Numeric(5, 2), nullable=True),  # For volume-adjusted strategies
    Column("parameters", JSON, nullable=True),  # Type-specific parameters
    Column("created_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP")),
    UniqueConstraint("strategy_id", name="uix_strategy_type_parameters"),
)

# Trading data tables
trade_opportunities = Table(
    "trade_opportunities",
    metadata,
    Column("id", UUID, primary_key=True, default=uuid.uuid4),
    Column("strategy_id", Integer, ForeignKey("strategies.id"), nullable=False),
    Column("trading_pair_id", Integer, ForeignKey("trading_pairs.id"), nullable=False),
    Column("buy_exchange_id", Integer, ForeignKey("exchanges.id"), nullable=False),
    Column("sell_exchange_id", Integer, ForeignKey("exchanges.id"), nullable=False),
    Column("buy_price", Numeric(18, 8), nullable=False),
    Column("sell_price", Numeric(18, 8), nullable=False),
    Column("spread", Numeric(18, 8), nullable=False),
    Column("volume", Numeric(18, 8), nullable=False),
    Column("opportunity_timestamp", TIMESTAMP, nullable=False),
    Column("opportunity_key", String),
)

trade_executions = Table(
    "trade_executions",
    metadata,
    Column("id", UUID, primary_key=True, default=uuid.uuid4),
    Column("strategy_id", Integer, ForeignKey("strategies.id"), nullable=False),
    Column("trading_pair_id", Integer, ForeignKey("trading_pairs.id"), nullable=False),
    Column("buy_exchange_id", Integer, ForeignKey("exchanges.id"), nullable=False),
    Column("sell_exchange_id", Integer, ForeignKey("exchanges.id"), nullable=False),
    Column("executed_buy_price", Numeric(18, 8), nullable=False),
    Column("executed_sell_price", Numeric(18, 8), nullable=False),
    Column("spread", Numeric(18, 8), nullable=False),
    Column("volume", Numeric(18, 8), nullable=False),
    Column("execution_timestamp", TIMESTAMP, nullable=False),
    Column("execution_id", String),
    Column("opportunity_id", UUID, ForeignKey("trade_opportunities.id")),
    Column("profit", Numeric(18, 8)),
    Column("status", String, server_default=text("'completed'")),
)

trade_execution_results = Table(
    "trade_execution_results",
    metadata,
    Column("id", UUID, primary_key=True, default=uuid.uuid4),
    Column("exchange_id", Integer, ForeignKey("exchanges.id"), nullable=False),
    Column("trading_pair_id", Integer, ForeignKey("trading_pairs.id"), nullable=False),
    Column("side", String, nullable=False),  # buy or sell
    Column("order_type", String, nullable=False),
    Column("requested_amount", Numeric(18, 8), nullable=False),
    Column("executed_amount", Numeric(18, 8), nullable=False),
    Column("price", Numeric(18, 8)),
    Column("average_price", Numeric(18, 8)),
    Column("status", String, nullable=False),
    Column("timestamp", TIMESTAMP, nullable=False),
    Column("fees", JSON),
    Column("trade_id", String),
    Column("order_id", String),
    Column("error", String),
    Column("raw_response", JSON),
    Column("strategy_id", Integer, ForeignKey("strategies.id")),
    Column("created_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP")),
)

orders = Table(
    "orders",
    metadata,
    Column("id", String, primary_key=True),
    Column("exchange_id", Integer, ForeignKey("exchanges.id"), nullable=False),
    Column("trading_pair_id", Integer, ForeignKey("trading_pairs.id"), nullable=False),
    Column("side", String, nullable=False),  # buy or sell
    Column("order_type", String, nullable=False),  # market, limit, etc.
    Column("price", Numeric(18, 8)),
    Column("amount", Numeric(18, 8), nullable=False),
    Column("filled", Numeric(18, 8), server_default=text("0")),
    Column("remaining", Numeric(18, 8)),
    Column("status", String, nullable=False),
    Column("timestamp", TIMESTAMP, nullable=False),
    Column("average_price", Numeric(18, 8)),
    Column("fees", JSON),
    Column("raw_response", JSON),
    Column("strategy_id", Integer, ForeignKey("strategies.id")),
    Column("created_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP")),
)

# Performance metrics tables
strategy_metrics = Table(
    "strategy_metrics",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
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
    # Tracking fields
    Column("total_trades", Integer, nullable=False, server_default=text("0")),
    Column("successful_trades", Integer, nullable=False, server_default=text("0")),
    Column("failed_trades", Integer, nullable=False, server_default=text("0")),
    Column("total_fees", Numeric(18, 8), nullable=False, server_default=text("0")),
    Column("average_execution_time", Integer, nullable=False, server_default=text("0")),
    Column("created_at", DateTime, server_default=func.now()),
    Column("updated_at", DateTime, server_default=func.now(), onupdate=func.now()),
)

strategy_trading_pair_metrics = Table(
    "strategy_trading_pair_metrics",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("strategy_metrics_id", Integer, ForeignKey("strategy_metrics.id"), nullable=False),
    Column("trading_pair_id", Integer, ForeignKey("trading_pairs.id"), nullable=False),
    Column("trade_count", Integer, nullable=False, server_default=text("0")),
    Column("net_profit", Numeric(18, 2), nullable=False, server_default=text("0")),
    Column("win_rate", Numeric(5, 2), nullable=False, server_default=text("0")),
    Column("created_at", DateTime, server_default=func.now()),
)

strategy_exchange_metrics = Table(
    "strategy_exchange_metrics",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("strategy_metrics_id", Integer, ForeignKey("strategy_metrics.id"), nullable=False),
    Column("exchange_id", Integer, ForeignKey("exchanges.id"), nullable=False),
    Column("trade_count", Integer, nullable=False, server_default=text("0")),
    Column("net_profit", Numeric(18, 2), nullable=False, server_default=text("0")),
    Column("win_rate", Numeric(5, 2), nullable=False, server_default=text("0")),
    Column("created_at", DateTime, server_default=func.now()),
)

# Market condition performance table
market_condition_performance = Table(
    "market_condition_performance",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("strategy_id", Integer, ForeignKey("strategies.id"), nullable=False),
    Column("market_condition", String, nullable=False),  # high_volatility, low_liquidity, etc.
    Column("exchange_id", Integer, ForeignKey("exchanges.id"), nullable=False),
    Column("trading_pair_id", Integer, ForeignKey("trading_pairs.id"), nullable=False),
    Column("trade_count", Integer, nullable=False, server_default=text("0")),
    Column("profit", Numeric(18, 2), nullable=False, server_default=text("0")),
    Column("win_rate", Numeric(5, 2), nullable=False, server_default=text("0")),
    Column("avg_profit_per_trade", Numeric(18, 2), nullable=False, server_default=text("0")),
    Column("created_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP")),
    UniqueConstraint(
        "strategy_id", "market_condition", "exchange_id", "trading_pair_id", name="uix_strategy_market_perf"
    ),
)

# System tables
system_health_checks = Table(
    "system_health_checks",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("timestamp", TIMESTAMP, nullable=False),
    Column("database_healthy", Boolean, nullable=False),
    Column("redis_healthy", Boolean, nullable=False),
    Column("exchanges", JSON, nullable=False),  # Status of each exchange
    Column("overall_health", Boolean, nullable=False),
)

Index("idx_health_checks_timestamp", system_health_checks.c.timestamp)

Index("idx_strategy_metrics_period", strategy_metrics.c.period_start, strategy_metrics.c.period_end)
Index("idx_strategy_metrics_strategy", strategy_metrics.c.strategy_id)

# Mapping tables
trade_execution_result_mapping = Table(
    "trade_execution_result_mapping",
    metadata,
    Column("execution_id", UUID, ForeignKey("trade_executions.id"), primary_key=True),
    Column("result_id", UUID, ForeignKey("trade_execution_results.id"), primary_key=True),
    Column("side", String, nullable=False),  # 'buy' or 'sell'
    Column("created_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP")),
)

# Mapping tables
strategy_execution_mapping = Table(
    "strategy_execution_mapping",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("strategy_id", Integer, ForeignKey("strategies.id"), nullable=False),
    Column("execution_strategy_id", Integer, ForeignKey("execution_methods.id"), nullable=False),
    Column("is_active", Boolean, nullable=False, server_default=text("true")),
    Column("priority", Integer, nullable=False, server_default=text("100")),  # Lower numbers = higher priority
    Column("created_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP")),
    UniqueConstraint("strategy_id", "execution_strategy_id", name="uix_strategy_execution"),
)

strategy_exchange_pair_mapping = Table(
    "strategy_exchange_pair_mapping",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("strategy_id", Integer, ForeignKey("strategies.id"), nullable=False),
    Column("exchange_id", Integer, ForeignKey("exchanges.id"), nullable=False),
    Column("trading_pair_id", Integer, ForeignKey("trading_pairs.id"), nullable=False),
    Column("is_active", Boolean, nullable=False, server_default=text("true")),
    Column("created_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP")),
    Column("updated_at", TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"), onupdate=text("CURRENT_TIMESTAMP")),
    UniqueConstraint("strategy_id", "exchange_id", "trading_pair_id", name="uix_strat_exchange_pair"),
)
