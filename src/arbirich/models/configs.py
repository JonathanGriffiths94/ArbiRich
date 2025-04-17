"""
Configuration models for the ArbiRich application.
These models represent the application configuration and are used to
initialize the application components.
"""

from typing import Any, Dict, List, Literal, Optional, Tuple, Union

from pydantic import Field

from .base import BaseModel
from .enums import LogLevel


class BaseExecutionConfig(BaseModel):
    """Base configuration model for execution methods."""

    method: str = Field(default="parallel", description="Execution method (parallel or staggered)")
    retry_attempts: int = Field(default=2, description="Number of retry attempts")
    cleanup_failed_trades: bool = Field(default=True, description="Whether to clean up after failed trades")
    stagger_delay: int = Field(default=500, description="Delay between trade legs in ms")
    abort_on_first_failure: bool = Field(default=True, description="Whether to abort on first failure")
    leg_order: str = Field(default="buy_first", description="Order of execution legs")
    timeout: int = Field(default=3000, description="Timeout in milliseconds")
    retry_delay: int = Field(default=200, description="ms delay between retries")
    max_slippage: float = Field(default=0.0005, description="Maximum allowed slippage")


class ParallelExecutionConfig(BaseExecutionConfig):
    """Configuration model for parallel execution."""

    type: Literal["parallel"] = Field(description="Execution type")
    max_concurrent_trades: int = Field(default=3, description="Maximum number of concurrent trades")
    execution_timeout: float = Field(default=10.0, description="Timeout for parallel execution in seconds")


class StaggeredExecutionConfig(BaseExecutionConfig):
    """Configuration model for staggered execution."""

    type: Literal["staggered"] = Field(description="Execution type")
    execution_order: str = Field(default="buy_first", description="Order of execution (buy_first or sell_first)")
    delay_between_orders: float = Field(default=0.5, description="Delay between orders in seconds")


class RiskAwareExecutionConfig(BaseExecutionConfig):
    """Configuration model for risk-aware execution."""

    type: Literal["risk_aware"] = Field(description="Execution type")
    base_execution_type: str = Field(default="parallel", description="Base execution method to wrap")
    max_slippage_tolerance: float = Field(default=0.001, description="Maximum allowed slippage")
    risk_check_timeout: float = Field(default=1.0, description="Timeout for risk checks in seconds")
    enable_pre_trade_validation: bool = Field(default=True, description="Enable pre-trade risk validation")
    enable_post_trade_analysis: bool = Field(default=True, description="Enable post-trade risk analysis")


class RiskConfig(BaseModel):
    """Risk management configuration."""

    max_position_size: float = 1.0
    max_daily_loss: float = 5.0  # Percentage of capital
    max_drawdown: float = 10.0  # Percentage of capital
    max_consecutive_losses: int = 3
    circuit_breaker_cooldown: int = 3600  # 1 hour in seconds
    scale_by_spread: bool = True
    base_spread_threshold: float = 0.001  # 0.1%
    max_spread_multiple: float = 5.0  # Scale up to 5x for very good spreads
    exchange_risk_factors: Dict[str, float] = Field(default_factory=dict)


class ExchangeConfig(BaseModel):
    """Exchange configuration."""

    name: str
    api_rate_limit: int = 100
    trade_fees: float = 0.001
    rest_url: Optional[str] = None
    ws_url: Optional[str] = None
    delimiter: str = ""
    withdrawal_fee: Dict[str, float] = Field(default_factory=dict)
    api_response_time: int = 100
    mapping: Dict[str, str] = Field(default_factory=dict)

    # API configuration
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    api_endpoints: Dict[str, str] = Field(
        default_factory=lambda: {
            "trading_fees": "/api/v1/fees",
            "withdrawal_fees": "/api/v1/withdraw/fees",
            "trading_rules": "/api/v1/rules",
            "symbols": "/api/v1/symbols",
        }
    )
    api_timeouts: Dict[str, int] = Field(
        default_factory=lambda: {
            "default": 5000,
            "fees": 10000,
            "trades": 3000,
            "orders": 3000,
        }
    )
    rate_limits: Dict[str, Dict] = Field(
        default_factory=lambda: {
            "public": {"requests_per_minute": 100},
            "private": {"requests_per_minute": 20},
            "orders": {"requests_per_minute": 10},
        }
    )

    # Feature flags
    enabled: bool = True
    paper_trading: bool = True
    fetch_fees_on_startup: bool = False  # Whether to fetch real fees on startup
    dynamic_fee_updates: bool = False  # Whether to update fees periodically
    fee_update_interval: int = 3600  # Update interval in seconds

    # Fallback values if API calls fail
    default_trade_fee: float = 0.001
    default_withdrawal_fees: Dict[str, float] = Field(default_factory=lambda: {"BTC": 0.0005, "ETH": 0.01, "USDT": 1.0})

    class Config:
        extra = "allow"  # Allow additional fields for exchange-specific config


class TradingPairConfig(BaseModel):
    """Trading pair configuration."""

    base_currency: str
    quote_currency: str
    symbol: Optional[str] = None

    # Additional fields for APIs
    min_qty: float = 0.0
    max_qty: float = 0.0
    price_precision: int = 8
    qty_precision: int = 8
    min_notional: float = 0.0
    enabled: bool = True

    strategy_specific_settings: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict, description="Strategy-specific settings for this trading pair"
    )
    supported_strategy_types: List[str] = Field(
        default_factory=list, description="List of strategy types supported for this pair"
    )
    exchange_settings: Dict[str, Dict[str, Any]] = Field(
        default_factory=dict, description="Exchange-specific settings for this pair"
    )
    execution_constraints: Dict[str, Any] = Field(
        default_factory=dict, description="Execution constraints for this pair"
    )

    def __init__(self, **data):
        super().__init__(**data)
        # Always set symbol if it's not provided
        if not self.symbol:
            self.symbol = f"{self.base_currency}-{self.quote_currency}"


class BaseComponentConfig(BaseModel):
    """Base model for component configurations."""

    class Config:
        extra = "allow"  # Allow extra fields


class DetectionComponentConfig(BaseComponentConfig):
    """Configuration model for the detection component."""

    scan_interval: float = Field(default=0.5, gt=0, description="Interval between opportunity scans in seconds")
    opportunity_threshold: float = Field(default=0.001, ge=0, description="Minimum spread threshold for opportunities")
    strategies: Optional[Dict] = Field(default={}, description="Strategy configurations")


class ExecutionMetricsConfig(BaseModel):
    """Configuration for execution metrics tracking."""

    track_results: bool = Field(default=True, description="Track execution results")
    track_latency: bool = Field(default=True, description="Track execution latency")
    store_execution_details: bool = Field(default=True, description="Store detailed execution information")
    result_retention_days: int = Field(default=30, description="Days to retain execution results")
    track_dependencies: bool = Field(default=True, description="Track execution dependencies")


class ExecutionMethodConfig(BaseModel):
    """Configuration for execution strategies."""

    name: str = Field(..., description="Name of the execution strategy")
    type: str = Field(..., description="Type of execution strategy")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Strategy parameters")
    enabled: bool = Field(default=True, description="Whether the strategy is enabled")
    priority: int = Field(default=0, description="Execution priority (lower executes first)")
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    timeout: float = Field(default=5.0, description="Strategy timeout in seconds")


class TradeExecutionConfig(BaseModel):
    """Configuration for trade execution and result handling."""

    tables: Dict[str, str] = Field(
        default_factory=lambda: {
            "executions": "trade_executions",
            "results": "trade_execution_results",
            "opportunities": "trade_opportunities",
            "mapping": "trade_execution_result_mapping",
        },
        description="Table names for execution data",
    )
    relationship_config: Dict[str, Any] = Field(
        default_factory=lambda: {
            "mapping_table": "trade_execution_result_mapping",
            "execution_id_field": "execution_id",
            "result_id_field": "result_id",
            "side_field": "side",
            "created_at_field": "created_at",
        },
        description="Configuration for execution-result relationships",
    )
    execution_tracking: Dict[str, Any] = Field(
        default_factory=lambda: {
            "track_opportunity_id": True,
            "track_individual_sides": True,
            "store_side_information": True,
            "track_timestamps": True,
        },
        description="Execution tracking settings",
    )
    result_retention_policy: Dict[str, Any] = Field(
        default_factory=lambda: {
            "retention_days": 30,
            "compress_after_days": 7,
            "delete_after_days": 90,
            "cleanup_orphaned": True,
            "cascade_deletions": False,
        },
        description="Data retention policy",
    )
    metrics_tracking: Dict[str, bool] = Field(
        default_factory=lambda: {
            "track_fees": True,
            "track_slippage": True,
            "track_execution_time": True,
            "track_order_lifecycle": True,
        },
        description="Metrics tracking configuration",
    )


class ExecutionComponentConfig(BaseComponentConfig):
    """Configuration model for the execution component."""

    listen_interval: float = Field(default=0.1, gt=0, description="Interval for checking new opportunities")
    max_concurrent_executions: int = Field(default=3, gt=0, description="Maximum number of concurrent trade executions")
    execution_timeout: float = Field(default=10.0, gt=0, description="Timeout for trade execution in seconds")
    dry_run: bool = Field(default=False, description="If true, simulates trades without actual execution")
    method: str = Field(default="parallel", description="Execution method (parallel or staggered)")
    retry_attempts: int = Field(default=2, description="Number of retry attempts")
    cleanup_failed_trades: bool = Field(default=True, description="Whether to clean up after failed trades")
    stagger_delay: int = Field(default=500, description="Delay between trade legs in ms")
    abort_on_first_failure: bool = Field(default=True, description="Whether to abort on first failure")
    leg_order: str = Field(default="buy_first", description="Order of execution legs")
    metrics: ExecutionMetricsConfig = Field(
        default_factory=ExecutionMetricsConfig, description="Execution metrics configuration"
    )
    strategies: Dict[str, ExecutionMethodConfig] = Field(
        default_factory=dict, description="Execution strategy configurations"
    )
    execution_handling: TradeExecutionConfig = Field(
        default_factory=TradeExecutionConfig, description="Configuration for execution and result handling"
    )


class MetricsConfig(BaseModel):
    """Configuration model for metrics collection and tracking."""

    enabled: bool = Field(default=True, description="Whether metrics collection is enabled")
    collection_interval: int = Field(default=60, description="Interval for metrics collection in seconds")
    retention_period: int = Field(default=30, description="Days to retain metrics data")
    aggregation_periods: List[str] = Field(
        default=["1h", "4h", "1d", "7d"], description="Time periods for metrics aggregation"
    )
    track_execution_times: bool = Field(default=True, description="Track strategy execution times")
    track_fees: bool = Field(default=True, description="Track trading fees")
    track_trade_counts: bool = Field(default=True, description="Track trade success/failure counts")


class ReportingComponentConfig(BaseComponentConfig):
    """Configuration model for the reporting component."""

    persistence_interval: float = Field(default=60, gt=0, description="Interval for persisting data in seconds")
    health_check_interval: float = Field(default=300, gt=0, description="Interval for health checks in seconds")
    report_generation_interval: float = Field(
        default=3600, gt=0, description="Interval for generating reports in seconds"
    )
    reports: Optional[Dict] = Field(default={}, description="Report configurations")
    metrics: MetricsConfig = Field(default_factory=MetricsConfig, description="Metrics configuration")


class IngestionComponentConfig(BaseComponentConfig):
    """Configuration model for the ingestion component."""

    update_interval: float = Field(default=0.5, gt=0, description="Interval for data updates in seconds")
    buffer_size: int = Field(default=100, gt=0, description="Size of the data buffer")


class MarketConditionConfig(BaseModel):
    """Configuration for market condition tracking."""

    condition_check_interval: int = Field(default=60, description="Interval for checking market conditions in seconds")
    volatility_window: int = Field(default=3600, description="Window for volatility calculations in seconds")
    min_volume_threshold: float = Field(
        default=1000.0, description="Minimum volume threshold for valid market conditions"
    )
    trend_detection_periods: List[int] = Field(
        default=[300, 900, 3600], description="Periods for trend detection in seconds"
    )
    performance_tracking_enabled: bool = Field(
        default=True, description="Enable performance tracking by market condition"
    )


class StrategyMetricsConfig(BaseModel):
    """Configuration for strategy-specific metrics."""

    track_market_conditions: bool = Field(
        default=True, description="Track performance under different market conditions"
    )
    track_pair_metrics: bool = Field(default=True, description="Track metrics per trading pair")
    track_exchange_metrics: bool = Field(default=True, description="Track metrics per exchange")
    performance_window_sizes: List[int] = Field(
        default=[3600, 86400, 604800], description="Window sizes for performance tracking in seconds"
    )
    store_detailed_execution: bool = Field(default=True, description="Store detailed execution information")


class BaseStrategyConfig(BaseModel):
    """Base model for strategy configurations."""

    name: str = Field(..., description="Name of the strategy")
    type: str = Field(..., description="Strategy type")
    threshold: float = Field(..., ge=0, description="Spread threshold for opportunity detection")
    starting_capital: float = Field(default=10000.0, description="Starting capital for the strategy")
    min_spread: float = Field(default=0.0001, description="Minimum spread for opportunities")
    exchanges: List[str] = Field(default_factory=list, description="List of exchanges to monitor")
    pairs: List[Union[str, Tuple[str, str]]] = Field(
        default_factory=list, description="List of trading pairs to monitor"
    )
    risk_management: RiskConfig = Field(default_factory=RiskConfig, description="Risk management configuration")
    execution: BaseExecutionConfig = Field(default_factory=BaseExecutionConfig, description="Execution configuration")
    additional_info: Dict[str, Any] = Field(default_factory=dict, description="Additional strategy information")
    enabled: bool = Field(default=True, description="Whether the strategy is enabled")
    market_conditions: MarketConditionConfig = Field(
        default_factory=MarketConditionConfig, description="Market condition monitoring configuration"
    )
    metrics: StrategyMetricsConfig = Field(
        default_factory=StrategyMetricsConfig, description="Strategy metrics configuration"
    )

    class Config:
        extra = "allow"


class BasicStrategyConfig(BaseStrategyConfig):
    """Configuration model for the basic arbitrage strategy."""

    type: Literal["basic"] = Field(description="Strategy type")


class MidPriceStrategyConfig(BaseStrategyConfig):
    """Configuration model for the mid-price arbitrage strategy."""

    type: Literal["mid_price"] = Field(description="Strategy type")
    min_depth: int = Field(default=3, gt=0, description="Minimum order book depth for calculation")


class VWAPStrategyConfig(BaseStrategyConfig):
    """Configuration model for the VWAP arbitrage strategy."""

    type: Literal["vwap"] = Field(description="Strategy type")
    min_depth: int = Field(default=3, gt=0, description="Minimum order book depth for calculation")
    target_volume: float = Field(default=100.0, description="Target volume in USDT")
    min_depth_percentage: float = Field(default=0.7, description="Minimum percentage of target volume available")


class LiquidityAdjustedStrategyConfig(BaseStrategyConfig):
    """Configuration model for the liquidity-adjusted arbitrage strategy."""

    type: Literal["liquidity_adjusted"] = Field(description="Strategy type")
    min_depth: int = Field(default=3, gt=0, description="Minimum order book depth for calculation")
    target_volume: float = Field(default=100.0, description="Target volume in USDT")
    min_depth_percentage: float = Field(default=0.7, description="Minimum percentage of target volume available")
    slippage_factor: float = Field(default=0.5, description="Weight for slippage consideration")
    liquidity_multiplier: float = Field(default=1.5, description="Reward for higher liquidity")
    dynamic_volume_adjust: bool = Field(default=True, description="Dynamically adjust position sizes")


class StrategyTypeConfig(BaseModel):
    """Configuration for strategy types."""

    name: str = Field(..., description="Name of the strategy type")
    description: str = Field(default="", description="Strategy type description")
    parameters: Dict[str, Any] = Field(default_factory=dict, description="Default parameters for this strategy type")
    trading_pair_requirements: Dict[str, Any] = Field(
        default_factory=dict, description="Requirements for trading pairs"
    )
    execution_requirements: Dict[str, Any] = Field(default_factory=dict, description="Execution requirements")


class StrategyExchangePairConfig(BaseModel):
    """Configuration for strategy-exchange-pair mappings."""

    strategy_id: str = Field(..., description="Strategy identifier")
    exchange_id: str = Field(..., description="Exchange identifier")
    pair_id: str = Field(..., description="Trading pair identifier")
    enabled: bool = Field(default=True, description="Whether this mapping is enabled")
    priority: int = Field(default=0, description="Priority for this mapping")
    custom_parameters: Dict[str, Any] = Field(default_factory=dict, description="Custom parameters for this mapping")
    execution_overrides: Dict[str, Any] = Field(default_factory=dict, description="Execution parameter overrides")


class AppConfig(BaseModel):
    """Main application configuration."""

    strategies: Dict[str, BaseStrategyConfig] = Field(default_factory=dict)
    exchanges: Dict[str, ExchangeConfig] = Field(default_factory=dict)
    trading_pairs: Dict[str, TradingPairConfig] = Field(default_factory=dict)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    execution_methods: Dict[str, Union[ParallelExecutionConfig, StaggeredExecutionConfig, RiskAwareExecutionConfig]] = (
        Field(default_factory=dict)
    )
    execution_methods: Dict[str, ExecutionMethodConfig] = Field(
        default_factory=dict, description="Global execution strategy configurations"
    )
    strategy_types: Dict[str, StrategyTypeConfig] = Field(
        default_factory=dict, description="Strategy type configurations"
    )
    strategy_exchange_pairs: Dict[str, StrategyExchangePairConfig] = Field(
        default_factory=dict, description="Strategy-exchange-pair mapping configurations"
    )
    database_uri: str = "sqlite:///arbirich.db"
    log_level: LogLevel = LogLevel.INFO
    websocket_port: int = 8000
    api_port: int = 8001
    enable_telemetry: bool = False
    metrics: MetricsConfig = Field(default_factory=MetricsConfig, description="Global metrics configuration")
    execution_metrics: ExecutionMetricsConfig = Field(
        default_factory=ExecutionMetricsConfig, description="Global execution metrics configuration"
    )
    market_conditions: MarketConditionConfig = Field(
        default_factory=MarketConditionConfig, description="Global market condition configuration"
    )
    trade_execution: TradeExecutionConfig = Field(
        default_factory=TradeExecutionConfig, description="Global trade execution configuration"
    )
