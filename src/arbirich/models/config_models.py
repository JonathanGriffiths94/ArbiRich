from typing import Any, Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, Field


class ExecutionConfig(BaseModel):
    """Execution method configuration matching the EXECUTION_METHODS in config.py"""

    method: str = "parallel"  # Could be "parallel" or "staggered"
    timeout: int = 3000  # Timeout in milliseconds
    retry_attempts: int = 2
    retry_delay: int = 200  # ms delay between retries
    max_slippage: float = 0.0005  # Maximum allowed slippage
    cleanup_failed_trades: bool = True  # Whether to clean up after failed trades
    stagger_delay: int = 500  # ms delay between trade legs (for staggered execution)
    abort_on_first_failure: bool = True  # Whether to abort on first failure (for staggered execution)
    leg_order: str = "buy_first"  # "buy_first" or "sell_first" (for staggered execution)

    def dict(self, *args, **kwargs) -> Dict[str, Any]:
        """Convert to dictionary with proper field casing"""
        return super().dict(*args, **kwargs)


class RiskConfig(BaseModel):
    """Risk management configuration matching the risk_management section in ALL_STRATEGIES"""

    max_position_size: float = 1.0
    max_daily_loss: float = 5.0  # Percentage of capital
    max_drawdown: float = 10.0  # Percentage of capital
    max_consecutive_losses: int = 3
    circuit_breaker_cooldown: int = 3600  # 1 hour in seconds
    scale_by_spread: bool = True
    base_spread_threshold: float = 0.001  # 0.1%
    max_spread_multiple: float = 5.0  # Scale up to 5x for very good spreads
    exchange_risk_factors: Dict[str, float] = Field(default_factory=dict)

    def dict(self, *args, **kwargs) -> Dict[str, Any]:
        """Convert to dictionary with proper field casing"""
        return super().dict(*args, **kwargs)


class ExchangeConfig(BaseModel):
    """Exchange configuration matching ALL_EXCHANGES entries in config.py"""

    name: str
    api_rate_limit: int = 100
    trade_fees: float = 0.001
    rest_url: Optional[str] = None
    ws_url: Optional[str] = None
    delimiter: str = ""
    withdrawal_fee: Dict[str, float] = Field(default_factory=dict)
    api_response_time: int = 100
    mapping: Dict[str, str] = Field(default_factory=dict)
    additional_info: Dict[str, Any] = Field(default_factory=dict)

    # Fields not in config but needed for API usage
    api_key: Optional[str] = None
    api_secret: Optional[str] = None
    enabled: bool = True
    paper_trading: bool = True


class TradingPairConfig(BaseModel):
    """Trading pair configuration matching ALL_PAIRS entries in config.py"""

    base_currency: str
    quote_currency: str
    symbol: Optional[str] = None

    # Additional fields used in APIs but not in config.py
    min_qty: float = 0.0
    max_qty: float = 0.0
    price_precision: int = 8
    qty_precision: int = 8
    min_notional: float = 0.0
    enabled: bool = True

    class Config:
        validate_assignment = True

    def __init__(self, **data):
        super().__init__(**data)
        # Always set symbol if it's not provided
        if not self.symbol:
            self.symbol = f"{self.base_currency}-{self.quote_currency}"


class StrategyConfig(BaseModel):
    """Strategy configuration matching ALL_STRATEGIES entries in config.py"""

    type: str
    name: str
    starting_capital: float = 10000.0
    min_spread: float = 0.0001
    threshold: float = 0.0001
    exchanges: List[str] = Field(default_factory=list)
    pairs: List[Union[str, Tuple[str, str]]] = Field(default_factory=list)
    risk_management: RiskConfig = Field(default_factory=RiskConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    additional_info: Dict[str, Any] = Field(default_factory=dict)
    enabled: bool = True

    def dict(self, *args, **kwargs) -> Dict[str, Any]:
        """Convert to dictionary with proper field casing"""
        result = super().dict(*args, **kwargs)

        # Process pairs to ensure they're in the right format
        if "pairs" in result:
            processed_pairs = []
            for pair in result["pairs"]:
                if isinstance(pair, tuple):
                    processed_pairs.append(pair)
                elif isinstance(pair, str) and "-" in pair:
                    base, quote = pair.split("-")
                    processed_pairs.append((base, quote))
                # Keep other formats as-is
                else:
                    processed_pairs.append(pair)
            result["pairs"] = processed_pairs

        return result


class AppConfig(BaseModel):
    """Main application configuration"""

    strategies: Dict[str, StrategyConfig] = Field(default_factory=dict)
    exchanges: Dict[str, ExchangeConfig] = Field(default_factory=dict)
    trading_pairs: Dict[str, TradingPairConfig] = Field(default_factory=dict)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    execution_methods: Dict[str, ExecutionConfig] = Field(default_factory=dict)
    database_uri: str = "sqlite:///arbirich.db"
    log_level: str = "INFO"
    websocket_port: int = 8000
    api_port: int = 8001
    enable_telemetry: bool = False
