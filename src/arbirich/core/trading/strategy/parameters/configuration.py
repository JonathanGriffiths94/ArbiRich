from typing import Any, Dict, Optional


class ConfigurationParameters:
    """Trading configuration parameters for arbitrage strategies"""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize configuration parameters from a dictionary

        Args:
            config: Dictionary of configuration parameters
        """
        # Strategy execution parameters
        self.cycle_interval = config.get("cycle_interval", 1.0)  # Time between cycles in seconds

        # Trading thresholds
        self.min_spread = config.get("min_spread", 0.001)  # Minimum spread as decimal (0.001 = 0.1%)
        self.max_slippage = config.get("max_slippage", 0.002)  # Maximum allowed slippage (0.2%)

        # Position sizing
        self.min_volume = config.get("min_volume", 0.001)  # Minimum volume to trade
        self.max_volume = config.get("max_volume", 1.0)  # Maximum volume to trade
        self.risk_percentage = config.get("risk_percentage", 2.0)  # % of available capital per trade

        # Execution parameters
        self.max_execution_time = config.get("max_execution_time", 3000)  # Maximum time for execution in ms
        self.execution_timeout = config.get("execution_timeout", 5000)  # Timeout for execution in ms

        # Risk management
        self.max_consecutive_losses = config.get("max_consecutive_losses", 3)
        self.daily_loss_limit = config.get("daily_loss_limit", 5.0)  # % of account value

        # Extra parameters
        self.extra = config.get("extra", {})  # Any additional parameters

    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration parameters to dictionary"""
        return {
            "cycle_interval": self.cycle_interval,
            "min_spread": self.min_spread,
            "max_slippage": self.max_slippage,
            "min_volume": self.min_volume,
            "max_volume": self.max_volume,
            "risk_percentage": self.risk_percentage,
            "max_execution_time": self.max_execution_time,
            "execution_timeout": self.execution_timeout,
            "max_consecutive_losses": self.max_consecutive_losses,
            "daily_loss_limit": self.daily_loss_limit,
            "extra": self.extra,
        }

    def update_from_dict(self, update_dict: Dict[str, Any]) -> None:
        """
        Update configuration parameters from a dictionary

        Args:
            update_dict: Dictionary of parameters to update
        """
        for key, value in update_dict.items():
            if hasattr(self, key):
                setattr(self, key, value)
            elif key == "extra":
                self.extra.update(value)

    def get_param(self, name: str, default: Optional[Any] = None) -> Any:
        """
        Get a parameter by name, checking both direct attributes and extra dict

        Args:
            name: Parameter name
            default: Default value if parameter not found

        Returns:
            Parameter value or default
        """
        if hasattr(self, name):
            return getattr(self, name)
        return self.extra.get(name, default)
