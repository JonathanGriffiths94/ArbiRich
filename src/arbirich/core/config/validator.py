"""Configuration validation utilities using Pydantic."""

from typing import Dict, List, Optional, Tuple, Union

from pydantic import BaseModel, Field


# Base component config model
class BaseComponentConfig(BaseModel):
    """Base model for component configurations."""

    class Config:
        extra = "allow"  # Allow extra fields


# Component-specific models
class DetectionComponentConfig(BaseComponentConfig):
    """Configuration model for the detection component."""

    scan_interval: float = Field(default=0.5, gt=0, description="Interval between opportunity scans in seconds")
    opportunity_threshold: float = Field(default=0.001, ge=0, description="Minimum spread threshold for opportunities")
    strategies: Optional[Dict] = Field(default={}, description="Strategy configurations")


class ExecutionComponentConfig(BaseComponentConfig):
    """Configuration model for the execution component."""

    listen_interval: float = Field(default=0.1, gt=0, description="Interval for checking new opportunities")
    max_concurrent_executions: int = Field(default=3, gt=0, description="Maximum number of concurrent trade executions")
    execution_timeout: float = Field(default=10.0, gt=0, description="Timeout for trade execution in seconds")
    dry_run: bool = Field(default=False, description="If true, simulates trades without actual execution")


class ReportingComponentConfig(BaseComponentConfig):
    """Configuration model for the reporting component."""

    persistence_interval: float = Field(default=60, gt=0, description="Interval for persisting data in seconds")
    health_check_interval: float = Field(default=300, gt=0, description="Interval for health checks in seconds")
    report_generation_interval: float = Field(
        default=3600, gt=0, description="Interval for generating reports in seconds"
    )
    reports: Optional[Dict] = Field(default={}, description="Report configurations")


class IngestionComponentConfig(BaseComponentConfig):
    """Configuration model for the ingestion component."""

    update_interval: float = Field(default=1.0, gt=0, description="Interval for data updates in seconds")
    buffer_size: int = Field(default=100, gt=0, description="Size of the data buffer")


# Base strategy config model
class BaseStrategyConfig(BaseModel):
    """Base model for strategy configurations."""

    name: str = Field(..., description="Name of the strategy")
    threshold: float = Field(..., ge=0, description="Spread threshold for opportunity detection")

    class Config:
        extra = "allow"  # Allow extra fields


# Strategy-specific models
class BasicStrategyConfig(BaseStrategyConfig):
    """Configuration model for the basic arbitrage strategy."""

    type: str = Field(default="basic", description="Strategy type")
    pairs: List[Union[str, Tuple[str, str]]] = Field(
        default=[], description="List of trading pairs to monitor (strings or tuples)"
    )
    exchanges: List[str] = Field(default=[], description="List of exchanges to monitor")


class MidPriceStrategyConfig(BaseStrategyConfig):
    """Configuration model for the mid-price arbitrage strategy."""

    type: str = Field(default="mid_price", description="Strategy type")
    min_depth: int = Field(default=3, gt=0, description="Minimum order book depth for calculation")
    pairs: List[Union[str, Tuple[str, str]]] = Field(
        default=[], description="List of trading pairs to monitor (strings or tuples)"
    )
    exchanges: List[str] = Field(default=[], description="List of exchanges to monitor")


class ConfigValidator:
    """Utility class for validating component and strategy configurations using Pydantic models."""

    @staticmethod
    def validate_component_config(component_name: str, config: Dict) -> Dict[str, List[str]]:
        """
        Validate configuration for a specific component using Pydantic models.

        Args:
            component_name: Name of the component (e.g., 'detection', 'execution')
            config: Configuration dictionary to validate

        Returns:
            Dictionary of validation errors by field, or empty dict if valid
        """
        errors = {}

        try:
            # Select the appropriate model based on component name
            if component_name == "detection":
                DetectionComponentConfig(**config)
            elif component_name == "execution":
                ExecutionComponentConfig(**config)
            elif component_name == "reporting":
                ReportingComponentConfig(**config)
            elif component_name == "ingestion":
                IngestionComponentConfig(**config)
            else:
                # Use base model for unknown components
                BaseComponentConfig(**config)

        except Exception as e:
            # Convert pydantic validation errors to our format
            if hasattr(e, "errors"):
                for error in e.errors():
                    field = ".".join(str(loc) for loc in error["loc"])
                    message = error["msg"]
                    errors.setdefault(field, []).append(message)
            else:
                errors.setdefault("config", []).append(str(e))

        return errors

    @staticmethod
    def validate_strategy_config(strategy_type: str, config: Dict) -> Dict[str, List[str]]:
        """
        Validate configuration for a specific strategy type using Pydantic models.

        Args:
            strategy_type: Type of strategy (e.g., 'basic', 'mid_price')
            config: Configuration dictionary to validate

        Returns:
            Dictionary of validation errors by field, or empty dict if valid
        """
        errors = {}

        try:
            # Select the appropriate model based on strategy type
            if strategy_type == "basic":
                BasicStrategyConfig(**config)
            elif strategy_type == "mid_price":
                MidPriceStrategyConfig(**config)
            else:
                # Use base model for unknown strategy types
                BaseStrategyConfig(**config)

        except Exception as e:
            # Convert pydantic validation errors to our format
            if hasattr(e, "errors"):
                for error in e.errors():
                    field = ".".join(str(loc) for loc in error["loc"])
                    message = error["msg"]
                    errors.setdefault(field, []).append(message)
            else:
                errors.setdefault("config", []).append(str(e))

        return errors

    @staticmethod
    def get_validated_component_config(component_name: str, config: Dict) -> Union[Dict, BaseComponentConfig]:
        """
        Get a validated component configuration, returning the Pydantic model if valid.

        Args:
            component_name: Name of the component
            config: Configuration dictionary

        Returns:
            Validated Pydantic model if valid, original config dict if component unknown

        Raises:
            ValueError: If configuration is invalid
        """
        if component_name == "detection":
            return DetectionComponentConfig(**config)
        elif component_name == "execution":
            return ExecutionComponentConfig(**config)
        elif component_name == "reporting":
            return ReportingComponentConfig(**config)
        elif component_name == "ingestion":
            return IngestionComponentConfig(**config)
        else:
            return config  # Return original for unknown components

    @staticmethod
    def get_validated_strategy_config(strategy_type: str, config: Dict) -> Union[Dict, BaseStrategyConfig]:
        """
        Get a validated strategy configuration, returning the Pydantic model if valid.

        Args:
            strategy_type: Type of strategy
            config: Configuration dictionary

        Returns:
            Validated Pydantic model if valid, original config dict if strategy type unknown

        Raises:
            ValueError: If configuration is invalid
        """
        if strategy_type == "basic":
            return BasicStrategyConfig(**config)
        elif strategy_type == "mid_price":
            return MidPriceStrategyConfig(**config)
        else:
            return config  # Return original for unknown strategy types
