"""Registry for accessing and creating configuration models."""

from typing import Any, Dict, Type

from src.arbirich.core.config.validator import (
    BaseComponentConfig,
    BaseStrategyConfig,
    BasicStrategyConfig,
    ConfigValidator,
    DetectionComponentConfig,
    ExecutionComponentConfig,
    IngestionComponentConfig,
    LiquidityAdjustedStrategyConfig,
    MidPriceStrategyConfig,
    ReportingComponentConfig,
    VwapStrategyConfig,
)
from src.arbirich.models.enums import StrategyType  # Use StrategyType from enums


class ConfigModelRegistry:
    """
    Registry for accessing and creating configuration models.

    This class provides a centralized way to access and create Pydantic models
    for component and strategy configurations.
    """

    _component_models = {
        "detection": DetectionComponentConfig,
        "execution": ExecutionComponentConfig,
        "reporting": ReportingComponentConfig,
        "ingestion": IngestionComponentConfig,
        "base": BaseComponentConfig,
    }

    _strategy_models = {
        StrategyType.BASIC: BasicStrategyConfig,
        StrategyType.MID_PRICE: MidPriceStrategyConfig,
        StrategyType.VWAP: VwapStrategyConfig,
        StrategyType.LIQUIDITY_ADJUSTED: LiquidityAdjustedStrategyConfig,
        "base": BaseStrategyConfig,  # Keep 'base' as string - it's not an enum value
    }

    @classmethod
    def get_component_model(cls, component_name: str) -> Type[BaseComponentConfig]:
        """
        Get the Pydantic model class for a component type.

        Args:
            component_name: Name of the component

        Returns:
            Pydantic model class for the component
        """
        return cls._component_models.get(component_name, BaseComponentConfig)

    @classmethod
    def get_strategy_model(cls, strategy_type: str) -> Type[BaseStrategyConfig]:
        """
        Get the Pydantic model class for a strategy type.

        Args:
            strategy_type: Type of strategy

        Returns:
            Pydantic model class for the strategy
        """
        # Handle both string and enum values
        if isinstance(strategy_type, StrategyType):  # Use StrategyType from enums
            return cls._strategy_models.get(strategy_type, BaseStrategyConfig)

        # Try to match with enum value
        try:
            strat_type = StrategyType(strategy_type)  # Use StrategyType from enums
            return cls._strategy_models.get(strat_type, BaseStrategyConfig)
        except ValueError:
            # Fall back to checking raw string
            for key, value in cls._strategy_models.items():
                if str(key) == strategy_type:
                    return value

            # Default to base model
            return BaseStrategyConfig

    @classmethod
    def create_component_config(cls, component_name: str, config: Dict[str, Any]) -> BaseComponentConfig:
        """
        Create a validated component configuration model.

        Args:
            component_name: Name of the component
            config: Raw configuration dictionary

        Returns:
            Validated Pydantic model

        Raises:
            ValueError: If the configuration is invalid
        """
        model_class = cls.get_component_model(component_name)
        return model_class(**config)

    @classmethod
    def create_strategy_config(cls, strategy_type: str, config: Dict[str, Any]) -> BaseStrategyConfig:
        """
        Create a validated strategy configuration model.

        Args:
            strategy_type: Type of strategy
            config: Raw configuration dictionary

        Returns:
            Validated Pydantic model

        Raises:
            ValueError: If the configuration is invalid
        """
        model_class = cls.get_strategy_model(strategy_type)
        return model_class(**config)

    @classmethod
    def validate_component_config(cls, component_name: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a component configuration.

        Args:
            component_name: Name of the component
            config: Raw configuration dictionary

        Returns:
            Dictionary of validation errors, or empty dict if valid
        """
        return ConfigValidator.validate_component_config(component_name, config)

    @classmethod
    def validate_strategy_config(cls, strategy_type: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate a strategy configuration.

        Args:
            strategy_type: Type of strategy
            config: Raw configuration dictionary

        Returns:
            Dictionary of validation errors, or empty dict if valid
        """
        return ConfigValidator.validate_strategy_config(strategy_type, config)
