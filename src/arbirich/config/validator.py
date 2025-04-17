from typing import Dict, List, Union

from src.arbirich.models import (
    BaseComponentConfig,
    BaseStrategyConfig,
    BasicStrategyConfig,
    DetectionComponentConfig,
    ExecutionComponentConfig,
    IngestionComponentConfig,
    LiquidityAdjustedStrategyConfig,
    MidPriceStrategyConfig,
    ReportingComponentConfig,
    StrategyType,
    VWAPStrategyConfig,
)


class ConfigValidator:
    """Utility class for validating component and strategy configurations using Pydantic models."""

    _strategy_type_map = {
        StrategyType.BASIC: BasicStrategyConfig,
        StrategyType.MID_PRICE: MidPriceStrategyConfig,
        StrategyType.VWAP: VWAPStrategyConfig,
        StrategyType.LIQUIDITY_ADJUSTED: LiquidityAdjustedStrategyConfig,
    }

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
            # Convert string to enum if needed
            if isinstance(strategy_type, str):
                try:
                    strategy_type = StrategyType(strategy_type)
                except ValueError:
                    pass

            # Get the appropriate model
            model = ConfigValidator._strategy_type_map.get(strategy_type, BaseStrategyConfig)
            model(**config)

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
        if isinstance(strategy_type, str):
            try:
                strategy_type = StrategyType(strategy_type)
            except ValueError:
                pass

        model = ConfigValidator._strategy_type_map.get(strategy_type, BaseStrategyConfig)
        return model(**config)
