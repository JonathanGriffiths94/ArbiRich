from typing import Optional

from src.arbirich.execution_strategies.base_execution_strategy import ExecutionStrategy
from src.arbirich.factories.factory_base import FactoryBase

# Singleton instance
_instance = FactoryBase[ExecutionStrategy]("ExecutionStrategy")

# Create registration decorator
register_strategy = _instance.create_decorator()


def get_strategy(name: str, config: dict = None) -> Optional[ExecutionStrategy]:
    """Get or create an execution strategy instance"""
    return _instance.get_instance(name, True, name=name, config=config or {})


def get_strategy_for_arbitrage(arbitrage_strategy_name: str) -> Optional[ExecutionStrategy]:
    """Get the appropriate execution strategy for a given arbitrage strategy."""
    from src.arbirich.config import EXECUTION_STRATEGY_MAP, get_execution_strategy_config

    # Get the execution strategy name
    execution_strategy_name = EXECUTION_STRATEGY_MAP.get(arbitrage_strategy_name, "basic_market")

    # Get the configuration for this strategy
    config = get_execution_strategy_config(execution_strategy_name)

    # Return the strategy instance
    return get_strategy(execution_strategy_name, config)
