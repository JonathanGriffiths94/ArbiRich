import logging
from typing import Dict, List

from src.arbirich.config.config import STRATEGIES, get_strategy_config
from src.arbirich.config.constants import ORDER_BOOK_CHANNEL, TRADE_EXECUTIONS_CHANNEL, TRADE_OPPORTUNITIES_CHANNEL

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class StrategyManager:
    """Manages strategy registration, configuration, and channel subscriptions"""

    def __init__(self):
        self.strategies = {}
        self.active_strategies = set()
        self.strategy_channels = {}
        self._load_strategies()

    def _load_strategies(self):
        """Load strategies from configuration"""
        for strategy_name, is_active in STRATEGIES.items():
            config = get_strategy_config(strategy_name)
            if not config:
                logger.warning(f"No configuration found for strategy {strategy_name}")
                continue

            self.strategies[strategy_name] = config
            if is_active:
                self.active_strategies.add(strategy_name)

            # Set up channels for this strategy
            self._configure_strategy_channels(strategy_name, config)

    def _configure_strategy_channels(self, strategy_name: str, config: Dict):
        """Set up Redis channels for a strategy"""
        channels = {
            "opportunities": f"{TRADE_OPPORTUNITIES_CHANNEL}:{strategy_name}",
            "executions": f"{TRADE_EXECUTIONS_CHANNEL}:{strategy_name}",
            "order_books": [],
        }

        # Add order book channels for each exchange/pair combo
        exchanges = config.get("exchanges", [])
        pairs = config.get("pairs", [])

        for exchange in exchanges:
            # Subscribe to exchange-level channel
            exchange_channel = f"{ORDER_BOOK_CHANNEL}:{exchange}"
            channels["order_books"].append(exchange_channel)

            # Subscribe to specific pair channels
            for pair in pairs:
                if isinstance(pair, (list, tuple)) and len(pair) == 2:
                    symbol = f"{pair[0]}-{pair[1]}"
                elif isinstance(pair, str):
                    symbol = pair
                else:
                    continue

                pair_channel = f"{ORDER_BOOK_CHANNEL}:{exchange}:{symbol}"
                channels["order_books"].append(pair_channel)

        self.strategy_channels[strategy_name] = channels

    def get_strategy_channels(self, strategy_name: str) -> Dict[str, List[str]]:
        """Get Redis channels for a specific strategy"""
        if strategy_name not in self.strategies:
            logger.warning(f"Strategy {strategy_name} not registered")
            return {"opportunities": [], "executions": [], "order_books": []}

        return self.strategy_channels.get(strategy_name, {"opportunities": [], "executions": [], "order_books": []})

    def get_all_channels(self) -> List[str]:
        """Get all channels across all strategies"""
        all_channels = set([ORDER_BOOK_CHANNEL, TRADE_OPPORTUNITIES_CHANNEL, TRADE_EXECUTIONS_CHANNEL])

        # Add all strategy-specific channels
        for strategy_name, channels in self.strategy_channels.items():
            all_channels.add(channels["opportunities"])
            all_channels.add(channels["executions"])
            all_channels.update(channels["order_books"])

        return list(all_channels)

    def is_strategy_active(self, strategy_name: str) -> bool:
        """Check if a strategy is active"""
        return strategy_name in self.active_strategies

    def activate_strategy(self, strategy_name: str) -> bool:
        """Activate a strategy"""
        if strategy_name not in self.strategies:
            logger.warning(f"Cannot activate unknown strategy: {strategy_name}")
            return False

        self.active_strategies.add(strategy_name)
        logger.info(f"Activated strategy: {strategy_name}")
        return True

    def deactivate_strategy(self, strategy_name: str) -> bool:
        """Deactivate a strategy"""
        if strategy_name not in self.active_strategies:
            logger.warning(f"Strategy not active: {strategy_name}")
            return False

        self.active_strategies.remove(strategy_name)
        logger.info(f"Deactivated strategy: {strategy_name}")
        return True
