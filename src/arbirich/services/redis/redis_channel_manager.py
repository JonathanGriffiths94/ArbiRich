import json
import logging
import threading
from typing import Dict, List, Optional, Set, Union

from src.arbirich.constants import ORDER_BOOK_CHANNEL, TRADE_EXECUTIONS_CHANNEL, TRADE_OPPORTUNITIES_CHANNEL
from src.arbirich.models.models import OrderBookUpdate, TradeExecution, TradeOpportunity
from src.arbirich.services.redis.redis_service import RedisService, get_shared_redis_client

logger = logging.getLogger(__name__)

# Singleton instance
_channel_manager_instance = None
_channel_manager_lock = threading.Lock()


def get_channel_manager():
    """Get the singleton RedisChannelManager instance"""
    global _channel_manager_instance
    with _channel_manager_lock:
        if _channel_manager_instance is None:
            _channel_manager_instance = RedisChannelManager()
        return _channel_manager_instance


def reset_channel_manager():
    """Reset the channel manager singleton"""
    global _channel_manager_instance
    with _channel_manager_lock:
        if _channel_manager_instance:
            try:
                _channel_manager_instance.close()
            except Exception as e:
                logger.error(f"Error closing channel manager: {e}")
            _channel_manager_instance = None
    logger.info("Channel manager reset")


class RedisChannelManager:
    """
    Manages Redis channels for different message types and provides
    consistent channel naming across the system.
    """

    # Channel type constants
    ORDER_BOOK = ORDER_BOOK_CHANNEL
    TRADE_OPPORTUNITIES = TRADE_OPPORTUNITIES_CHANNEL
    TRADE_EXECUTIONS = TRADE_EXECUTIONS_CHANNEL

    def __init__(self, redis_service: Optional[RedisService] = None):
        """
        Initialize with an existing RedisService or create a new one

        Args:
            redis_service: Optional existing RedisService instance
        """
        self.redis = redis_service or get_shared_redis_client()
        self._subscribed_channels: Set[str] = set()

    # ===== Channel Name Formatting =====

    def get_orderbook_channel(self, exchange: str, symbol: Optional[str] = None) -> str:
        """
        Get the channel name for order book updates

        Args:
            exchange: Exchange name
            symbol: Optional trading pair symbol

        Returns:
            Channel name string
        """
        if symbol:
            return f"{self.ORDER_BOOK}:{exchange}:{symbol}"
        return f"{self.ORDER_BOOK}:{exchange}"

    def get_opportunity_channel(self, strategy_name: Optional[str] = None) -> str:
        """
        Get the channel name for trade opportunities

        Args:
            strategy_name: Optional strategy name

        Returns:
            Channel name string
        """
        if strategy_name:
            return f"{self.TRADE_OPPORTUNITIES}:{strategy_name}"
        return self.TRADE_OPPORTUNITIES

    def get_execution_channel(self, strategy_name: Optional[str] = None) -> str:
        """
        Get the channel name for trade executions

        Args:
            strategy_name: Optional strategy name

        Returns:
            Channel name string
        """
        if strategy_name:
            return f"{self.TRADE_EXECUTIONS}:{strategy_name}"
        return self.TRADE_EXECUTIONS

    # ===== Publishing Methods =====

    def publish_order_book(self, exchange: str, symbol: str, order_book: Union[Dict, OrderBookUpdate]) -> int:
        """
        Publish an order book update to appropriate channels

        Args:
            exchange: Exchange name
            symbol: Trading pair symbol
            order_book: Order book data (dict or OrderBookUpdate)

        Returns:
            Total number of subscribers reached
        """
        try:
            # Convert OrderBookUpdate to dict if needed
            if hasattr(order_book, "model_dump"):
                data = order_book.model_dump()
            elif hasattr(order_book, "dict"):
                data = order_book.dict()  # For older Pydantic versions
            else:
                data = order_book  # Assume it's already a dict

            # Add exchange and symbol if needed
            if "exchange" not in data:
                data["exchange"] = exchange
            if "symbol" not in data:
                data["symbol"] = symbol

            # Convert to JSON
            message_json = json.dumps(data)

            # Publish to multiple channels for maximum compatibility
            channels = [
                self.ORDER_BOOK,  # Generic channel
                f"{self.ORDER_BOOK}:{exchange}",  # Exchange-specific channel
                f"{self.ORDER_BOOK}:{exchange}:{symbol}",  # Exchange and symbol specific channel
            ]

            total_subscribers = 0
            for channel in channels:
                subscribers = self.redis.client.publish(channel, message_json)
                total_subscribers += subscribers

                if subscribers > 0:
                    logger.debug(f"Published to {channel}: {subscribers} subscribers")

            if total_subscribers > 0:
                logger.debug(f"Order book for {exchange}:{symbol} published to {total_subscribers} subscribers")
            else:
                logger.warning(f"Order book for {exchange}:{symbol} published but no subscribers")

            return total_subscribers

        except Exception as e:
            logger.error(f"Error publishing order book: {e}", exc_info=True)
            return 0

    def publish_opportunity(self, opportunity: TradeOpportunity) -> int:
        """
        Publish a trade opportunity to appropriate channels

        Args:
            opportunity: TradeOpportunity object

        Returns:
            Total number of subscribers reached
        """
        try:
            # Convert to JSON
            message_json = opportunity.model_dump_json()

            # Determine channels to publish to
            channels = [self.TRADE_OPPORTUNITIES]  # Always publish to main channel

            # Add strategy-specific channel if available
            if opportunity.strategy:
                channels.append(f"{self.TRADE_OPPORTUNITIES}:{opportunity.strategy}")

            # Publish to all channels
            total_subscribers = 0
            for channel in channels:
                subscribers = self.redis.client.publish(channel, message_json)
                total_subscribers += subscribers

                if subscribers > 0:
                    logger.debug(f"Published opportunity to {channel}: {subscribers} subscribers")

            if total_subscribers > 0:
                logger.debug(f"Opportunity {opportunity.id} published to {total_subscribers} subscribers")
            else:
                logger.warning(f"Opportunity {opportunity.id} published but no subscribers")

            return total_subscribers

        except Exception as e:
            logger.error(f"Error publishing opportunity: {e}", exc_info=True)
            return 0

    def publish_execution(self, execution: TradeExecution) -> int:
        """
        Publish a trade execution to appropriate channels

        Args:
            execution: TradeExecution object

        Returns:
            Total number of subscribers reached
        """
        try:
            # Convert to JSON
            message_json = execution.model_dump_json()

            # Determine channels to publish to
            channels = [self.TRADE_EXECUTIONS]  # Always publish to main channel

            # Add strategy-specific channel if available
            if execution.strategy:
                channels.append(f"{self.TRADE_EXECUTIONS}:{execution.strategy}")

            # Publish to all channels
            total_subscribers = 0
            for channel in channels:
                subscribers = self.redis.client.publish(channel, message_json)
                total_subscribers += subscribers

                if subscribers > 0:
                    logger.debug(f"Published execution to {channel}: {subscribers} subscribers")

            if total_subscribers > 0:
                logger.debug(f"Execution {execution.id} published to {total_subscribers} subscribers")
            else:
                logger.warning(f"Execution {execution.id} published but no subscribers")

            return total_subscribers

        except Exception as e:
            logger.error(f"Error publishing execution: {e}", exc_info=True)
            return 0

    # ===== Subscription Methods =====

    def subscribe_to_orderbook(self, exchange: str, symbol: Optional[str] = None, callback=None):
        """
        Subscribe to order book updates for an exchange

        Args:
            exchange: Exchange name
            symbol: Optional symbol to filter updates
            callback: Optional callback function for processing messages
        """
        channel = self.get_orderbook_channel(exchange, symbol)

        # Subscribe using Redis pubsub
        pubsub = self.redis.client.pubsub()
        pubsub.subscribe(channel)
        logger.info(f"Subscribed to channel: {channel}")

        return pubsub

    def subscribe_to_opportunities(self, strategy_name: Optional[str] = None, callback=None):
        """
        Subscribe to trade opportunity updates

        Args:
            strategy_name: Optional strategy name to filter updates
            callback: Optional callback function for processing messages
        """
        channel = self.get_opportunity_channel(strategy_name)

        # Subscribe using Redis pubsub
        pubsub = self.redis.client.pubsub()
        pubsub.subscribe(channel)
        logger.info(f"Subscribed to channel: {channel}")

        return pubsub

    def subscribe_to_executions(self, strategy_name: Optional[str] = None, callback=None):
        """
        Subscribe to trade execution updates

        Args:
            strategy_name: Optional strategy name to filter updates
            callback: Optional callback function for processing messages
        """
        channel = self.get_execution_channel(strategy_name)

        # Subscribe using Redis pubsub
        pubsub = self.redis.client.pubsub()
        pubsub.subscribe(channel)
        logger.info(f"Subscribed to channel: {channel}")

        return pubsub

    # ===== Utility Methods =====

    def ensure_subscribed(self, channels: List[str]):
        """
        Ensure subscription to a list of channels

        Args:
            channels: List of channel names
        """
        pubsub = self.redis.client.pubsub()
        for channel in channels:
            if channel not in self._subscribed_channels:
                pubsub.subscribe(channel)
                self._subscribed_channels.add(channel)
                logger.info(f"Subscribed to channel: {channel}")

        return pubsub

    def close(self):
        """Close the Redis connection"""
        self.redis.close()

    def get_all_system_channels(self) -> List[str]:
        """Get a list of all standard system channels"""
        from src.arbirich.config.config import EXCHANGES, PAIRS, STRATEGIES

        channels = [
            # Base channels
            self.ORDER_BOOK,
            self.TRADE_OPPORTUNITIES,
            self.TRADE_EXECUTIONS,
        ]

        # Add strategy-specific channels
        for strategy_name in STRATEGIES.keys():
            channels.append(f"{self.TRADE_OPPORTUNITIES}:{strategy_name}")
            channels.append(f"{self.TRADE_EXECUTIONS}:{strategy_name}")

        # Add exchange-pair specific channels
        for exchange in EXCHANGES:
            for base, quote in PAIRS:
                symbol = f"{base}-{quote}"
                channel = self.get_orderbook_channel(exchange, symbol)
                channels.append(channel)

        return channels
