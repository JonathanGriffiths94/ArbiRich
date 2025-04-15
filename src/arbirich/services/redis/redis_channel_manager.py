import logging
import threading

from src.arbirich.models.enums import ChannelName
from src.arbirich.models.models import OrderBookUpdate, TradeExecution, TradeOpportunity

logger = logging.getLogger(__name__)

# Global shared instance for the channel manager
_channel_manager = None
_channel_manager_lock = threading.RLock()


def get_channel_manager():
    """Get or create a shared channel manager instance"""
    global _channel_manager
    with _channel_manager_lock:
        if _channel_manager is None:
            from src.arbirich.services.redis.redis_service import get_shared_redis_client

            redis_client = get_shared_redis_client()
            if redis_client:
                _channel_manager = RedisChannelManager(redis_client)
            else:
                logger.error("Failed to get Redis client for channel manager")
                return None
        return _channel_manager


def reset_channel_manager():
    """Reset the shared channel manager instance"""
    global _channel_manager
    with _channel_manager_lock:
        _channel_manager = None
        logger.info("Channel manager reset")


class RedisChannelManager:
    """Manages Redis channel subscriptions and publishing"""

    def __init__(self, redis_service):
        self.redis = redis_service
        self.logger = redis_service.logger if hasattr(redis_service, "logger") else None

    def publish_execution(self, execution: TradeExecution) -> int:
        """
        Publish execution data to main execution channel only

        Args:
            execution: Execution data object

        Returns:
            Number of subscribers that received the message
        """
        try:
            # Use the repository pattern through redis_service
            subscribers = self.redis.get_trade_execution_repository().publish(execution)

            if self.logger:
                channel = ChannelName.TRADE_EXECUTIONS.value
                self.logger.info(f"Published execution to {channel} with {subscribers} subscribers")

            return subscribers
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error publishing execution: {e}")
            return 0

    def publish_opportunity(self, opportunity: TradeOpportunity) -> int:
        """
        Publish opportunity data to main opportunity channel and strategy-specific channel

        Args:
            opportunity: Opportunity data object

        Returns:
            Number of subscribers that received the message
        """
        try:
            # Use the repository pattern through redis_service
            subscribers = self.redis.get_trade_opportunity_repository().publish(opportunity)

            # Log successful publishing
            if self.logger:
                channel = ChannelName.TRADE_OPPORTUNITIES.value
                strategy = opportunity.strategy
                if strategy:
                    strategy_channel = f"{channel}:{strategy}"
                    self.logger.info(
                        f"Published opportunity to {channel} and {strategy_channel} with {subscribers} total subscribers"
                    )
                else:
                    self.logger.info(f"Published opportunity to {channel} with {subscribers} subscribers")

            return subscribers
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error publishing opportunity: {e}")
            return 0

    def publish_order_book(self, order_book: OrderBookUpdate, exchange: str = None, symbol: str = None) -> int:
        """
        Publish order book data to the appropriate channel

        Args:
            order_book: Order book data object
            exchange: Optional exchange override (defaults to order_book.exchange)
            symbol: Optional symbol override (defaults to order_book.symbol)

        Returns:
            Number of subscribers that received the message
        """
        try:
            # Use the repository pattern through redis_service
            subscribers = self.redis.get_order_book_repository().publish(order_book, exchange, symbol)

            if self.logger:
                exchange_name = exchange or order_book.exchange
                symbol_name = symbol or order_book.symbol
                channel = f"{ChannelName.ORDER_BOOK.value}:{exchange_name}:{symbol_name}"
                self.logger.debug(f"Published order book to {channel} with {subscribers} subscribers")

            return subscribers
        except Exception as e:
            if self.logger:
                self.logger.error(f"Error publishing order book: {e}")
            return 0

    def get_opportunity_channel(self) -> str:
        """Get the standard opportunity channel name"""
        return ChannelName.TRADE_OPPORTUNITIES.value

    def get_execution_channel(self) -> str:
        """Get the standard execution channel name"""
        return ChannelName.TRADE_EXECUTIONS.value
