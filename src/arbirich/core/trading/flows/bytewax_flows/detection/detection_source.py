import json
import logging
import time
from typing import Dict, List, Optional, Tuple

import redis
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

from arbirich.constants import ORDER_BOOK_CHANNEL
from src.arbirich.models.models import OrderBookUpdate
from src.arbirich.services.database.database_service import DatabaseService
from src.arbirich.services.redis.redis_service import get_shared_redis_client, register_redis_client

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

_redis_client = None


def get_redis_client():
    """Get or create a Redis client for this module."""
    global _redis_client
    if _redis_client is None:
        from src.arbirich.services.redis.redis_service import RedisService

        _redis_client = RedisService().client
    return _redis_client


def reset_shared_redis_client():
    """Reset the shared Redis client."""
    global _redis_client
    _redis_client = None


register_redis_client("detection", reset_shared_redis_client)


class RedisExchangePartition(StatefulSourcePartition):
    """
    Redis source partition for order book updates specific to an exchange.
    """

    def __init__(self, exchange: str, strategy_name: str):
        self.exchange = exchange
        self.strategy_name = strategy_name
        self.redis_client = get_shared_redis_client()
        self.pubsub = None
        self.last_activity = time.time()
        self.pairs = self._get_strategy_pairs()

        # Initialize pubsub
        self._initialize_pubsub()

    def _get_strategy_pairs(self) -> List[str]:
        """Get trading pairs for this strategy from database"""
        try:
            with DatabaseService() as db:
                strategy = db.get_strategy_by_name(self.strategy_name)
                if not strategy or not strategy.additional_info:
                    logger.warning(f"No strategy found or no pairs defined for {self.strategy_name}")
                    return []

                # Extract pairs from strategy config
                if isinstance(strategy.additional_info, dict):
                    pairs = strategy.additional_info.get("pairs", [])
                else:
                    # Parse JSON if stored as string
                    import json

                    try:
                        additional_info = json.loads(strategy.additional_info)
                        pairs = additional_info.get("pairs", [])
                    except json.JSONDecodeError:
                        logger.error(f"Could not parse additional_info for strategy {self.strategy_name}")
                        return []

                # Normalize pairs to string format
                normalized_pairs = []
                for pair in pairs:
                    if isinstance(pair, (list, tuple)) and len(pair) == 2:
                        normalized_pairs.append(f"{pair[0]}-{pair[1]}")
                    elif isinstance(pair, str):
                        normalized_pairs.append(pair)

                logger.info(f"Strategy {self.strategy_name} uses pairs: {normalized_pairs}")
                return normalized_pairs
        except Exception as e:
            logger.error(f"Error getting pairs for strategy {self.strategy_name}: {e}")
            return []

    def _initialize_pubsub(self):
        """Initialize Redis PubSub and subscribe to relevant channels"""
        if not self.redis_client:
            logger.error("No Redis client available")
            return

        if not self.pubsub:
            self.pubsub = self.redis_client.client.pubsub(ignore_subscribe_messages=True)

        # Subscribe to specific channels for each pair
        channels = []
        for pair in self.pairs:
            # Create the specific channel name for this exchange and pair
            channel = f"{ORDER_BOOK_CHANNEL}:{self.exchange}:{pair}"
            channels.append(channel)
            logger.info(f"Subscribing to channel: {channel}")

        # Subscribe to all channels
        if channels:  # Only subscribe if we have channels to subscribe to
            self.pubsub.subscribe(*channels)
            logger.info(
                f"Initialized RedisExchangePartition for {self.exchange} and subscribed to channels: {channels}"
            )
        else:
            # If no specific pairs found, just initialize without subscribing
            logger.warning(f"No channels to subscribe for {self.exchange}, skipping subscription")
            logger.info(f"Initialized RedisExchangePartition for {self.exchange} without channel subscriptions")

    def snapshot(self) -> Optional[Dict]:
        """Take snapshot of current state"""
        return {"exchange": self.exchange, "last_activity": self.last_activity}

    def restore(self, state: Dict):
        """Restore from snapshot"""
        self.last_activity = state.get("last_activity", time.time())

    def next_batch(self) -> List[Tuple[str, OrderBookUpdate]]:
        """
        Get a batch of order book updates. Required by Bytewax's StatefulSourcePartition.

        Returns:
            List of tuples (exchange, OrderBookUpdate)
        """
        try:
            # Try to get a single item using the next() method
            item = self.next()

            # If we got an item, return it as a single-item list
            if item:
                return [item]

            # Otherwise return an empty list
            return []
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Redis connection error in {self.exchange} partition: {e}")
            # Try to reconnect
            try:
                logger.info(f"Attempting to reconnect Redis for {self.exchange} partition")
                # Close existing pubsub if it exists
                if self.pubsub:
                    try:
                        self.pubsub.unsubscribe()
                        self.pubsub.close()
                    except Exception as close_error:
                        logger.warning(f"Error closing pubsub: {close_error}")

                # Reset connection
                self.pubsub = None
                self.redis_client = get_shared_redis_client()

                # Re-initialize pubsub
                self._initialize_pubsub()
                logger.info(f"Successfully reconnected Redis for {self.exchange}")
            except Exception as reconnect_error:
                logger.error(f"Failed to reconnect Redis: {reconnect_error}")

            # Return empty list to allow flow to continue
            return []
        except Exception as e:
            logger.error(f"Unexpected error in next_batch for {self.exchange}: {e}")
            return []

    def next(self) -> Optional[Tuple[str, OrderBookUpdate]]:
        """
        Get the next order book update for this exchange.

        Returns:
            Tuple of (exchange, OrderBookUpdate) or None if no message is available
        """
        if not self.pubsub:
            try:
                self._initialize_pubsub()
                if not self.pubsub:
                    return None
            except Exception as e:
                logger.error(f"Error initializing pubsub for {self.exchange}: {e}")
                return None

        try:
            # Process messages
            message = self.pubsub.get_message(timeout=0.01)
            if not message:
                return None

            # Update activity timestamp
            self.last_activity = time.time()

            # Process message data
            channel = message.get("channel", b"").decode("utf-8")
            data = message.get("data")

            if not data:
                return None

            try:
                # Decode data if it's bytes
                if isinstance(data, bytes):
                    data = json.loads(data.decode("utf-8"))

                # Create OrderBookUpdate object
                if isinstance(data, dict):
                    # Use the full exchange:pair identifier from the channel name
                    # Extract from channel if possible
                    if ":" in channel:
                        parts = channel.split(":")
                        if len(parts) >= 2:
                            exchange_id = parts[1]  # Get exchange part
                            if len(parts) >= 3:
                                # Include the trading pair in the exchange identifier
                                symbol = parts[2]
                            else:
                                symbol = data.get("symbol", "unknown")
                    else:
                        exchange_id = self.exchange
                        symbol = data.get("symbol", "unknown")

                    # Create OrderBookUpdate with proper exchange and symbol
                    order_book = OrderBookUpdate(
                        id=data.get("id", ""),
                        exchange=exchange_id,
                        symbol=symbol,
                        bids=data.get("bids", {}),
                        asks=data.get("asks", {}),
                        timestamp=data.get("timestamp", time.time()),
                        sequence=data.get("sequence"),
                    )

                    return (self.exchange, order_book)
            except Exception as e:
                logger.error(f"Error processing order book update for {self.exchange}: {e}")

            return None
        except redis.exceptions.ConnectionError:
            # Let the connection error be caught by next_batch for reconnection handling
            raise
        except Exception as e:
            logger.error(f"Error processing order book update for {self.exchange}: {e}")

        return None

    def close(self):
        """Close the PubSub connection"""
        if self.pubsub:
            try:
                self.pubsub.unsubscribe()
                self.pubsub.close()
            except Exception as e:
                logger.error(f"Error closing PubSub for {self.exchange}: {e}")


class RedisOrderBookSource(FixedPartitionedSource):
    """
    Redis source for order book updates from multiple exchanges.
    """

    def __init__(
        self,
        exchanges: List[str] = None,
        strategy_name: str = None,
        exchange_channels: Dict[str, str] = None,
        pairs: List[str] = None,
        stop_event=None,
    ):
        """
        Initialize Redis source for order book updates.

        Args:
            exchanges: List of exchange IDs to listen for
            strategy_name: Name of the strategy
            exchange_channels: Dictionary mapping exchange names to channel names
            pairs: List of trading pairs to monitor
            stop_event: Event to signal when to stop
        """
        # Handle either exchanges list or exchange_channels dict
        if exchange_channels:
            self.exchanges = list(exchange_channels.keys())
        else:
            self.exchanges = exchanges or []

        self.strategy_name = strategy_name
        self.exchange_channels = exchange_channels or {}
        self.pairs = pairs or []
        self.stop_event = stop_event
        self.partitions = {}
        logger.info(f"List of partitions: {self.exchanges}")

    def list_parts(self) -> List[str]:
        """List partitions (exchanges)"""
        return self.exchanges

    def build_part(self, step_id, for_key, resume_state) -> RedisExchangePartition:
        """
        Build partition for an exchange.

        Args:
            step_id: The step ID in the dataflow
            for_key: The key (exchange) to build a partition for
            resume_state: Optional state to resume from

        Returns:
            A RedisExchangePartition instance
        """
        logger.info(f"Building partition for exchange: {for_key}")
        return RedisExchangePartition(for_key, self.strategy_name)

    def close(self):
        """Close all partitions"""
        for exchange, partition in self.partitions.items():
            try:
                partition.close()
            except Exception as e:
                logger.error(f"Error closing partition for {exchange}: {e}")
