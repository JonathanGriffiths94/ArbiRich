import json
import logging
import threading
import time
from typing import Dict, List, Optional, Tuple

import redis
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

from arbirich.core.state.system_state import is_system_shutting_down
from src.arbirich.core.trading.flows.bytewax_flows.common.redis_utils import (
    close_all_pubsubs,
    get_redis_client,
    register_pubsub,
    reset_all_redis_connections,
)
from src.arbirich.core.trading.flows.bytewax_flows.common.shutdown_utils import is_force_kill_set, mark_force_kill
from src.arbirich.models.models import OrderBookUpdate
from src.arbirich.services.redis.redis_service import register_redis_client

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Add a debug mode toggle
_debug_mode = False
_debug_lock = threading.Lock()


def mark_detection_force_kill():
    """Mark detection for force-kill, ensuring all partitions exit immediately"""
    mark_force_kill("detection")

    # Additional aggressive cleanup actions
    try:
        # Close all tracked pubsubs immediately
        close_all_pubsubs()

        # Force clean TCP connections to Redis
        reset_all_redis_connections()

        # Force exit if called in standalone script context
        if __name__ == "__main__":
            import os
            import signal

            logger.warning("🛑 Force killing process in standalone mode")
            # Try SIGTERM first
            try:
                os.kill(os.getpid(), signal.SIGTERM)
                time.sleep(0.5)  # Give it a moment
            except Exception as e:
                logger.warning(f"⚠️ Error sending SIGTERM: {e}")

            # Use os._exit which doesn't call cleanup handlers
            try:
                os._exit(1)
            except Exception as e:
                logger.error(f"❌ Failed to exit process: {e}")
    except Exception as e:
        logger.error(f"❌ Error during aggressive detection cleanup: {e}")
        # Last resort - try to force exit
        try:
            import os

            os._exit(1)
        except Exception as e:
            logger.error(f"❌ Failed to exit process: {e}")


def enable_debug_mode():
    """Enable debug mode for detailed arbitrage detection logging"""
    global _debug_mode
    with _debug_lock:
        _debug_mode = True
        logger.info("🔍 DETECTION DEBUG MODE ENABLED - detailed arbitrage logs will be shown")


def disable_debug_mode():
    """Disable debug mode for arbitrage detection logging"""
    global _debug_mode
    with _debug_lock:
        _debug_mode = False
        logger.info("🔒 DETECTION DEBUG MODE DISABLED - returning to normal logging")


def is_debug_mode_enabled():
    """Check if debug mode is enabled"""
    with _debug_lock:
        return _debug_mode


def reset_shared_redis_client():
    """Reset the shared Redis client with proper cleanup."""
    # First set the force-kill flag to make partitions exit
    mark_detection_force_kill()

    # Use the common utilities to reset all Redis connections
    reset_all_redis_connections()

    # Explicitly wait a brief moment after reset to allow connections to fully close
    time.sleep(0.1)

    # Now try to establish a fresh connection to ensure it's working
    try:
        from src.arbirich.services.redis.redis_service import RedisService

        # Create and immediately close a Redis connection to verify connectivity
        test_client = RedisService()
        is_healthy = test_client.is_healthy()
        test_client.close()

        if is_healthy:
            logger.info("✅ Redis connectivity verified after reset")
        else:
            logger.warning("⚠️ New Redis connection not healthy after reset")
    except Exception as e:
        logger.error(f"❌ Failed to verify Redis connectivity after reset: {e}")

    return True


register_redis_client("detection", reset_shared_redis_client)


class RedisExchangePartition(StatefulSourcePartition):
    """Redis source partition for order book updates specific to an exchange."""

    def __init__(self, exchange: str, strategy_name: str):
        # Initialize basic properties
        self.exchange = exchange
        self.strategy_name = strategy_name
        self.redis_client = get_redis_client("detection")
        self.pubsub = None
        self.last_activity = time.time()
        self.pairs = []
        self.shutdown_checked = False
        self.initialization_failed = False
        self.partitions_closed = False

        # Exit immediately if in shutdown or force-kill mode
        if is_system_shutting_down() or is_force_kill_set("detection"):
            self.initialization_failed = True
            logger.info(f"⏭️ Skipping initialization for {exchange} - system is shutting down")
            return

        # Try to initialize
        try:
            self.pairs = self._get_strategy_pairs()
            self._initialize_pubsub()
        except Exception as e:
            self.initialization_failed = True
            logger.error(f"❌ Error initializing partition for {exchange}: {e}")

    def _get_strategy_pairs(self) -> List[str]:
        """Get trading pairs for this strategy from database using the new schema structure"""
        try:
            from src.arbirich.services.database.database_service import DatabaseService
            from src.arbirich.services.database.repositories.strategy_repository import StrategyRepository

            db_service = DatabaseService()
            strategy_repo = StrategyRepository(engine=db_service.engine)

            strategy = strategy_repo.get_by_name(self.strategy_name)

            if not strategy:
                raise ValueError(f"Strategy '{self.strategy_name}' not found in database")

            if not hasattr(strategy, "exchange_pair_mappings") or not strategy.exchange_pair_mappings:
                raise ValueError(f"Strategy {self.strategy_name} has no exchange_pair_mappings")

            pairs = []
            for mapping in strategy.exchange_pair_mappings:
                if mapping.is_active and mapping.exchange_name == self.exchange:
                    pair_symbol = mapping.pair_symbol
                    if pair_symbol and pair_symbol not in pairs:
                        pairs.append(pair_symbol)

            if not pairs:
                raise ValueError(f"No active pairs found for exchange {self.exchange} in strategy {self.strategy_name}")

            logger.info(f"📋 Using pairs from exchange_pair_mappings: {pairs}")
            return pairs

        except Exception as e:
            logger.error(f"❌ Error getting pairs from database for strategy {self.strategy_name}: {e}", exc_info=True)
            raise

    def _initialize_pubsub(self):
        """Initialize Redis PubSub and subscribe to relevant channels"""
        # Quick exit if shutting down or force-kill set
        if is_system_shutting_down() or is_force_kill_set("detection"):
            logger.info(f"🛑 System shutting down, not initializing pubsub for {self.exchange}")
            return

        if not self.redis_client:
            self.redis_client = get_redis_client("detection")
            if not self.redis_client:
                logger.warning(f"⚠️ Cannot initialize pubsub for {self.exchange} - Redis client unavailable")
                self.initialization_failed = True
                return

        try:
            # Create the pubsub object
            if not self.pubsub:
                # Access the Redis client properly
                if hasattr(self.redis_client, "client"):
                    self.pubsub = self.redis_client.client.pubsub(ignore_subscribe_messages=True)
                else:
                    # Fallback if the structure is different
                    self.pubsub = self.redis_client.pubsub(ignore_subscribe_messages=True)

                # Track this pubsub for global cleanup
                register_pubsub(self.pubsub)

                logger.info(f"🔌 Created pubsub for {self.exchange}")

            # Get channels from pairs
            from src.arbirich.models.enums import ChannelName

            channels = []
            for pair in self.pairs:
                channel = f"{ChannelName.ORDER_BOOK.value}:{self.exchange}:{pair}"
                channels.append(channel)

            if not channels:
                logger.warning(f"⚠️ No channels to subscribe for {self.exchange}")
                return

            # Subscribe to channels
            try:
                self.pubsub.subscribe(*channels)
                logger.info(f"📡 Subscribed to {len(channels)} channels for {self.exchange}")
            except Exception as e:
                logger.error(f"❌ Error subscribing to channels for {self.exchange}: {e}")
                self.initialization_failed = True

        except Exception as e:
            logger.error(f"❌ Error initializing pubsub for {self.exchange}: {e}")
            self.initialization_failed = True

    def next_batch(self) -> List[Tuple[str, OrderBookUpdate]]:
        """Get the next batch of order book updates."""
        # Immediately return empty batch for a failed partition
        if self.initialization_failed:
            if is_system_shutting_down() or is_force_kill_set("detection"):
                # If we're shutting down, try to close just to be sure
                if not self.partitions_closed:
                    self.close()
                    self.partitions_closed = True
            return []

        # Check if we're shutting down - first priority check
        if is_system_shutting_down() or is_force_kill_set("detection"):
            if not self.shutdown_checked:
                logger.info(f"🛑 System shutdown detected in {self.exchange} partition")
                self.shutdown_checked = True
                self.close()
                self.partitions_closed = True
            return []

        # Reset shutdown flag during normal operation
        self.shutdown_checked = False

        try:
            # Try to get items with a timeout to ensure we can check shutdown
            start_time = time.time()
            timeout = 0.1  # Short timeout to allow frequent shutdown checks

            # Keep trying until we get an item or timeout
            while time.time() - start_time < timeout:
                # Check shutdown again to abort quickly
                if is_system_shutting_down() or is_force_kill_set("detection"):
                    return []

                item = self.next()
                if item:
                    return [item]

                # Brief sleep to prevent tight CPU loops
                time.sleep(0.01)

            # No item found within timeout
            return []

        except redis.exceptions.ConnectionError as e:
            # Don't attempt reconnection during shutdown
            if is_system_shutting_down() or is_force_kill_set("detection"):
                logger.info(f"⚠️ Connection error during shutdown for {self.exchange}, not reconnecting")
                self.close()
                self.partitions_closed = True
                return []

            # During normal operation, try to reconnect
            logger.warning(f"⚠️ Redis connection error in {self.exchange} partition: {e}")
            try:
                logger.info(f"🔄 Attempting to reconnect Redis for {self.exchange} partition")
                self.close()  # Clean close of existing connection
                self.pubsub = None
                self.redis_client = get_redis_client("detection")
                self._initialize_pubsub()
            except Exception as reconnect_error:
                logger.error(f"❌ Failed to reconnect Redis for {self.exchange}: {reconnect_error}")
                self.initialization_failed = True

            return []

        except Exception as e:
            logger.error(f"❌ Unexpected error in next_batch for {self.exchange}: {e}")

            # Brief sleep to prevent tight error loops
            time.sleep(0.1)
            return []

    def next(self) -> Optional[Tuple[str, OrderBookUpdate]]:
        """Get the next order book update."""
        # Quick exit for shutdown
        if is_system_shutting_down() or is_force_kill_set("detection") or self.initialization_failed:
            return None

        if not self.pubsub:
            if is_system_shutting_down() or is_force_kill_set("detection"):
                return None

            try:
                self._initialize_pubsub()
                if not self.pubsub:
                    logger.warning(f"⚠️ Failed to initialize pubsub for {self.exchange}")
                    return None
            except Exception as e:
                logger.error(f"❌ Error initializing pubsub for {self.exchange}: {e}")
                self.initialization_failed = True
                return None

        try:
            # Final check before attempting to get a message
            if is_system_shutting_down() or is_force_kill_set("detection"):
                return None

            # Use very short timeout (non-blocking mode) to prevent getting stuck
            timeout = 0.001  # 1ms timeout for ultra-responsiveness to shutdown

            message = None
            try:
                message = self.pubsub.get_message(timeout=timeout)
            except Exception as e:
                if "connection closed" in str(e).lower() or "connection reset" in str(e).lower():
                    # Common error during shutdown - handle gracefully
                    if not is_system_shutting_down():
                        logger.warning(f"⚠️ Connection closed for {self.exchange}, will try to reconnect")
                        self.pubsub = None  # Force reconnect on next call
                    return None
                raise  # Re-raise other errors

            if not message:
                return None

            channel = message.get("channel", b"")
            if isinstance(channel, bytes):
                channel = channel.decode("utf-8")
            elif not isinstance(channel, str):
                channel = str(channel)

            data = message.get("data")
            logger.info(f"📬 Received message on channel: {channel} for {self.exchange}")

            if not data:
                logger.warning(f"⚠️ Empty data in message on channel {channel}")
                return None

            try:
                if isinstance(data, bytes):
                    data_str = data.decode("utf-8")
                    logger.info(f"📦 Decoded bytes data (first 100 chars): {data_str[:100]}...")
                    data = json.loads(data_str)
                elif isinstance(data, str):
                    logger.info(f"📦 String data (first 100 chars): {data[:100]}...")
                    data = json.loads(data)

                if isinstance(data, dict) and data.get("type") == "heartbeat":
                    logger.info(f"💓 Received heartbeat on channel {channel}")
                    return None

                if isinstance(data, dict):
                    exchange_id = self.exchange
                    symbol = None

                    if ":" in channel:
                        parts = channel.split(":")
                        if len(parts) >= 2:
                            exchange_id = parts[1]
                            if len(parts) >= 3:
                                symbol = parts[2]

                    if not symbol:
                        symbol = data.get("symbol", "unknown")

                    logger.info(f"📦 PROCESSING ORDER BOOK: Exchange={exchange_id}, Symbol={symbol}")

                    logger.info(f"Order book data keys: {list(data.keys())}")
                    if "bids" in data:
                        bid_count = len(data["bids"])
                        logger.info(
                            f"Bids sample: {list(data['bids'].items())[:2] if isinstance(data['bids'], dict) else data['bids'][:2]}"
                        )
                    if "asks" in data:
                        ask_count = len(data["asks"])
                        logger.info(
                            f"Asks sample: {list(data['asks'].items())[:2] if isinstance(data['asks'], dict) else data['asks'][:2]}"
                        )

                    order_book = OrderBookUpdate(
                        id=data.get("id", ""),
                        exchange=exchange_id,
                        symbol=symbol,
                        bids=data.get("bids", {}),
                        asks=data.get("asks", {}),
                        timestamp=data.get("timestamp", time.time()),
                        sequence=data.get("sequence"),
                    )

                    bid_count = len(order_book.bids) if hasattr(order_book, "bids") else 0
                    ask_count = len(order_book.asks) if hasattr(order_book, "asks") else 0

                    logger.info(
                        f"📊 ORDER BOOK DETAILS: {exchange_id}:{symbol}\n"
                        f"  • Bids: {bid_count}, Asks: {ask_count}\n"
                        f"  • Timestamp: {order_book.timestamp}"
                    )

                    return (exchange_id, order_book)
                else:
                    logger.warning(f"⚠️ Unexpected data type: {type(data)}, not a dict")
            except Exception as e:
                logger.error(f"❌ Error processing order book update for {self.exchange}: {e}", exc_info=True)
                logger.error(f"Problem data: {str(data)[:200]}")

            return None
        except redis.exceptions.ConnectionError:
            raise
        except Exception as e:
            logger.error(f"❌ Error processing order book update for {self.exchange}: {e}")

        return None

    def close(self):
        """Close the partition and clean up resources"""
        logger.info(f"🔒 Closing partition for exchange {self.exchange}")

        if self.pubsub:
            try:
                # First try to unsubscribe - but with quick timeout
                try:
                    self.pubsub.unsubscribe()
                except Exception as e:
                    logger.debug(f"⚠️ Error during unsubscribe: {e}")

                # Then try to close
                try:
                    self.pubsub.close()
                except Exception as e:
                    logger.debug(f"⚠️ Error closing pubsub: {e}")

                # Remove from global tracking
                close_all_pubsubs()

                self.pubsub = None
                logger.info(f"✅ Closed PubSub for {self.exchange}")
            except Exception as e:
                logger.warning(f"⚠️ Error during pubsub cleanup: {e}")
                self.pubsub = None

        # Mark as failed to prevent further usage
        self.initialization_failed = True
        self.partitions_closed = True

    def snapshot(self) -> Optional[Dict]:
        """Take snapshot of current state for recovery.

        This method is required by StatefulSourcePartition abstract class.
        """
        return {"exchange": self.exchange, "last_activity": self.last_activity}

    def restore(self, state: Dict):
        """Restore from snapshot.

        This method is required by StatefulSourcePartition abstract class.
        """
        self.last_activity = state.get("last_activity", time.time())


class RedisOrderBookSource(FixedPartitionedSource):
    """Redis source for order book updates from multiple exchanges."""

    # Class-level storage of partitions for shutdown cleanup
    partitions = {}

    def __init__(
        self,
        exchanges: List[str] = None,
        strategy_name: str = None,
        exchange_channels: Dict[str, str] = None,
        pairs: List[str] = None,
        stop_event=None,
    ):
        if exchange_channels:
            self.exchanges = list(exchange_channels.keys())
        else:
            self.exchanges = exchanges or []

        self.strategy_name = strategy_name
        self.exchange_channels = exchange_channels or {}
        self.pairs = pairs or []
        self.stop_event = stop_event
        self.partitions = {}
        RedisOrderBookSource.partitions = {}

        logger.info(f"📋 List of partitions: {self.exchanges}")

    def list_parts(self) -> List[str]:
        return self.exchanges

    def build_part(self, step_id, for_key, resume_state) -> RedisExchangePartition:
        """Build a partition for an exchange."""
        # Fast exit during shutdown
        if is_system_shutting_down() or is_force_kill_set("detection"):
            logger.info(f"🛑 System shutting down, returning minimal partition for {for_key}")
            minimal_partition = RedisExchangePartition(for_key, self.strategy_name)
            minimal_partition.initialization_failed = True
            minimal_partition.shutdown_checked = True
            return minimal_partition

        logger.info(f"🔧 Building partition for exchange: {for_key} with strategy: {self.strategy_name}")

        # Create and store the partition
        partition = RedisExchangePartition(for_key, self.strategy_name)
        RedisOrderBookSource.partitions[for_key] = partition
        self.partitions[for_key] = partition

        return partition

    def close(self):
        """Close all partitions and clean up resources."""
        logger.info(f"🔒 Closing {len(self.partitions)} partitions")

        # Set force-kill flag to ensure everything exits quickly
        mark_detection_force_kill()

        # Close all partitions
        for exchange, partition in list(self.partitions.items()):
            try:
                partition.close()
                logger.info(f"✅ Closed partition for {exchange}")
            except Exception as e:
                logger.error(f"❌ Error closing partition for {exchange}: {e}")

        # Clear the partitions
        self.partitions.clear()
        RedisOrderBookSource.partitions.clear()
        logger.info("🧹 All partitions cleared")

        # Also reset the Redis client
        reset_shared_redis_client()
