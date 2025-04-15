import json
import logging
import threading
import time
from threading import Lock
from typing import Dict, Optional

import redis  # Added missing import for redis
from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

from arbirich.core.state.system_state import is_system_shutting_down, mark_component_notified, set_stop_event
from src.arbirich.constants import TRADE_OPPORTUNITIES_CHANNEL
from src.arbirich.models.enums import ChannelName
from src.arbirich.models.models import TradeOpportunity
from src.arbirich.services.redis.redis_service import get_shared_redis_client

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Add this shared client variable at module level
_shared_redis_client = None

# Maintain a debounce dictionary to track recently processed opportunities by pair/exchange
_debounce_dict: Dict[str, float] = {}
_DEBOUNCE_INTERVAL = 1.0  # seconds


def should_process_opportunity(opportunity: TradeOpportunity) -> bool:
    """
    Check if an opportunity should be processed or debounced.

    Args:
        opportunity: The opportunity to check

    Returns:
        True if the opportunity should be processed, False if it should be debounced
    """
    global _debounce_dict

    # Basic validation
    if not opportunity:
        return False

    if not opportunity.buy_exchange or not opportunity.sell_exchange:
        logger.warning(f"Skipping opportunity {opportunity.id}: Missing exchange information")
        return False

    if opportunity.buy_price <= 0 or opportunity.sell_price <= 0:
        logger.warning(f"Skipping opportunity {opportunity.id}: Invalid prices")
        return False

    # Create debounce key based on the exchanges and pair
    debounce_key = f"{opportunity.pair}:{opportunity.buy_exchange}:{opportunity.sell_exchange}"

    current_time = time.time()

    # Check if we've seen this pair/exchange combination recently
    if debounce_key in _debounce_dict:
        last_time = _debounce_dict[debounce_key]
        if current_time - last_time < _DEBOUNCE_INTERVAL:
            logger.info(f"Debouncing opportunity {opportunity.id} for {debounce_key}: Too soon after last one")
            return False

    # Update the debounce time
    _debounce_dict[debounce_key] = current_time

    # Clean up old entries
    for key in list(_debounce_dict.keys()):
        if current_time - _debounce_dict[key] > _DEBOUNCE_INTERVAL * 5:
            del _debounce_dict[key]

    return True


def reset_shared_redis_client() -> bool:
    """
    Reset the shared Redis client for execution flows.

    Returns:
        bool: True if reset was successful, False otherwise
    """
    global _shared_redis_client

    try:
        logger.info("🔄 Resetting shared Redis client for execution flows")

        # Close existing pubsub if any instances exist
        for partition in RedisExecutionSource.__dict__.get("_partitions", {}).values():
            if hasattr(partition, "pubsub") and partition.pubsub:
                try:
                    partition.pubsub.unsubscribe()
                    partition.pubsub.close()
                    partition.pubsub = None  # Set to None to prevent reuse of closed connection
                    logger.info("✅ Closed execution partition pubsub")
                except Exception as e:
                    logger.warning(f"⚠️ Error closing execution partition pubsub: {e}")

        # Reset the shared client
        if _shared_redis_client is not None:
            try:
                _shared_redis_client.close()
                logger.info("✅ Closed execution shared Redis client")
            except Exception as e:
                logger.warning(f"⚠️ Error closing execution shared Redis client: {e}")

            _shared_redis_client = None

        # Reset any Redis clients in existing partitions
        reset_count = 0
        for partition in RedisExecutionSource.__dict__.get("_partitions", {}).values():
            if hasattr(partition, "redis_client"):
                try:
                    if partition.redis_client:
                        partition.redis_client = None
                        reset_count += 1
                except Exception as e:
                    logger.warning(f"⚠️ Error resetting partition Redis client: {e}")

        if reset_count > 0:
            logger.info(f"✅ Reset {reset_count} Redis clients in execution partitions")

        return True
    except Exception as e:
        logger.error(f"❌ Error in reset_shared_redis_client for execution: {e}")
        return False


class RedisExecutionPartition(StatefulSourcePartition):
    def __init__(self, strategy_name=None, stop_event=None):
        """
        Initialize a Redis execution partition.

        Args:
            strategy_name: Optional filter for a specific strategy
            stop_event: Optional threading.Event for stopping the partition
        """
        logger.info(f"🛠️ Initializing RedisExecutionPartition for strategy: {strategy_name or 'all'}")
        self.strategy_name = strategy_name
        self.stop_event = stop_event  # Store the passed stop event
        self.redis_client = get_shared_redis_client()
        self._running = True  # Always start in running state
        self._last_activity = time.time()
        self._lock = Lock()
        self._error_backoff = 1  # Initial backoff in seconds
        self._max_backoff = 30  # Maximum backoff in seconds
        self._initialized = False  # Track initialization state

        self.channels_to_check = []

        if strategy_name:
            strategy_channel = f"{ChannelName.TRADE_OPPORTUNITIES.value}:{strategy_name}"
            self.channels_to_check.append(strategy_channel)
            logger.info(f"Will check only strategy-specific channel: {strategy_channel}")
        else:
            # If no strategy specified, get all configured strategies
            from src.arbirich.config.config import STRATEGIES

            for strat_name in STRATEGIES.keys():
                strategy_channel = f"{ChannelName.TRADE_OPPORTUNITIES.value}:{strat_name}"
                self.channels_to_check.append(strategy_channel)
                logger.info(f"Will check strategy channel: {strategy_channel}")

        # Create explicit subscriptions for all channels
        self.pubsub = self.redis_client.client.pubsub(ignore_subscribe_messages=True)
        for channel in self.channels_to_check:
            self.pubsub.subscribe(channel)
            logger.info(f"Explicitly subscribed to: {channel}")

        self._initialized = True
        logger.info("✅ RedisExecutionPartition initialization complete")

    def next_batch(self) -> list:
        try:
            # Only check for shutdown if we've been initialized
            # This prevents premature shutdown during restart
            if self._initialized and is_system_shutting_down():
                component_id = f"execution:{self.strategy_name or 'all'}"
                if mark_component_notified("execution", component_id):
                    logger.info(f"🛑 Stop event detected for {component_id}")
                    # Clean up resources
                    try:
                        self.pubsub.unsubscribe()
                        self.pubsub.close()
                        logger.info(f"✅ Successfully unsubscribed {component_id} from Redis channels")
                    except Exception as e:
                        logger.error(f"❌ Error unsubscribing {component_id} from Redis channels: {e}")
                self._running = False
                return []

            with self._lock:
                # After a restart, make sure we're in running state if system isn't shutting down
                if not self._running and not is_system_shutting_down():
                    logger.info("🔄 Restoring running state after system restart")
                    self._running = True

                # Check if we should perform periodic health check
                current_time = time.time()
                if current_time - self._last_activity > 30:  # 30 seconds timeout
                    logger.info("🔍 Performing periodic health check on Redis connection")
                    self._last_activity = current_time

                    # Check connection health and recreate if necessary
                    if not self.redis_client.is_healthy():
                        logger.warning("⚠️ Redis connection appears unhealthy, reconnecting")
                        try:
                            self.pubsub.unsubscribe()
                            self.pubsub.close()
                        except Exception as e:
                            logger.warning(f"⚠️ Error closing Redis pubsub: {e}")

                        self.redis_client = get_shared_redis_client()
                        # Recreate subscriptions
                        self.pubsub = self.redis_client.client.pubsub(ignore_subscribe_messages=True)
                        for channel in self.channels_to_check:
                            self.pubsub.subscribe(channel)
                            logger.info(f"🔌 Resubscribed to: {channel}")

                if not self._running:
                    logger.info("🛑 Partition marked as not running, stopping")
                    return []

                # Check for messages from our pubsub client directly
                try:
                    # Check shutdown again just before getting a message
                    if is_system_shutting_down():
                        return []

                    message = self.pubsub.get_message(timeout=0.01)
                    opportunity = None

                    if message and message.get("type") == "message":
                        channel = message.get("channel")
                        if isinstance(channel, bytes):
                            channel = channel.decode("utf-8")

                        data = message.get("data")
                        if isinstance(data, bytes):
                            data = data.decode("utf-8")

                        logger.info(f"📬 Received message from channel: {channel}")
                        try:
                            # Parse the JSON data into a dictionary
                            opportunity_dict = json.loads(data)
                            # Convert dictionary to TradeOpportunity model
                            opportunity = parse_opportunity_message(opportunity_dict)
                        except json.JSONDecodeError:
                            logger.error(f"❌ Error decoding message data: {data}")
                        except Exception as e:
                            logger.error(f"❌ Error creating TradeOpportunity model: {e}")
                            opportunity = None

                    if opportunity:
                        logger.info(f"🎯 Processing opportunity: {opportunity.id} for pair {opportunity.pair}")
                        self._last_activity = time.time()
                        self._error_backoff = 1  # Reset backoff on success
                        return [opportunity]
                    else:
                        # Periodically log that we're waiting for opportunities
                        if current_time - self._last_activity > 120:  # Every 2 minutes
                            logger.info("⏳ Waiting for opportunities...")
                            self._last_activity = current_time

                except (redis.exceptions.ConnectionError, OSError) as e:
                    # Handle specific connection errors including "Bad file descriptor"
                    if "bad file descriptor" in str(e).lower() or "connection closed" in str(e).lower():
                        logger.warning(f"🔌 Redis connection lost: {e}")

                        # Check if system is shutting down before attempting reconnect
                        if is_system_shutting_down():
                            logger.info("🛑 System is shutting down, not attempting Redis reconnection")
                            self._running = False
                            return []

                        # Try to reconnect
                        logger.info("🔄 Attempting to reconnect to Redis...")
                        try:
                            # Clean up old connection
                            try:
                                if self.pubsub:
                                    self.pubsub.close()
                                    self.pubsub = None
                            except Exception as close_error:
                                logger.warning(f"⚠️ Error closing existing Redis connection: {close_error}")

                            # Get a fresh connection
                            time.sleep(0.5)  # Brief pause before reconnecting
                            self.redis_client = get_shared_redis_client()

                            if self.redis_client:
                                self.pubsub = self.redis_client.client.pubsub(ignore_subscribe_messages=True)
                                # Resubscribe to channels
                                for channel in self.channels_to_check:
                                    self.pubsub.subscribe(channel)
                                logger.info("✅ Successfully reconnected to Redis")
                            else:
                                logger.error("❌ Failed to get new Redis client")
                        except Exception as reconnect_error:
                            logger.error(f"❌ Redis reconnection failed: {reconnect_error}")

                        # Use backoff for retries
                        time.sleep(min(self._error_backoff, self._max_backoff))
                        self._error_backoff = min(self._error_backoff * 2, self._max_backoff)
                    else:
                        # For other Redis errors, log and continue
                        logger.error(f"❌ Redis error: {e}")

                    # Return empty for this batch
                    return []

                # No opportunity but no error either
                return []

        except Exception as e:
            logger.error(f"❌ Error in next_batch: {e}")
            # Use exponential backoff for errors
            time.sleep(min(self._error_backoff, self._max_backoff))
            self._error_backoff = min(self._error_backoff * 2, self._max_backoff)
            return []

    def snapshot(self) -> None:
        logger.debug("📸 Snapshot requested for RedisExecutionPartition (returning None)")
        return None


def parse_opportunity_message(message: Dict) -> Optional[TradeOpportunity]:
    """Parse a trade opportunity message from Redis."""
    try:
        # Convert Redis message to TradeOpportunity model
        opportunity = TradeOpportunity(**message)

        # Enhanced validation and debouncing
        if not should_process_opportunity(opportunity):
            return None

        logger.info(f"✅ Parsed opportunity: {opportunity.id}")
        return opportunity
    except Exception as e:
        logger.error(f"❌ Error parsing opportunity: {e}")
        return None


class RedisExecutionSource(FixedPartitionedSource):
    """A source that reads trade opportunities from Redis."""

    def __init__(self, strategy_name=None, stop_event=None):
        """
        Initialize the Redis execution source.

        Args:
            strategy_name: Optional filter for a specific strategy
            stop_event: Optional threading.Event for signaling source to stop
        """
        # Don't convert None to default - keep it as is
        self.strategy_name = strategy_name
        self.stop_event = stop_event
        self.logger = logging.getLogger(__name__)

        # Determine channels to subscribe to
        self.channels = []

        if self.strategy_name is None:
            # For all strategies, get the list of configured strategies
            from src.arbirich.config.config import STRATEGIES

            for strat_name in STRATEGIES.keys():
                channel = f"{ChannelName.TRADE_OPPORTUNITIES.value}:{strat_name}"
                self.channels.append(channel)
            self.logger.info(f"Will subscribe to {len(self.channels)} strategy channels")
        else:
            # For named strategies, use their specific channel
            channel = f"{ChannelName.TRADE_OPPORTUNITIES.value}:{self.strategy_name}"
            self.channels.append(channel)
            self.logger.info(f"Will subscribe to strategy channel: {channel}")

        self.logger.info(f"🔧 RedisExecutionSource initialized with {len(self.channels)} channels")

        # Monitor the stop_event and propagate it to the system-wide shutdown flag
        if self.stop_event:

            def monitor_stop():
                while not self.stop_event.is_set():
                    time.sleep(0.1)
                set_stop_event()  # Signal system-wide shutdown

            monitor_thread = threading.Thread(target=monitor_stop, daemon=True)
            monitor_thread.start()

    def list_parts(self):
        """List available partitions."""
        # Return the actual list of channels we want to subscribe to
        if self.channels:
            logger.info(f"Listing {len(self.channels)} partitions: {self.channels}")
            return self.channels
        else:
            # Fallback in case channels weren't properly initialized
            logger.warning("No channels specified, using default channel")
            return [TRADE_OPPORTUNITIES_CHANNEL]

    def build_part(self, step_id, for_key, _resume_state):
        """
        Create a partition for the given key.

        Args:
            step_id: The step ID
            for_key: The channel to subscribe to
            _resume_state: Resume state if any

        Returns:
            RedisExecutionPartition: A partition for the specific channel
        """
        logger.info(f"🔧 Building execution partition for key: {for_key}")

        # Extract the strategy name from the channel if possible
        channel_strategy = None
        if ":" in for_key:
            parts = for_key.split(":")
            if len(parts) > 1:
                channel_strategy = parts[-1]
                logger.info(f"Extracted strategy name from channel: {channel_strategy}")

        # Use the extracted strategy name or fall back to the main strategy name
        effective_strategy = channel_strategy or self.strategy_name

        # Create a partition with the specific channel to subscribe to
        partition = RedisExecutionPartition(strategy_name=effective_strategy, stop_event=self.stop_event)

        # If we have a specific channel, ensure the partition subscribes to it
        if (
            for_key != "ALL_STRATEGY_CHANNELS"
            and partition.channels_to_check
            and for_key not in partition.channels_to_check
        ):
            logger.info(f"Adding specific channel {for_key} to partition subscription")
            partition.channels_to_check = [for_key]  # Only subscribe to this specific channel

            # Update the pubsub subscription if it exists
            if hasattr(partition, "pubsub") and partition.pubsub:
                try:
                    # Unsubscribe from any existing channels
                    partition.pubsub.unsubscribe()
                    # Subscribe to the specific channel
                    partition.pubsub.subscribe(for_key)
                    logger.info(f"Updated pubsub subscription to channel: {for_key}")
                except Exception as e:
                    logger.error(f"Failed to update pubsub subscription: {e}")

        return partition
