"""
Background service to maintain active Redis subscriptions.
This ensures channels always have at least one subscriber.
"""

import logging
import threading
import time
from typing import List, Set

from src.arbirich.config import EXCHANGES, PAIRS, STRATEGIES
from src.arbirich.constants import ORDER_BOOK_CHANNEL, TRADE_OPPORTUNITIES_CHANNEL
from src.arbirich.services.redis_service import RedisService

logger = logging.getLogger(__name__)


class BackgroundSubscriber:
    """
    Maintains persistent Redis subscriptions in a background thread.
    This ensures that all required channels always have at least one subscriber.
    """

    def __init__(self):
        self.redis = None
        self.pubsub = None
        self._stop_event = threading.Event()
        self._thread = None
        self.subscribed_channels: Set[str] = set()

    def start(self):
        """Start the background subscriber in a separate thread."""
        if self._thread and self._thread.is_alive():
            logger.info("Background subscriber already running")
            return

        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True, name="background-redis-subscriber")
        self._thread.start()
        logger.info("Started background Redis subscriber thread")

    def stop(self):
        """Stop the background subscriber."""
        if not self._thread or not self._thread.is_alive():
            logger.info("Background subscriber not running")
            return

        logger.info("Stopping background Redis subscriber...")
        self._stop_event.set()
        self._thread.join(timeout=2.0)

        if self._thread.is_alive():
            logger.warning("Background subscriber thread did not stop gracefully")
        else:
            logger.info("Background subscriber thread stopped")

        self._thread = None

    def _get_channels_to_subscribe(self) -> List[str]:
        """Get the list of channels to subscribe to."""
        channels = [
            ORDER_BOOK_CHANNEL,  # Main order book channel
            TRADE_OPPORTUNITIES_CHANNEL,  # Main opportunities channel
        ]

        # Add strategy-specific trade opportunity channels
        for strategy_name in STRATEGIES.keys():
            channels.append(f"{TRADE_OPPORTUNITIES_CHANNEL}:{strategy_name}")

        # Add exchange-specific channels
        for exchange in EXCHANGES:
            channels.append(f"order_book:{exchange}")

        # Add pair-specific channels
        for base, quote in PAIRS:
            pair = f"{base}-{quote}"
            channels.append(f"order_book:{pair}")

            # Add fully qualified exchange+pair channels
            for exchange in EXCHANGES:
                channels.append(f"order_book:{pair}:{exchange}")

        return channels

    def _run(self):
        """Main subscriber loop."""
        try:
            # Connect to Redis
            self.redis = RedisService()
            self.pubsub = self.redis.client.pubsub(ignore_subscribe_messages=True)

            # Get channels to subscribe to
            channels = self._get_channels_to_subscribe()

            # Subscribe to all channels
            for channel in channels:
                try:
                    self.pubsub.subscribe(channel)
                    self.subscribed_channels.add(channel)
                    logger.info(f"Subscribed to {channel}")
                except Exception as e:
                    logger.error(f"Error subscribing to {channel}: {e}")

            logger.info(f"Background subscriber listening on {len(self.subscribed_channels)} channels")

            # Main loop to keep subscriptions active
            last_check = time.time()
            message_count = 0

            while not self._stop_event.is_set():
                # Get message with timeout (this keeps the subscription alive)
                message = self.pubsub.get_message(timeout=0.1)

                if message and message.get("type") == "message":
                    message_count += 1

                # Periodically verify subscriptions are still active
                current_time = time.time()
                if current_time - last_check > 30:  # Check every 30 seconds
                    last_check = current_time

                    # Get active channels from Redis
                    try:
                        active_channels = self.redis.client.pubsub_channels()
                        active_channels = [
                            ch.decode("utf-8") if isinstance(ch, bytes) else ch for ch in active_channels
                        ]

                        logger.info(f"Active Redis channels: {active_channels}")
                        logger.info(f"Processed {message_count} messages so far")

                        # Resubscribe to any missing channels
                        for channel in channels:
                            if channel not in active_channels:
                                logger.warning(f"Resubscribing to {channel}")
                                try:
                                    self.pubsub.subscribe(channel)
                                except Exception as e:
                                    logger.error(f"Error resubscribing to {channel}: {e}")

                    except Exception as e:
                        logger.error(f"Error checking active channels: {e}")

                        # If there's an error, try to reconnect
                        try:
                            self.redis.reconnect_if_needed()
                            self.pubsub = self.redis.client.pubsub(ignore_subscribe_messages=True)

                            # Resubscribe to all channels
                            for channel in channels:
                                try:
                                    self.pubsub.subscribe(channel)
                                except Exception as sub_error:
                                    logger.error(f"Error resubscribing to {channel}: {sub_error}")
                        except Exception as reconnect_error:
                            logger.error(f"Error reconnecting: {reconnect_error}")

                # Small pause to prevent high CPU usage
                time.sleep(0.01)

        except Exception as e:
            logger.error(f"Error in background subscriber: {e}")
        finally:
            # Clean up
            if self.pubsub:
                try:
                    for channel in self.subscribed_channels:
                        self.pubsub.unsubscribe(channel)
                    self.pubsub.close()
                except Exception as e:
                    logger.error(f"Error unsubscribing: {e}")

            if self.redis:
                try:
                    self.redis.close()
                except Exception as e:
                    logger.error(f"Error closing Redis connection: {e}")

            logger.info("Background subscriber stopped")


# Singleton instance
_subscriber = None


def get_background_subscriber():
    """Get the singleton background subscriber instance."""
    global _subscriber
    if _subscriber is None:
        _subscriber = BackgroundSubscriber()
    return _subscriber
