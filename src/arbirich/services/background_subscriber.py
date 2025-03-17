"""
Background service to maintain active Redis subscriptions.
This ensures channels always have at least one subscriber.
"""

import logging
import threading
import time

from src.arbirich.config import EXCHANGES, PAIRS, STRATEGIES
from src.arbirich.constants import TRADE_EXECUTIONS_CHANNEL, TRADE_OPPORTUNITIES_CHANNEL
from src.arbirich.services.redis.redis_service import RedisService

logger = logging.getLogger(__name__)


class BackgroundSubscriber:
    """
    Maintains Redis subscriptions in a background thread.
    This ensures channels remain active even if other flows disconnect.
    """

    def __init__(self):
        self.redis = RedisService()
        self.pubsub = None
        self.thread = None
        self.running = False
        self.channels = []

    def start(self):
        """Start the background subscriber thread"""
        if self.thread and self.thread.is_alive():
            logger.warning("Background subscriber thread already running")
            return

        self.running = True
        self.thread = threading.Thread(target=self._subscribe_loop, daemon=True)
        self.thread.start()
        logger.info("Started background Redis subscriber thread")

    def stop(self):
        """Stop the background subscriber thread"""
        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=3.0)

        if self.pubsub:
            try:
                self.pubsub.unsubscribe()
                self.pubsub.close()
            except Exception as e:
                logger.error(f"Error closing pubsub: {e}")

        try:
            self.redis.close()
        except Exception as e:
            logger.error(f"Error closing Redis connection: {e}")

        logger.info("Background subscriber stopped")

    def _subscribe_loop(self):
        """Main subscriber loop that keeps subscriptions active"""
        try:
            # Initialize pubsub
            self.pubsub = self.redis.client.pubsub(ignore_subscribe_messages=True)

            # Subscribe to all required channels
            self._subscribe_to_channels()

            # Keep subscriptions alive
            while self.running:
                try:
                    # Process any pubsub messages (keep subscriptions alive)
                    message = self.pubsub.get_message(timeout=0.1)
                    if message:
                        logger.debug(f"Background subscriber received message: {message}")

                    # Sleep to avoid high CPU usage
                    time.sleep(0.1)
                except Exception as e:
                    logger.error(f"Error in background subscriber: {e}")
                    time.sleep(1)  # Sleep and retry
        except Exception as e:
            logger.error(f"Error in background subscriber: {e}")
        finally:
            logger.info("Background subscriber thread exiting")

            if self.pubsub:
                try:
                    self.pubsub.unsubscribe()
                    self.pubsub.close()
                except Exception as e:
                    logger.error(f"Error closing pubsub: {e}")

            self.redis.close()

    def _subscribe_to_channels(self):
        """Subscribe to all required channels"""
        # Subscribe to main channels
        self._subscribe_channel("order_book")
        self._subscribe_channel(TRADE_OPPORTUNITIES_CHANNEL)
        self._subscribe_channel(TRADE_EXECUTIONS_CHANNEL)

        # Subscribe to strategy-specific channels
        for strategy_name in STRATEGIES.keys():
            self._subscribe_channel(f"{TRADE_OPPORTUNITIES_CHANNEL}:{strategy_name}")
            self._subscribe_channel(f"{TRADE_EXECUTIONS_CHANNEL}:{strategy_name}")

        # Subscribe to exchange-specific channels
        for exchange in EXCHANGES:
            self._subscribe_channel(f"order_book:{exchange}")

        # Subscribe to pair-specific channels
        for base, quote in PAIRS:
            symbol = f"{base}-{quote}"
            self._subscribe_channel(f"order_book:{symbol}")

            # Subscribe to pair-exchange combinations
            for exchange in EXCHANGES:
                self._subscribe_channel(f"order_book:{symbol}:{exchange}")

        logger.info(f"Background subscriber listening on {len(self.channels)} channels")

    def _subscribe_channel(self, channel):
        """Subscribe to a specific channel and log it"""
        try:
            self.pubsub.subscribe(channel)
            self.channels.append(channel)
            logger.info(f"Subscribed to {channel}")
        except Exception as e:
            logger.error(f"Error subscribing to {channel}: {e}")


# Singleton instance
_background_subscriber = None


def get_background_subscriber():
    """Get or create the background subscriber singleton"""
    global _background_subscriber
    if _background_subscriber is None:
        _background_subscriber = BackgroundSubscriber()
    return _background_subscriber
