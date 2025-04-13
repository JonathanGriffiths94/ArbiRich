import json
import logging
import threading
import time

from arbirich.core.state.system_state import is_system_shutting_down
from src.arbirich.config.config import EXCHANGES, PAIRS, STRATEGIES
from src.arbirich.constants import ORDER_BOOK_CHANNEL, TRADE_EXECUTIONS_CHANNEL, TRADE_OPPORTUNITIES_CHANNEL
from src.arbirich.services.redis.redis_service import get_shared_redis_client

logger = logging.getLogger(__name__)


class BackgroundSubscriber:
    """
    Maintains Redis subscriptions in a background thread.
    This ensures channels remain active even if other flows disconnect.
    """

    def __init__(self):
        self.redis = get_shared_redis_client()
        self.pubsub = None
        self.thread = None
        self.running = False
        self.channels = set()

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
            logger.info("Waiting for background subscriber thread to terminate...")
            self.thread.join(timeout=3.0)
            if self.thread.is_alive():
                logger.warning("Background subscriber thread did not terminate within timeout")
            else:
                logger.info("Background subscriber thread terminated successfully")

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

    def reset():
        """Reset the background subscriber for system restart"""
        global _background_subscriber
        if _background_subscriber:
            _background_subscriber.stop()
            _background_subscriber = None
        logger.info("Background subscriber reset")

    def _subscribe_loop(self):
        """Main subscriber loop that keeps subscriptions active"""
        try:
            # Initialize pubsub
            self.pubsub = self.redis.client.pubsub(ignore_subscribe_messages=True)

            # Subscribe to all required channels
            self._subscribe_to_channels()

            # Keep subscriptions alive
            while self.running and not is_system_shutting_down():
                try:
                    # Process any pubsub messages (keep subscriptions alive)
                    message = self.pubsub.get_message(timeout=0.1)
                    if message:
                        logger.debug(f"Background subscriber received message: {message}")

                    # Sleep to avoid high CPU usage
                    time.sleep(0.1)
                except Exception as e:
                    if is_system_shutting_down():
                        break
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
        self._subscribe_channel(ORDER_BOOK_CHANNEL)  # Changed from "order_book" to use the constant
        self._subscribe_channel(TRADE_OPPORTUNITIES_CHANNEL)
        self._subscribe_channel(TRADE_EXECUTIONS_CHANNEL)

        # Subscribe to strategy-specific channels
        for strategy_name in STRATEGIES.keys():
            self._subscribe_channel(f"{TRADE_OPPORTUNITIES_CHANNEL}:{strategy_name}")
            self._subscribe_channel(f"{TRADE_EXECUTIONS_CHANNEL}:{strategy_name}")
            # Make sure this channel is initialized even without messages
            logger.info(f"Subscribed to trade execution channel for {strategy_name}")

            # Send heartbeat to strategy channels to ensure they're active
            self.redis.publish(
                f"{TRADE_EXECUTIONS_CHANNEL}:{strategy_name}",
                json.dumps({"type": "heartbeat", "timestamp": time.time()}),
            )

        # Subscribe to exchange-specific order book channels

        # Subscribe to exchange-pair combinations with consistent format
        for exchange in EXCHANGES:
            # First subscribe to exchange-level channel
            self._subscribe_channel(f"{ORDER_BOOK_CHANNEL}:{exchange}")
            logger.info(f"Subscribed to order book channel for {exchange}")

            # Send heartbeat to exchange channel
            self.redis.publish(
                f"{ORDER_BOOK_CHANNEL}:{exchange}", json.dumps({"type": "heartbeat", "timestamp": time.time()})
            )

            # Then subscribe to specific pair channels
            for base, quote in PAIRS:
                symbol = f"{base}-{quote}"
                channel = f"{ORDER_BOOK_CHANNEL}:{exchange}:{symbol}"
                self._subscribe_channel(channel)
                logger.info(f"Subscribed to order book channel for {exchange}:{symbol}")

                # Send heartbeat to pair-specific channel to ensure it's active
                self.redis.publish(
                    channel,
                    json.dumps({"type": "heartbeat", "timestamp": time.time(), "exchange": exchange, "symbol": symbol}),
                )

        logger.info(f"Background subscriber listening on {len(self.channels)} channels")

        # Log channels for debugging
        logger.info(f"Subscribed channels: {sorted(list(self.channels))}")

    def _subscribe_channel(self, channel):
        """Subscribe to a Redis channel with error handling"""
        try:
            # Use existing method, but add more robust error handling
            if channel not in self.channels:
                self.pubsub.subscribe(channel)
                self.channels.add(channel)
                logger.info(f"Subscribed to channel: {channel}")

            # Also publish a test message to ensure channel appears in Redis
            if channel.startswith(TRADE_EXECUTIONS_CHANNEL):
                # Send a test heartbeat message to initialize execution channels
                self.redis.publish(channel, json.dumps({"type": "heartbeat", "timestamp": time.time()}))
        except Exception as e:
            logger.error(f"Error subscribing to channel {channel}: {e}")


# Singleton instance
_background_subscriber = None


def get_background_subscriber():
    """Get or create the background subscriber singleton"""
    global _background_subscriber
    if _background_subscriber is None:
        _background_subscriber = BackgroundSubscriber()
    return _background_subscriber


def reset():
    """
    Reset the background subscriber service.
    """
    try:
        # Add logic to clean up or reset any resources used by the background subscriber
        logger.info("Resetting background subscriber service")
        # Example: Close any open connections or threads
        # background_subscriber_instance.close()
    except Exception as e:
        logger.error(f"Error resetting background subscriber service: {e}")
