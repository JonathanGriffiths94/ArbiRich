"""Service to maintain Redis channel subscriptions."""

import logging
import threading
import time

from src.arbirich.config import STRATEGIES
from src.arbirich.constants import TRADE_EXECUTIONS_CHANNEL, TRADE_OPPORTUNITIES_CHANNEL
from src.arbirich.services.redis.redis_service import RedisService

logger = logging.getLogger(__name__)


class ChannelMaintenanceService:
    """Service to maintain Redis channel subscriptions."""

    def __init__(self, check_interval=60):
        """Initialize the service."""
        self.check_interval = check_interval
        self.redis = RedisService()
        self.running = False
        self.thread = None
        self.pubsub = None

    def start(self):
        """Start the maintenance service."""
        if self.running:
            logger.warning("Channel maintenance service already running")
            return

        self.running = True
        self.thread = threading.Thread(target=self._maintenance_loop, daemon=True)
        self.thread.start()
        logger.info("Channel maintenance service started")

    def stop(self):
        """Stop the maintenance service."""
        if not self.running:
            logger.warning("Channel maintenance service not running")
            return

        self.running = False
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=2.0)

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

        logger.info("Channel maintenance service stopped")

    def _maintenance_loop(self):
        """Main maintenance loop."""
        self.pubsub = self.redis.client.pubsub()

        # Channels to maintain
        channels = [
            "order_book",
            TRADE_OPPORTUNITIES_CHANNEL,
            TRADE_EXECUTIONS_CHANNEL,
        ]

        # Add strategy-specific channels
        for strategy_name in STRATEGIES.keys():
            channels.append(f"{TRADE_OPPORTUNITIES_CHANNEL}:{strategy_name}")
            channels.append(f"{TRADE_EXECUTIONS_CHANNEL}:{strategy_name}")

        # Subscribe to all channels
        for channel in channels:
            self.pubsub.subscribe(channel)
            logger.info(f"Maintenance service subscribed to {channel}")

        # Maintenance loop
        while self.running:
            try:
                # Check active channels
                active_channels = self.redis.client.pubsub_channels()
                active_channels = [ch.decode("utf-8") if isinstance(ch, bytes) else ch for ch in active_channels]

                # Check for missing channels
                for channel in channels:
                    if channel not in active_channels:
                        logger.warning(f"Channel {channel} missing, resubscribing")
                        self.pubsub.subscribe(channel)

                # Process any messages (though we don't care about the content)
                self.pubsub.get_message(timeout=0.1)

                # Sleep for a while
                time.sleep(self.check_interval)
            except Exception as e:
                logger.error(f"Error in maintenance loop: {e}")
                time.sleep(10)  # Wait a bit longer if there's an error


# Singleton instance
_service_instance = None


def get_channel_maintenance_service():
    """Get the singleton channel maintenance service."""
    global _service_instance
    if _service_instance is None:
        _service_instance = ChannelMaintenanceService()
    return _service_instance
