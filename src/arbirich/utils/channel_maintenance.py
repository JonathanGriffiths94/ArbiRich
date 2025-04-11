import logging
import threading
import time

from arbirich.core.state.system_state import is_system_shutting_down, mark_component_notified
from src.arbirich.services.redis.redis_channel_manager import get_channel_manager
from src.arbirich.services.redis.redis_service import get_shared_redis_client

logger = logging.getLogger(__name__)


class ChannelMaintenanceService:
    """Service to maintain Redis channel subscriptions."""

    def __init__(self, check_interval=60):
        """Initialize the service."""
        self.check_interval = check_interval
        self.redis = get_shared_redis_client()
        self.running = False
        self.thread = None
        self.pubsub = None

    def reset(self):
        """Reset service for system restart"""
        self.stop()  # Stop any running maintenance
        self.redis = get_shared_redis_client()
        logger.info("Channel maintenance service reset")

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
        try:
            self.pubsub = self.redis.client.pubsub()

            # Mark component as active
            mark_component_notified("channel_maintenance", "main")

            # Get channels to maintain using consistent format
            channels = self._get_channels_to_maintain()

            # Subscribe to all channels
            for channel in channels:
                self.pubsub.subscribe(channel)
                logger.info(f"Maintenance service subscribed to {channel}")

            # Maintenance loop with shutdown check
            while self.running and not is_system_shutting_down():
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

                    # Shorter sleep intervals with multiple checks for faster shutdown response
                    for _ in range(min(10, self.check_interval)):
                        if not self.running or is_system_shutting_down():
                            break
                        time.sleep(1)
                except Exception as e:
                    if not self.running or is_system_shutting_down():
                        break
                    logger.error(f"Error in maintenance loop: {e}")
                    time.sleep(5)  # Shorter retry interval
        except Exception as e:
            if not is_system_shutting_down():
                logger.error(f"Fatal error in maintenance loop: {e}")

        logger.info("Channel maintenance loop exited")

    def _get_channels_to_maintain(self):
        """Get list of channels to maintain with consistent formatting"""
        # Use the channel manager to get a consistent list of channels
        channel_manager = get_channel_manager()
        return channel_manager.get_all_system_channels()


# Singleton instance
_service_instance = None


def get_channel_maintenance_service():
    """Get the singleton channel maintenance service."""
    global _service_instance
    if _service_instance is None:
        _service_instance = ChannelMaintenanceService()
    return _service_instance


def reset_channel_maintenance_service():
    """Reset the singleton channel maintenance service."""
    global _service_instance
    if _service_instance:
        _service_instance.reset()
        _service_instance = None
    logger.info("Channel maintenance service singleton reset")
