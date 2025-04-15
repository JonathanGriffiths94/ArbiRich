# reporting/flow.py

import argparse
import asyncio
import json
import logging
import signal
import sys
import time
import uuid
from typing import Dict

from src.arbirich.constants import TRADE_EXECUTIONS_CHANNEL, TRADE_OPPORTUNITIES_CHANNEL
from src.arbirich.core.trading.flows.flow_manager import FlowManager
from src.arbirich.services.redis.redis_service import get_shared_redis_client

from .message_processor import process_message

logger = logging.getLogger(__name__)

# Global flow manager instance
_flow_manager = None


def get_flow_manager() -> FlowManager:
    """Get the reporting flow manager singleton"""
    global _flow_manager
    if _flow_manager is None:
        _flow_manager = FlowManager(flow_id="reporting")
    return _flow_manager


async def get_flow():
    """Get the reporting flow instance"""
    manager = get_flow_manager()
    # Check if we already have a reporting flow instance
    flow = manager.get_flow_instance("reporting")
    if flow is None:
        # Create a new reporting flow and store it in the manager
        flow = ReportingFlow()
        manager.set_flow_instance("reporting", flow)
    return flow


async def start_flow(config=None):
    """Start the reporting flow"""
    flow = await get_flow()

    if config:
        flow.config = config

    # Initialize and run the flow
    success = await flow.initialize()
    if not success:
        logger.error("❌ Failed to initialize reporting flow")
        return False

    return await flow.run()


def stop_flow():
    """Stop the reporting flow"""
    manager = get_flow_manager()
    flow = manager.get_flow_instance("reporting")
    if flow:
        return asyncio.run(flow.stop())
    return True


class ReportingFlow:
    """
    Reporting flow that processes messages from Redis channels
    and updates the database and metrics.
    """

    def __init__(self, config=None):
        self.config = config or {}
        self.logger = logger
        self.redis = None
        self.pubsub = None
        self.channels = []
        self.active = False
        self.task = None
        self.flow_id = str(uuid.uuid4())[:8]
        self.message_count = 0
        self.error_count = 0
        self.last_activity = time.time()

    async def initialize(self):
        """Initialize the reporting flow"""
        try:
            # Initialize Redis connection
            self.redis = get_shared_redis_client()
            if not self.redis:
                self.logger.error("❌ Failed to get Redis client")
                return False

            # Create pubsub object
            self.pubsub = self.redis.client.pubsub(ignore_subscribe_messages=True)

            # Subscribe only to main channels - removing strategy specific channels
            self.channels = [
                TRADE_OPPORTUNITIES_CHANNEL,  # Main opportunities channel
                TRADE_EXECUTIONS_CHANNEL,  # Main executions channel
            ]

            # Subscribe to channels
            self.pubsub.subscribe(*self.channels)

            self.logger.info(f"🔄 Subscribed to {len(self.channels)} channels: {self.channels}")
            return True
        except Exception as e:
            self.logger.error(f"❌ Error initializing reporting flow: {e}")
            return False

    async def run(self):
        """Run the reporting flow"""
        if not self.pubsub:
            success = await self.initialize()
            if not success:
                self.logger.error("❌ Failed to initialize reporting flow")
                return False

        self.active = True

        # Create a task to run the message loop
        self.task = asyncio.create_task(self._message_loop())

        self.logger.info("🚀 Reporting flow started")
        return True

    async def _message_loop(self):
        """Main message processing loop"""
        try:
            self.logger.info("🔄 Entering message loop")

            while self.active:
                try:
                    # Get message with timeout
                    message = self.pubsub.get_message(timeout=0.1)

                    if message and message.get("type") == "message":
                        # Process message
                        await self._process_message(message)

                    # Brief sleep to prevent CPU spinning
                    await asyncio.sleep(0.01)

                except Exception as e:
                    self.logger.error(f"❌ Error in message loop: {e}")
                    self.error_count += 1
                    await asyncio.sleep(1)  # Back off on errors
        except asyncio.CancelledError:
            self.logger.info("🛑 Message loop cancelled")
            raise
        finally:
            self.active = False
            self.logger.info("🛑 Exiting message loop")

    async def _process_message(self, message):
        """Process a Redis message"""
        try:
            channel = message.get("channel", "")
            if isinstance(channel, bytes):
                channel = channel.decode("utf-8")

            data = message.get("data", "")
            if isinstance(data, bytes):
                data = data.decode("utf-8")

            # Parse data if it's JSON
            try:
                data = json.loads(data)
            except json.JSONDecodeError:
                self.logger.warning(f"⚠️ Invalid JSON in message: {data[:100]}...")
                return

            # Process the message (without assigning to unused result variable)
            await process_message(channel, data)

            # Update stats
            self.message_count += 1
            self.last_activity = time.time()

            # Log message processing
            if self.message_count % 100 == 0:
                self.logger.info(f"📊 Processed {self.message_count} messages")

        except Exception as e:
            self.logger.error(f"❌ Error processing message: {e}")
            self.error_count += 1

    async def stop(self):
        """Stop the reporting flow"""
        try:
            self.active = False

            # Cancel the task
            if self.task:
                self.task.cancel()
                try:
                    await self.task
                except asyncio.CancelledError:
                    pass
                self.task = None

            # Unsubscribe and close pubsub
            if self.pubsub:
                try:
                    self.pubsub.unsubscribe()
                    self.pubsub.close()
                except Exception as e:
                    self.logger.warning(f"⚠️ Error closing pubsub: {e}")
                self.pubsub = None

            self.logger.info("🛑 Reporting flow stopped")
            return True
        except Exception as e:
            self.logger.error(f"❌ Error stopping reporting flow: {e}")
            return False

    def get_stats(self) -> Dict:
        """Get flow statistics"""
        return {
            "id": self.flow_id,
            "active": self.active,
            "message_count": self.message_count,
            "error_count": self.error_count,
            "channels": self.channels,
            "last_activity": self.last_activity,
            "uptime": time.time() - self.last_activity if self.active else 0,
        }


async def main(log_level="INFO"):
    """Main function to run the reporting flow standalone"""
    try:
        # First, ensure environment variables are loaded
        from dotenv import find_dotenv, load_dotenv

        # Load environment variables from .env file
        env_file = find_dotenv()
        if env_file:
            load_dotenv(env_file)
            print(f"🔧 Loaded environment from {env_file}")
        else:
            print("⚠️ No .env file found, using existing environment variables")
    except ImportError:
        print("⚠️ python-dotenv not installed. Make sure environment variables are set manually.")

    # Configure logging
    logging_level = getattr(logging, log_level.upper())
    logging.basicConfig(level=logging_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    logger.info("🚀 Starting reporting flow in standalone mode")

    # Set up signal handling for graceful shutdown
    loop = asyncio.get_event_loop()
    stop_event = asyncio.Event()

    def signal_handler():
        logger.info("🛑 Shutdown signal received, stopping flow...")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, signal_handler)

    # Create a direct instance of ReportingFlow for standalone mode
    standalone_flow = ReportingFlow()

    try:
        # Initialize and run the flow directly
        success = await standalone_flow.initialize()
        if not success:
            logger.error("❌ Failed to initialize reporting flow")
            return 1

        success = await standalone_flow.run()
        if not success:
            logger.error("❌ Failed to start reporting flow")
            return 1

        logger.info("✅ Reporting flow started successfully. Press Ctrl+C to stop.")

        # Wait for stop signal
        await stop_event.wait()
    finally:
        # Stop the flow directly
        logger.info("🛑 Stopping reporting flow...")
        if standalone_flow:
            await standalone_flow.stop()
        logger.info("✅ Reporting flow stopped")

    return 0


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Run the reporting flow as a standalone service")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    args = parser.parse_args()

    # Run the main function
    sys.exit(asyncio.run(main(args.log_level)))
