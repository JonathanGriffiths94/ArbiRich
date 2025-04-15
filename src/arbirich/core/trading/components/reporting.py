import asyncio
import logging
from typing import Any, Dict

from src.arbirich.core.trading.components.base import Component
from src.arbirich.services.reporting.reporting_service import ReportingService

logger = logging.getLogger(__name__)


class ReportingComponent(Component):
    """
    Reporting component for data persistence and monitoring.
    Uses the centralized ReportingService for core functionality.
    """

    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self.update_interval = config.get("update_interval", 60.0)
        self.stop_event = asyncio.Event()

        # Create the reporting service with our config
        self.reporting_service = ReportingService(config)

    async def initialize(self) -> bool:
        """Initialize reporting configurations"""
        try:
            # Initialize the reporting service
            success = self.reporting_service.initialize()
            if not success:
                logger.error("Failed to initialize reporting service")
                return False

            # Set up error handling
            self.reporting_service.set_error_handler(self.handle_service_error)

            # Get channels to monitor
            channels = self.reporting_service.get_or_build_channel_list()

            # Subscribe to channels
            await self.reporting_service.subscribe_to_channels(channels)

            logger.info(f"Initialized reporting component with {len(channels)} channels")
            return True

        except Exception as e:
            logger.error(f"Error initializing reporting component: {e}", exc_info=True)
            return False

    def handle_service_error(self, exception: Exception, task_name: str):
        """Handle errors from the reporting service"""
        # Use our component's error handler
        self.handle_error(exception)

    async def run(self) -> None:
        """Run reporting tasks using the reporting service"""
        logger.info("Starting reporting component")
        self.active = True

        try:
            # Start the reporting service
            await self.reporting_service.start()

            # Keep the component alive until stopped
            while self.active and not self.stop_event.is_set():
                await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            logger.info("Reporting component cancelled")
            raise
        except Exception as e:
            logger.error(f"Unexpected error in reporting component: {e}", exc_info=True)
            if not self.handle_error(e):
                self.active = False
        finally:
            await self.cleanup()

    async def cleanup(self) -> None:
        """Clean up resources used by the reporting component."""
        try:
            logger.info("Cleaning up reporting component")

            # Stop the reporting service
            await self.reporting_service.stop()

        except Exception as e:
            logger.error(f"Error cleaning up reporting component: {e}")

    def handle_error(self, error: Exception) -> bool:
        """Handle component errors, return True if error was handled"""
        # Override the base class method
        logger.error(f"Error in reporting component: {error}", exc_info=True)
        # Always attempt to continue despite errors
        return True

    async def stop(self) -> bool:
        """
        Signal component to stop.
        Override the base class method but call it to ensure proper cleanup.

        Returns:
            bool: True if the component was stopped successfully
        """
        logger.info("Stopping reporting component")
        self.stop_event.set()

        # Set system shutdown flag if available
        try:
            from src.arbirich.core.state.system_state import mark_system_shutdown

            mark_system_shutdown(True)
            logger.info("System shutdown flag set")
        except Exception as e:
            logger.error(f"Error setting system shutdown flag: {e}")

        # Use the base class stop method for standard cleanup
        return await super().stop()
