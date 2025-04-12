"""Base component class for all trading system components."""

import asyncio
import logging
from typing import Any, Dict


class Component:
    """Base class for all trading system components."""

    def __init__(self, name: str, config: Dict[str, Any]):
        """
        Initialize the component.

        Args:
            name: Name of the component
            config: Component configuration
        """
        self.name = name
        self.config = config
        self.active = False
        self.task = None
        self.component_type = self._get_component_type()
        self.logger = logging.getLogger(f"src.arbirich.core.trading.components.{name}")

    def _get_component_type(self) -> str:
        """
        Get the component type based on class name.

        Returns:
            str: Component type (detection, execution, reporting, ingestion)
        """
        class_name = self.__class__.__name__.lower()
        if "detection" in class_name:
            return "detection"
        elif "execution" in class_name:
            return "execution"
        elif "reporting" in class_name:
            return "reporting"
        elif "ingestion" in class_name:
            return "ingestion"
        else:
            return "unknown"

    async def initialize(self) -> bool:
        """
        Initialize component resources.

        Returns:
            bool: True if successfully initialized, False otherwise
        """
        try:
            self.logger.info(f"Initializing {self.name} component")
            return True
        except Exception as e:
            self.logger.error(f"Error initializing {self.name} component: {e}")
            return False

    async def start(self) -> bool:
        """
        Start the component.

        Returns:
            bool: True if successfully started, False otherwise
        """
        try:
            if not await self.initialize():
                self.logger.error(f"Failed to initialize {self.name} component")
                return False

            self.logger.info(f"Starting {self.name} component")
            self.active = True

            # Start background task
            if self.task is None or self.task.done():
                self.task = asyncio.create_task(self.run())

            return True
        except Exception as e:
            self.logger.error(f"Error starting {self.name} component: {e}")
            return False

    async def run(self) -> None:
        """
        Main component logic, should be overridden by subclasses.
        """
        try:
            self.logger.info(f"Running {self.name} component")
            while self.active:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            self.logger.info(f"{self.name} component task cancelled")
            raise
        except Exception as e:
            self.logger.error(f"Error in {self.name} component: {e}")
            if not self.handle_error(e):
                self.active = False

    async def stop(self) -> bool:
        """
        Stop the component.

        Returns:
            bool: True if successfully stopped, False otherwise
        """
        try:
            self.logger.info(f"Stopping {self.name} component")
            self.active = False

            # Notify system state about shutdown
            try:
                from src.arbirich.core.state.system_state import mark_component_notified

                mark_component_notified(self.component_type, self.name)
            except ImportError:
                self.logger.warning("Could not import system_state module")

            # Cancel task if running
            if self.task and not self.task.done():
                self.task.cancel()
                try:
                    await self.task
                except asyncio.CancelledError:
                    pass

            # Clean up resources
            await self.cleanup()

            return True
        except Exception as e:
            self.logger.error(f"Error stopping {self.name} component: {e}")
            return False

    async def cleanup(self) -> None:
        """
        Clean up resources, should be overridden by subclasses.
        """
        self.logger.info(f"Cleaning up {self.name} component")

    def get_status(self) -> Dict[str, Any]:
        """
        Get the component status.

        Returns:
            Dict containing component status information
        """
        return {
            "active": self.active,
            "name": self.name,
            "has_task": self.task is not None and not self.task.done() if self.task else False,
        }

    def handle_error(self, error: Exception) -> bool:
        """
        Handle component errors.

        Args:
            error: The exception that occurred

        Returns:
            bool: True if error was handled and component should continue, False to stop
        """
        self.logger.error(f"Error in {self.name} component: {error}", exc_info=True)
        # By default, we continue despite errors
        return True
