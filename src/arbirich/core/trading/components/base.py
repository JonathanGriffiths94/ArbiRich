"""Base component class for trading system components."""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict

from src.arbirich.core.config.validator import (
    ArbitrageComponentConfig,
    BaseComponentConfig,
    ConfigValidator,
    ExecutionComponentConfig,
    IngestionComponentConfig,
    ReportingComponentConfig,
)


class Component(ABC):
    """
    Base class for all trading system components.

    Each component follows a consistent lifecycle:
    1. Initialize - Set up resources and connections
    2. Run - Execute main component logic
    3. Cleanup - Release resources and perform cleanup
    """

    def __init__(self, name: str, config: Dict[str, Any]):
        self.name = name
        self.logger = logging.getLogger(f"{__name__}.{name}")
        self.active = False
        self.task = None

        # Validate configuration using Pydantic models
        self.raw_config = config
        self.config = self._validate_config(config)

        self.logger.debug(f"Component {name} initialized with config: {self.config}")

    def _validate_config(self, config: Dict[str, Any]) -> BaseComponentConfig:
        """Validate component configuration using Pydantic models."""
        errors = ConfigValidator.validate_component_config(self.name, config)

        if errors:
            error_msg = f"Invalid configuration for component {self.name}: {errors}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)

        # Convert to appropriate config model based on component type
        try:
            if self.name == "arbitrage":
                return ArbitrageComponentConfig(**config)
            elif self.name == "execution":
                return ExecutionComponentConfig(**config)
            elif self.name == "reporting":
                return ReportingComponentConfig(**config)
            elif self.name == "ingestion":
                return IngestionComponentConfig(**config)
            else:
                # For unknown components, return the raw config
                return BaseComponentConfig(**config)
        except Exception as e:
            self.logger.error(f"Error creating config model for {self.name}: {e}")
            # Fall back to raw config if model creation fails
            return BaseComponentConfig(**config)

    @abstractmethod
    async def initialize(self) -> bool:
        """
        Initialize the component.

        Sets up any resources, connections, or state needed for the component.

        Returns:
            bool: True if initialization succeeded, False otherwise
        """
        pass

    @abstractmethod
    async def run(self) -> None:
        """
        Run the component's main logic.

        This method should contain the core functionality of the component.
        It runs continuously until the component is stopped.
        """
        pass

    @abstractmethod
    async def cleanup(self) -> None:
        """
        Clean up resources used by the component.

        Release connections, close files, and perform any other cleanup tasks.
        """
        pass

    async def start(self) -> bool:
        """
        Start the component's execution.

        Returns:
            bool: True if the component was started successfully, False otherwise
        """
        if self.active:
            self.logger.info(f"Component {self.name} is already active")
            return True

        try:
            # Initialize the component
            if not await self.initialize():
                self.logger.error(f"Component {self.name} failed to initialize")
                return False

            # Set component to active
            self.active = True

            # Create the run task
            self.task = asyncio.create_task(self.run(), name=f"{self.name}_component")

            self.logger.info(f"Component {self.name} started successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error starting component {self.name}: {e}", exc_info=True)
            self.active = False
            return False

    async def stop(self) -> bool:
        """
        Stop the component's execution.

        Returns:
            bool: True if the component was stopped successfully, False otherwise
        """
        if not self.active:
            self.logger.info(f"Component {self.name} is not active")
            return True

        try:
            # Set component to inactive
            self.active = False

            # Cancel the task if it exists
            if self.task and not self.task.done():
                self.task.cancel()
                try:
                    await self.task
                except asyncio.CancelledError:
                    pass

            # Perform cleanup
            await self.cleanup()

            self.logger.info(f"Component {self.name} stopped successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error stopping component {self.name}: {e}", exc_info=True)
            return False

    def get_status(self) -> Dict[str, Any]:
        """
        Get the current status of the component.

        Returns:
            Dict[str, Any]: Status information
        """
        return {
            "name": self.name,
            "active": self.active,
            "task_running": self.task is not None and not self.task.done() if self.task else False,
        }

    def handle_error(self, error: Exception) -> bool:
        """
        Handle an error that occurred during component execution.

        Args:
            error: The exception that occurred

        Returns:
            bool: True if the component should continue, False if it should stop
        """
        self.logger.error(f"Error in component {self.name}: {error}", exc_info=True)

        # By default, continue for most errors
        return True
