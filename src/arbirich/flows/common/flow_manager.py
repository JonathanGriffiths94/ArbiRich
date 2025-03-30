"""
Flow manager for Bytewax dataflows.
"""

import asyncio
import concurrent.futures
import logging
import threading
import time
from typing import Callable, Dict, Optional

from bytewax.dataflow import Dataflow

# Update import based on documentation
from bytewax.run import cli_main

from src.arbirich.core.system_state import is_system_shutting_down
from src.arbirich.utils.thread_helpers import terminate_thread_pool, try_join_thread

logger = logging.getLogger(__name__)

# Global registry of flow managers
_flow_managers: Dict[str, "BytewaxFlowManager"] = {}
_registry_lock = threading.Lock()

# Guards against reusing the same executor
_global_executor_lock = threading.Lock()
_executors_in_use = set()


def clear_flow_registry():
    """
    Clear all registered flow managers.

    This is crucial for proper system restart.
    """
    with _registry_lock:
        # First stop any running flows
        flows_to_stop = list(_flow_managers.values())
        for flow_manager in flows_to_stop:
            try:
                if flow_manager.is_running():
                    logger.info(f"Stopping flow {flow_manager.name} during registry clear")
                    flow_manager.stop_flow()
            except Exception as e:
                logger.error(f"Error stopping flow {flow_manager.name}: {e}")

        # Then clear the registry
        old_count = len(_flow_managers)
        _flow_managers.clear()

        # Also clear executors
        with _global_executor_lock:
            executor_count = len(_executors_in_use)
            _executors_in_use.clear()

        logger.info(f"Flow registry cleared: removed {old_count} flows and {executor_count} executors")


class BytewaxFlowManager:
    """
    Manager for Bytewax dataflow execution with graceful shutdown support.
    """

    @classmethod
    def get_or_create(cls, flow_name: str) -> "BytewaxFlowManager":
        """Get an existing flow manager or create a new one."""
        with _registry_lock:
            if flow_name not in _flow_managers:
                _flow_managers[flow_name] = BytewaxFlowManager(flow_name)
            return _flow_managers[flow_name]

    @classmethod
    def get_manager(cls, flow_name: str) -> Optional["BytewaxFlowManager"]:
        """Get an existing flow manager if it exists."""
        with _registry_lock:
            return _flow_managers.get(flow_name)

    @classmethod
    def stop_all_flows(cls):
        """Stop all managed flows."""
        with _registry_lock:
            for name, manager in _flow_managers.items():
                try:
                    if manager.is_running():
                        logger.info(f"Stopping flow manager: {name}")
                        manager.stop_flow()
                except Exception as e:
                    logger.error(f"Error stopping flow {name}: {e}")

    def __init__(self, name: str):
        self.name = name
        self.stop_event = threading.Event()
        self.thread = None
        self.build_flow: Callable[[], Dataflow] = None
        self._flow_running = False
        self._thread_pool = None
        self._stop_timeout = 3.0  # Seconds to wait for thread to exit

        # Better logging with flow name prefix
        self.logger = logging.getLogger(f"{__name__}.{name}")

    async def run_flow(self) -> bool:
        """
        Run the flow in a separate thread and return immediately.
        """
        if self._flow_running:
            self.logger.info(f"Flow {self.name} is already running")
            # Force reset flow state if we're trying to start a flow that thinks it's running but not actually active
            if self.thread and not self.thread.is_alive():
                self.logger.warning(f"Flow {self.name} marked as running but thread is dead - resetting state")
                self._flow_running = False
                self.stop_event.clear()
                self.thread = None
            else:
                return True

        # Create a new stop event if an old one exists
        self.stop_event.clear()

        # Check if another manager with the same name is running
        with _registry_lock:
            existing = _flow_managers.get(self.name)
            if existing and existing is not self:
                self.logger.warning(f"Stopping existing {self.name} before creating new instance")
                await existing.stop_flow_async()

            # Register as the active flow manager
            _flow_managers[self.name] = self

        # Create a new thread
        self.thread = threading.Thread(
            target=self._run_flow_thread,
            name=f"{self.name}_flow",
            daemon=True,  # Make daemon to ensure it doesn't block process exit
        )

        # Start the thread
        self.thread.start()
        self._flow_running = True
        self.logger.info(f"Started {self.name} flow thread")

        return True

    def _run_flow_thread(self):
        """Thread function to run the flow."""
        try:
            flow = self.build_flow()
            if not flow:
                self.logger.error(f"Failed to build {self.name} flow")
                return

            with _global_executor_lock:
                # Create a new thread pool executor
                self._thread_pool = concurrent.futures.ThreadPoolExecutor(
                    max_workers=2,
                    thread_name_prefix=f"{self.name}_worker",
                )
                _executors_in_use.add(self._thread_pool)

            # Start a monitor thread to check for stop events
            monitor_thread = threading.Thread(
                target=self._monitor_stop,
                name=f"{self.name}_monitor",
                daemon=True,
            )
            monitor_thread.start()

            # Run the flow using cli_main instead of run_main
            self.logger.info(f"Running {self.name} flow")

            # Use cli_main with appropriate parameters
            cli_main(
                flow,
                workers_per_process=1,  # Use a single worker per process for simplicity
                process_id=0,  # Set process ID to 0 for single-process execution
            )

        except Exception as e:
            self.logger.error(f"Error in {self.name} flow: {e}", exc_info=True)
        finally:
            self._flow_running = False
            self.logger.info(f"{self.name} flow shutdown initiated")

    def _monitor_stop(self):
        """Monitor for stop event and initiate shutdown when triggered."""
        try:
            # Wait for the stop event or check system shutdown
            while not self.stop_event.is_set() and not is_system_shutting_down():
                time.sleep(0.1)  # Short sleep to avoid CPU spinning

            if self.stop_event.is_set():
                self.logger.info("Stop condition detected, terminating flow")
            elif is_system_shutting_down():
                self.logger.info("System shutdown detected, terminating flow")
                # Make sure our internal stop event is also set
                self.stop_event.set()

            # Terminate the thread pool - this will cause run_main to exit
            self._shutdown_thread_pool()

        except Exception as e:
            self.logger.error(f"Error in monitor thread: {e}")
        finally:
            self.logger.info(f"{self.name} flow monitor exited")

    def _shutdown_thread_pool(self):
        """Shutdown the thread pool executor."""
        if self._thread_pool:
            try:
                # Use our helper function for better thread pool termination
                terminate_thread_pool(self._thread_pool)
                self.logger.info(f"Shutdown thread pool executor for {self.name}")
            except Exception as e:
                self.logger.error(f"Error shutting down thread pool: {e}")
            finally:
                with _global_executor_lock:
                    if self._thread_pool in _executors_in_use:
                        _executors_in_use.remove(self._thread_pool)
                self._thread_pool = None

    def stop_flow(self) -> bool:
        """Synchronously stop the flow."""
        # Set the stop event to signal shutdown
        self.stop_event.set()
        self.logger.info(f"Stop signal sent to {self.name} flow")

        # Mark system as shutting down to ensure other components respect it
        from src.arbirich.core.system_state import mark_system_shutdown

        mark_system_shutdown(True)

        # Wait for the thread to exit
        if self.thread and self.thread.is_alive():
            self.logger.info(f"Waiting for {self.name} flow thread to exit (max {self._stop_timeout}s)...")
            thread_exited = try_join_thread(self.thread, self._stop_timeout)

            if not thread_exited:
                self.logger.warning(f"{self.name} flow thread didn't exit cleanly, forcing cleanup...")
                # Force cleanup of resources even if thread didn't exit
                self._shutdown_thread_pool()

                # Close Redis connection
                try:
                    # First try module-specific cleanup for common flows
                    if self.name == "reporting":
                        from src.arbirich.flows.reporting.reporting_source import (
                            reset_shared_redis_client,
                            terminate_all_partitions,
                        )

                        terminate_all_partitions()
                        reset_shared_redis_client()
                        self.logger.info("Force closed reporting partitions and Redis client")
                    elif self.name == "arbitrage":
                        from src.arbirich.flows.arbitrage.arbitrage_source import reset_shared_redis_client

                        reset_shared_redis_client()
                        self.logger.info("Force closed arbitrage Redis client")
                    elif self.name == "execution":
                        from src.arbirich.flows.execution.execution_source import reset_shared_redis_client

                        reset_shared_redis_client()
                        self.logger.info("Force closed execution Redis client")
                    elif self.name == "ingestion":
                        from src.arbirich.flows.ingestion.ingestion_sink import reset_shared_redis_client

                        reset_shared_redis_client()
                        self.logger.info("Force closed ingestion Redis client")
                except Exception as e:
                    self.logger.error(f"Error closing Redis connection: {e}")

        self._flow_running = False
        self.logger.info(f"{self.name} flow stop completed")
        return True

    async def stop_flow_async(self) -> bool:
        """Asynchronously stop the flow."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.stop_flow)

    def is_running(self) -> bool:
        """Check if the flow is running."""
        # Check both our internal state and the thread state
        return self._flow_running and (self.thread and self.thread.is_alive())
