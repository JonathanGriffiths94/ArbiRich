"""
Unified flow manager for all trading system components.
Provides consistent management of dataflows with special handling for Bytewax.
"""

import asyncio
import concurrent.futures
import logging
import threading
import time
from typing import Any, Callable, Dict, Optional, Set

# Update import based on documentation
from bytewax.run import cli_main

from src.arbirich.core.state.system_state import is_system_shutting_down
from src.arbirich.utils.thread_helpers import try_join_thread

logger = logging.getLogger(__name__)

# Global registry of flow managers
_flow_managers: Dict[str, "FlowManager"] = {}
_registry_lock = threading.Lock()

# Guards against reusing the same executor
_global_executor_lock = threading.Lock()
_executors_in_use: Set[concurrent.futures.ThreadPoolExecutor] = set()


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
                    logger.info(f"Stopping flow {flow_manager.flow_id} during registry clear")
                    flow_manager.stop_flow()
            except Exception as e:
                logger.error(f"Error stopping flow {flow_manager.flow_id}: {e}")

        # Then clear the registry
        old_count = len(_flow_managers)
        _flow_managers.clear()

        # Also clear executors
        with _global_executor_lock:
            executor_count = len(_executors_in_use)
            _executors_in_use.clear()

        logger.info(f"Flow registry cleared: removed {old_count} flows and {executor_count} executors")


class FlowManager:
    """Base class for managing dataflows."""

    @classmethod
    def get_or_create(cls, flow_id: str) -> "FlowManager":
        """Get an existing flow manager or create a new one."""
        with _registry_lock:
            if flow_id not in _flow_managers:
                _flow_managers[flow_id] = cls(flow_id)
            return _flow_managers[flow_id]

    @classmethod
    def get_manager(cls, flow_id: str) -> Optional["FlowManager"]:
        """Get an existing flow manager if it exists."""
        with _registry_lock:
            return _flow_managers.get(flow_id)

    @classmethod
    def stop_all_flows(cls):
        """Stop all managed flows."""
        with _registry_lock:
            for flow_id, manager in _flow_managers.items():
                try:
                    if manager.is_running():
                        logger.info(f"Stopping flow manager: {flow_id}")
                        manager.stop_flow()
                except Exception as e:
                    logger.error(f"Error stopping flow {flow_id}: {e}")

    def __init__(self, flow_id: str):
        """Initialize the flow manager."""
        self.flow_id = flow_id
        self.flow = None
        self.build_flow_fn: Callable[[], Any] = None
        self.running = False
        self.runner_thread = None
        self.stop_event = threading.Event()
        self.logger = logging.getLogger(f"arbirich.flows.{flow_id}")

    def build_flow(self) -> Any:
        """Build the dataflow."""
        if self.build_flow_fn:
            return self.build_flow_fn()
        else:
            self.logger.error("No flow builder function set")
            raise NotImplementedError("build_flow function must be set before running flow")

    def set_flow_builder(self, builder_fn: Callable[[], Any]) -> None:
        """Set the flow builder function."""
        self.build_flow_fn = builder_fn

    async def run_flow(self) -> bool:
        """Run the dataflow in a separate thread."""
        if self.running:
            self.logger.warning(f"Flow {self.flow_id} is already running")
            # Force reset flow state if thread is dead but running flag is still set
            if self.runner_thread and not self.runner_thread.is_alive():
                self.logger.warning(f"Flow {self.flow_id} marked as running but thread is dead - resetting state")
                self.running = False
                self.stop_event.clear()
                self.runner_thread = None
            else:
                return True

        try:
            # Reset stop event
            self.stop_event.clear()

            # Check if another manager with the same name is running
            with _registry_lock:
                existing = _flow_managers.get(self.flow_id)
                if existing and existing is not self:
                    self.logger.warning(f"Stopping existing {self.flow_id} before creating new instance")
                    await existing.stop_flow_async()

                # Register as the active flow manager
                _flow_managers[self.flow_id] = self

            # Build the flow if needed
            if not self.flow:
                self.flow = self.build_flow()
                if not self.flow:
                    self.logger.error(f"Failed to build {self.flow_id} flow")
                    return False

            # Start the flow in a separate thread
            def run_flow_thread():
                try:
                    self.logger.info(f"Starting flow {self.flow_id}")
                    # Generic implementation - subclasses should override this
                    self.running = True

                    # Wait for stop event in generic implementation
                    while not self.stop_event.is_set() and not is_system_shutting_down():
                        time.sleep(0.1)

                except Exception as e:
                    self.logger.error(f"Error running flow {self.flow_id}: {e}", exc_info=True)
                finally:
                    self.running = False
                    self.logger.info(f"Flow {self.flow_id} stopped")

            self.runner_thread = threading.Thread(target=run_flow_thread, name=f"flow-{self.flow_id}")
            self.runner_thread.daemon = True
            self.runner_thread.start()

            # Wait briefly to ensure flow starts
            await asyncio.sleep(0.5)
            return self.running

        except Exception as e:
            self.logger.error(f"Error starting flow {self.flow_id}: {e}", exc_info=True)
            return False

    def is_running(self) -> bool:
        """Check if the flow is running."""
        return self.running and (self.runner_thread is not None and self.runner_thread.is_alive())

    def stop_flow(self):
        """Signal the flow to stop synchronously with enhanced error handling"""
        flow_name = self.flow_id
        logger.info(f"Stop signal sent to {flow_name} flow")

        try:
            from src.arbirich.core.state.system_state import mark_system_shutdown

            # Make sure we set the shutdown flag
            mark_system_shutdown(True)

            # Set the stop event to signal all threads
            self.stop_event.set()

            # Use a shorter timeout for waiting and log our intention
            logger.info(f"Waiting for {flow_name} flow thread to exit (max 2.0s)...")

            # If there's a thread, attempt to join it with timeout
            if hasattr(self, "runner_thread") and self.runner_thread and self.runner_thread.is_alive():
                import threading

                current_thread_id = threading.get_ident()
                if self.runner_thread.ident != current_thread_id:
                    self.runner_thread.join(timeout=2.0)
                else:
                    logger.warning(f"Cannot join current thread for {flow_name}, skipping thread join")

            # Forcefully shutdown the executor with more aggressive approach
            if hasattr(self, "_thread_pool") and self._thread_pool:
                try:
                    self._thread_pool.shutdown(wait=False)
                    logger.info(f"Shutdown thread pool executor for {flow_name}")
                except Exception as e:
                    logger.error(f"Error shutting down thread pool: {e}")

                    # Try to forcibly clear the thread pool
                    try:
                        self._thread_pool = None
                        logger.info("Forcibly cleared thread pool reference")
                    except Exception:
                        pass

            # If it's a known bytewax flow, try more aggressive cleanup
            if "detection" in flow_name or "ingestion" in flow_name:
                try:
                    # Try to force reset Redis connections
                    from src.arbirich.services.redis.redis_service import reset_redis_pool

                    reset_redis_pool()
                    logger.info("Force reset all Redis pools")
                except Exception as e:
                    logger.error(f"Error resetting Redis pools: {e}")

            return True
        except Exception as e:
            logger.error(f"Error stopping flow {flow_name}: {e}")
            # Still return True to indicate stop was attempted
            return True

    async def stop_flow_async(self) -> bool:
        """Asynchronously stop the flow."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.stop_flow)


class BytewaxFlowManager(FlowManager):
    """Manager for Bytewax dataflows with enhanced capabilities."""

    def __init__(self, flow_id: str):
        super().__init__(flow_id)
        self._thread_pool = None
        self._stop_timeout = 3.0  # Seconds to wait for thread to exit

    def run_flow_with_direct_api(self):
        """
        Run the flow directly using bytewax API.
        This is used for direct CLI invocation.
        """
        if self.running:
            self.logger.warning(f"BytewaxFlow {self.flow_id} is already running")
            return

        # Reset stop event
        self.stop_event.clear()

        try:
            # Build the flow
            flow = self.build_flow()
            if not flow:
                self.logger.error(f"Failed to build {self.flow_id} flow")
                return

            self.flow = flow
            self.running = True

            # Set up a monitor thread to check for stop event
            monitor_thread = threading.Thread(
                target=self._monitor_stop,
                name=f"{self.flow_id}_monitor",
                daemon=True,
            )
            monitor_thread.start()

            # Set runner thread to current thread
            self.runner_thread = threading.current_thread()

            # Use thread pool for better management
            with _global_executor_lock:
                # Create a new thread pool executor
                self._thread_pool = concurrent.futures.ThreadPoolExecutor(
                    max_workers=2,
                    thread_name_prefix=f"{self.flow_id}_worker",
                )
                _executors_in_use.add(self._thread_pool)

            self.logger.info(f"Running BytewaxFlow {self.flow_id} directly")

            # Run the flow using cli_main
            cli_main(
                self.flow,
                workers_per_process=1,  # Use a single worker per process for simplicity
                process_id=0,  # Set process ID to 0 for single-process execution
            )

        except Exception as e:
            self.logger.error(f"Error running BytewaxFlow {self.flow_id}: {e}", exc_info=True)
        finally:
            self.running = False
            self._shutdown_thread_pool()
            self.logger.info(f"BytewaxFlow {self.flow_id} direct execution completed")

    async def run_flow(self) -> bool:
        """Run a Bytewax flow in a separate thread."""
        if self.running:
            self.logger.warning(f"Bytewax flow {self.flow_id} is already running")
            # Force reset flow state if thread is dead but running flag is still set
            if self.runner_thread and not self.runner_thread.is_alive():
                self.logger.warning(f"Flow {self.flow_id} marked as running but thread is dead - resetting state")
                self.running = False
                self.stop_event.clear()
                self.runner_thread = None
            else:
                return True

        # Create a new stop event
        self.stop_event.clear()

        # Check if another manager with the same name is running
        with _registry_lock:
            existing = _flow_managers.get(self.flow_id)
            if existing and existing is not self:
                self.logger.warning(f"Stopping existing {self.flow_id} before creating new instance")
                await existing.stop_flow_async()

            # Register as the active flow manager
            _flow_managers[self.flow_id] = self

        try:
            # Build the flow if needed
            flow = self.build_flow()
            if not flow:
                self.logger.error(f"Failed to build {self.flow_id} flow")
                return False

            self.flow = flow

            # Start the flow in a separate thread
            def run_bytewax_flow_thread():
                try:
                    with _global_executor_lock:
                        # Create a new thread pool executor
                        self._thread_pool = concurrent.futures.ThreadPoolExecutor(
                            max_workers=2,
                            thread_name_prefix=f"{self.flow_id}_worker",
                        )
                        _executors_in_use.add(self._thread_pool)

                    # Start a monitor thread to check for stop events
                    monitor_thread = threading.Thread(
                        target=self._monitor_stop,
                        name=f"{self.flow_id}_monitor",
                        daemon=True,
                    )
                    monitor_thread.start()

                    # Run the flow using cli_main
                    self.logger.info(f"Running {self.flow_id} flow")
                    self.running = True

                    # Use cli_main with appropriate parameters
                    cli_main(
                        self.flow,
                        workers_per_process=1,  # Use a single worker per process for simplicity
                        process_id=0,  # Set process ID to 0 for single-process execution
                    )
                except Exception as e:
                    self.logger.error(f"Error running Bytewax flow {self.flow_id}: {e}", exc_info=True)
                finally:
                    self.running = False
                    self.logger.info(f"Bytewax flow {self.flow_id} shutdown completed")

            self.runner_thread = threading.Thread(target=run_bytewax_flow_thread, name=f"bytewax-{self.flow_id}")
            self.runner_thread.daemon = True
            self.runner_thread.start()

            # Wait briefly to ensure flow starts
            await asyncio.sleep(0.5)
            return self.running

        except Exception as e:
            self.logger.error(f"Error starting Bytewax flow {self.flow_id}: {e}", exc_info=True)
            return False

    def _monitor_stop(self):
        """Monitor for stop event and initiate shutdown when triggered."""
        try:
            # Set initial check interval
            check_interval = 0.1  # Start with more frequent checks

            # Wait for the stop event or check system shutdown
            while not self.stop_event.is_set() and not is_system_shutting_down():
                time.sleep(check_interval)  # Short sleep to avoid CPU spinning

                # Gradually increase check interval to reduce CPU usage over time
                # but keep it responsive for quick shutdown
                if check_interval < 0.5:  # Don't go above 0.5 seconds
                    check_interval = min(check_interval * 1.05, 0.5)

            # Log why we're shutting down
            if self.stop_event.is_set():
                self.logger.info("Stop event detected, terminating flow")
            elif is_system_shutting_down():
                self.logger.info("System shutdown detected, terminating flow")
                # Make sure our internal stop event is also set
                self.stop_event.set()

            # Terminate the thread pool - this will cause run_main to exit
            self._shutdown_thread_pool()

            # Don't rely on imported functions during shutdown - use a direct approach
            self.logger.info(f"Flow {self.flow_id} shutting down - not waiting for component notification")

        except Exception as e:
            self.logger.error(f"Error in monitor thread: {e}")
        finally:
            self.logger.info(f"{self.flow_id} flow monitor exited")

    def _shutdown_thread_pool(self):
        """Shutdown the thread pool executor with improvements."""
        if self._thread_pool:
            try:
                # First attempt a normal shutdown with a timeout
                self.logger.info(f"Initiating thread pool shutdown for {self.flow_id}")
                self._thread_pool.shutdown(wait=True, timeout=2.0)
                self.logger.info(f"Thread pool shutdown completed for {self.flow_id}")
            except (TypeError, AttributeError):
                # Older Python versions don't support timeout parameter
                try:
                    self._thread_pool.shutdown(wait=False)
                    self.logger.info(f"Thread pool shutdown initiated for {self.flow_id} (no wait)")
                except Exception as e:
                    self.logger.error(f"Error during thread pool shutdown: {e}")
            except Exception as e:
                self.logger.error(f"Error shutting down thread pool: {e}")
            finally:
                with _global_executor_lock:
                    if self._thread_pool in _executors_in_use:
                        _executors_in_use.remove(self._thread_pool)
                self._thread_pool = None
                self.logger.info(f"Thread pool cleared for {self.flow_id}")

    def stop_flow(self) -> bool:
        """Synchronously stop the flow with more aggressive handling."""
        # Set the stop event to signal shutdown
        self.stop_event.set()
        self.logger.info(f"Stop signal sent to {self.flow_id} flow")

        # Mark system as shutting down to ensure other components respect it
        try:
            from src.arbirich.core.state.system_state import mark_system_shutdown

            mark_system_shutdown(True)
        except ImportError:
            self.logger.warning("Could not import system_state module")

        # Wait for the thread to exit, but with a shorter timeout
        if self.runner_thread and self.runner_thread.is_alive():
            self.logger.info(f"Waiting for {self.flow_id} flow thread to exit (max 1.5s)...")
            thread_exited = try_join_thread(self.runner_thread, 1.5)  # Shorter timeout

            if not thread_exited:
                # More aggressive cleanup if the thread doesn't exit quickly
                self.logger.warning(f"{self.flow_id} flow thread didn't exit cleanly, using aggressive cleanup...")

                # Force cleanup of resources even if thread didn't exit
                self._shutdown_thread_pool()

                # Attempt to identify and kill associated processes more aggressively
                self._force_kill_processes()

                # Flow-specific cleanup for common flows - update paths
                try:
                    # Flow-specific Redis cleanup
                    if "detection" in self.flow_id:
                        from src.arbirich.core.trading.flows.bytewax_flows.detection.detection_source import (
                            reset_shared_redis_client,
                        )

                        reset_shared_redis_client()
                        self.logger.info("Force closed detection Redis client")
                    elif "execution" in self.flow_id:
                        from src.arbirich.core.trading.flows.bytewax_flows.execution.execution_source import (
                            reset_shared_redis_client,
                        )

                        reset_shared_redis_client()
                        self.logger.info("Force closed execution Redis client")
                    elif "ingestion" in self.flow_id:
                        from src.arbirich.core.trading.flows.bytewax_flows.ingestion.ingestion_sink import (
                            reset_shared_redis_client,
                        )

                        reset_shared_redis_client()
                        self.logger.info("Force closed ingestion Redis client")

                    # Add aggressive cleanup for all Redis clients
                    try:
                        from src.arbirich.services.redis.redis_service import reset_redis_pool

                        reset_redis_pool()
                        self.logger.info("Force reset all Redis pools")
                    except Exception as e:
                        self.logger.error(f"Error resetting Redis pools: {e}")

                except Exception as e:
                    self.logger.error(f"Error in cleanup operations: {e}")

        self.running = False
        self.flow = None  # Clear flow to force rebuild on next run
        self.logger.info(f"{self.flow_id} flow stop completed")
        return True

    def _force_kill_processes(self):
        """Attempt to forcefully terminate processes related to this flow."""
        try:
            import psutil

            # Get the current process
            current_process = psutil.Process()

            # Get all child processes
            children = current_process.children(recursive=True)

            # Filter those that might be related to this flow
            flow_id_lower = self.flow_id.lower()
            for child in children:
                try:
                    # Check if process name or command line contains flow_id
                    cmdline = " ".join(child.cmdline()).lower()
                    if flow_id_lower in cmdline or "bytewax" in cmdline:
                        self.logger.warning(f"Force killing process {child.pid} with cmdline: {cmdline[:50]}...")

                        # Try SIGTERM first
                        child.terminate()

                        # Give it a short time to terminate gracefully
                        gone, still_alive = psutil.wait_procs([child], timeout=0.5)

                        # If still alive, use SIGKILL
                        if child in still_alive:
                            self.logger.warning(f"Process {child.pid} still alive after SIGTERM, using SIGKILL")
                            child.kill()
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    continue
                except Exception as e:
                    self.logger.error(f"Error killing process: {e}")
        except Exception as e:
            self.logger.error(f"Error in force_kill_processes: {e}")
