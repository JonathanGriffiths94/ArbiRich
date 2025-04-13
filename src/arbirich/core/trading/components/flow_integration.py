import asyncio
import logging
from typing import Any, Callable, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


async def start_flow(
    flow_name: str, run_func: Callable, stop_func: Callable, config: Dict[str, Any]
) -> Tuple[asyncio.Task, Callable]:
    """
    Start a ByteWax flow as an asyncio task.

    Args:
        flow_name: Name of the flow to start
        run_func: Function to run the flow
        stop_func: Function to stop the flow
        config: Configuration for the flow

    Returns:
        Tuple containing the flow task and stop function
    """
    logger.info(f"Starting {flow_name} flow")

    # Extract relevant configuration for the flow
    flow_config = {}
    if flow_name == "execution":
        flow_config["strategy_name"] = config.get("strategy_name")
        flow_config["debug_mode"] = config.get("debug_mode", False)
    elif flow_name == "detection":
        flow_config["strategy_name"] = config.get("strategy_name")
        flow_config["debug_mode"] = config.get("debug_mode", False)
    elif flow_name == "reporting":
        # For reporting, pass the entire config
        flow_config = config

    # Try to get a flow manager first
    try:
        from src.arbirich.core.trading.flows.flow_manager import FlowManager

        # Check if there's a registered flow manager
        flow_manager = FlowManager.get_manager(flow_name)
        if flow_manager:
            logger.info(f"Using registered flow manager for {flow_name}")
            # Start the flow using the manager instead
            manager_task = asyncio.create_task(flow_manager.run_flow(), name=f"{flow_name}_flow_manager_task")
            return manager_task, flow_manager.stop_flow
    except Exception as e:
        logger.warning(f"Could not use flow manager for {flow_name}: {e}")

    # Fall back to direct function if no manager was found or it failed
    # Start the flow
    task = asyncio.create_task(run_func(**flow_config), name=f"{flow_name}_flow_task")

    logger.info(f"{flow_name} flow started")
    return task, stop_func


async def stop_flow(flow_name: str, task: Optional[asyncio.Task], stop_func: Optional[Callable]) -> bool:
    """
    Stop a ByteWax flow gracefully.

    Args:
        flow_name: Name of the flow to stop
        task: The asyncio task running the flow
        stop_func: Function to stop the flow

    Returns:
        True if flow was stopped successfully, False otherwise
    """
    logger.info(f"Stopping {flow_name} flow")
    success = True

    # Try to get a flow manager first
    try:
        from src.arbirich.core.trading.flows.flow_manager import FlowManager

        # Check if there's a registered flow manager
        flow_manager = FlowManager.get_manager(flow_name)
        if flow_manager:
            logger.info(f"Using registered flow manager to stop {flow_name}")
            # For synchronous stop functions
            if hasattr(flow_manager, "stop_flow") and callable(flow_manager.stop_flow):
                try:
                    flow_manager.stop_flow()
                    logger.info(f"Stopped {flow_name} via flow manager's stop_flow")
                    # If task exists and isn't done, try to clean it up
                    if task and not task.done():
                        task.cancel()
                    return True
                except Exception as e:
                    logger.error(f"Error stopping {flow_name} via flow manager: {e}")

            # For asynchronous stop functions
            if hasattr(flow_manager, "stop_flow_async") and callable(flow_manager.stop_flow_async):
                try:
                    await flow_manager.stop_flow_async()
                    logger.info(f"Stopped {flow_name} via flow manager's stop_flow_async")
                    # If task exists and isn't done, try to clean it up
                    if task and not task.done():
                        task.cancel()
                    return True
                except Exception as e:
                    logger.error(f"Error stopping {flow_name} via flow manager async: {e}")
    except Exception as e:
        logger.warning(f"Could not use flow manager to stop {flow_name}: {e}")

    # Call the stop function
    if stop_func:
        try:
            # Handle both synchronous and asynchronous stop functions
            if asyncio.iscoroutinefunction(stop_func):
                await stop_func()
            else:
                stop_func()
            logger.info(f"{flow_name} flow stopped via stop function")
        except Exception as e:
            logger.error(f"Error stopping {flow_name} flow: {e}")
            success = False

    # Cancel the task if it's still running
    if task and not task.done():
        try:
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                logger.warning(f"{flow_name} flow task cancelled or timed out")
        except Exception as e:
            logger.error(f"Error cancelling {flow_name} flow task: {e}")
            success = False

    return success
