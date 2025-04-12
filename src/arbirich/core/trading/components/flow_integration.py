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

    # Call the stop function
    if stop_func:
        try:
            await stop_func()
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
