"""
Thread management utilities for graceful termination of threads.
"""

import asyncio
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

logger = logging.getLogger(__name__)


def try_join_thread(thread: threading.Thread, timeout: float = 3.0) -> bool:
    """
    Try to join a thread with a timeout, returning success status.

    Args:
        thread: The thread to join
        timeout: Maximum time to wait in seconds

    Returns:
        True if thread terminated, False if timeout occurred
    """
    if not thread.is_alive():
        return True

    logger.debug(f"Waiting for thread {thread.name} to exit (timeout: {timeout}s)")
    thread.join(timeout)
    success = not thread.is_alive()

    if success:
        logger.debug(f"Thread {thread.name} exited successfully")
    else:
        logger.warning(f"Thread {thread.name} did not exit within {timeout}s")

    return success


def terminate_thread_pool(executor: ThreadPoolExecutor, timeout: float = 2.0) -> bool:
    """
    Gracefully terminate a thread pool executor.

    Args:
        executor: The thread pool to shut down
        timeout: Maximum time to wait in seconds

    Returns:
        True if all threads terminated, False otherwise
    """
    if executor is None:
        return True

    try:
        # Attempt graceful shutdown first
        executor.shutdown(wait=False)

        # Wait a short time
        time.sleep(timeout)

        # Check if threads are still running
        return not executor._threads  # Check if any threads remain
    except Exception as e:
        logger.error(f"Error shutting down thread pool: {e}")
        return False


async def wait_for_tasks(tasks: List[asyncio.Task], timeout: float = 2.0) -> List[asyncio.Task]:
    """
    Wait for asyncio tasks to complete with a timeout.

    Args:
        tasks: List of tasks to wait for
        timeout: Maximum time to wait in seconds

    Returns:
        List of tasks that did not complete
    """
    if not tasks:
        return []

    try:
        # Wait for tasks with timeout
        done, pending = await asyncio.wait(tasks, timeout=timeout)

        # Cancel any pending tasks
        for task in pending:
            task.cancel()

        return list(pending)
    except Exception as e:
        logger.error(f"Error waiting for asyncio tasks: {e}")
        return tasks


def cancel_all_tasks(loop: Optional[asyncio.AbstractEventLoop] = None) -> int:
    """
    Cancel all running tasks in the current loop or specified loop.

    Args:
        loop: Event loop to cancel tasks in, or None for current loop

    Returns:
        Number of tasks canceled
    """
    try:
        if loop is None:
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                # No running loop
                return 0

        # Get all tasks
        tasks = asyncio.all_tasks(loop=loop)
        current_task = asyncio.current_task(loop=loop)

        # Don't cancel the current task
        if current_task is not None:
            tasks.discard(current_task)

        # Cancel all other tasks
        for task in tasks:
            task.cancel()

        return len(tasks)
    except Exception as e:
        logger.error(f"Error canceling asyncio tasks: {e}")
        return 0
