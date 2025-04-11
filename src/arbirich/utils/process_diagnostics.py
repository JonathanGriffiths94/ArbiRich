import logging
import sys
import threading
import time
import traceback  # Add missing import here

import psutil

logger = logging.getLogger(__name__)

# Add deduplication support using a simple time-based approach
_last_diagnostics_run = 0
_min_interval = 3.0  # seconds between allowed runs


def log_thread_stack_traces():
    """
    Log stack traces from all threads to help diagnose hanging issues.
    """
    frames = sys._current_frames()

    logger.info(f"--- Stack traces for {len(frames)} threads ---")

    for thread_id, frame in frames.items():
        thread_name = None
        for thread in threading.enumerate():
            if thread.ident == thread_id:
                thread_name = thread.name
                break

        logger.info(f"Thread {thread_id} ({thread_name}):")

        for filename, lineno, name, line in traceback.extract_stack(frame):
            logger.info(f'  File "{filename}", line {lineno}, in {name}')
            if line:
                logger.info(f"    {line.strip()}")

    logger.info("--- End of stack traces ---")


def log_process_diagnostics(deduplicate=False):
    """
    Log diagnostic information about the current process and its children.

    Args:
        deduplicate: If True, will only run if no diagnostics have run in the last few seconds
    """
    global _last_diagnostics_run

    # Check if we should deduplicate
    if deduplicate:
        current_time = time.time()
        if current_time - _last_diagnostics_run < _min_interval:
            logger.debug("Skipping duplicate diagnostics run")
            return

    # Update last run time
    _last_diagnostics_run = time.time()

    try:
        current_process = psutil.Process()

        # Log basic process info
        logger.info("--- Process Diagnostics ---")
        logger.info(f"Current process: PID={current_process.pid}, name={current_process.name()}")
        logger.info(f"Status: {current_process.status()}")
        logger.info(f"Created: {time.ctime(current_process.create_time())}")

        cpu_percent = current_process.cpu_percent(interval=0.1)
        memory_info = current_process.memory_info()

        logger.info(f"CPU usage: {cpu_percent}%")
        logger.info(f"Memory usage: {memory_info.rss / (1024 * 1024):.2f} MB")

        # Log open files
        try:
            open_files = current_process.open_files()
            logger.info(f"Open files ({len(open_files)}):")
            for file in open_files[:10]:  # Show only first 10 to avoid excessive logging
                logger.info(f"  {file.path}")
            if len(open_files) > 10:
                logger.info(f"  ... and {len(open_files) - 10} more")
        except psutil.AccessDenied:
            logger.info("Cannot access open files (permission denied)")

        # Log connections
        try:
            connections = current_process.connections()
            logger.info(f"Network connections ({len(connections)}):")
            for conn in connections[:10]:
                logger.info(f"  {conn.laddr} -> {conn.raddr if conn.raddr else 'None'} ({conn.status})")
            if len(connections) > 10:
                logger.info(f"  ... and {len(connections) - 10} more")
        except psutil.AccessDenied:
            logger.info("Cannot access connections (permission denied)")

        # Log child processes
        children = current_process.children(recursive=True)
        logger.info(f"Child processes ({len(children)}):")
        for child in children:
            try:
                logger.info(f"  PID={child.pid}, name={child.name()}, status={child.status()}")
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                logger.info(f"  PID={child.pid} (process no longer exists or access denied)")

        # Log threads
        logger.info(f"Python threads ({threading.active_count()}):")
        for thread in threading.enumerate():
            logger.info(f"  {thread.name} (daemon: {thread.daemon})")

        logger.info("--- End Process Diagnostics ---")

    except Exception as e:
        logger.error(f"Error gathering process diagnostics: {e}")


# Automatically run diagnostic info when this module is imported
if __name__ != "__main__":
    try:
        log_process_diagnostics()
    except Exception as e:
        logger.error(f"Error running process diagnostics: {e}")
