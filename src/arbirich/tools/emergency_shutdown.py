#!/usr/bin/env python3
"""
Emergency shutdown tool for ArbiRich.
Use when the application doesn't respond to normal shutdown signals.
"""

import logging
import os
import signal
import time
import traceback
from multiprocessing import Process

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


def force_kill_process_group():
    """Force kill the current process group"""
    try:
        pgid = os.getpgrp()
        logger.info(f"Sending SIGTERM to process group {pgid}")
        os.killpg(pgid, signal.SIGTERM)

        # Wait a bit for processes to shutdown gracefully
        time.sleep(2)

        # If we're still running, use SIGKILL
        logger.info(f"Sending SIGKILL to process group {pgid}")
        os.killpg(pgid, signal.SIGKILL)
    except Exception as e:
        logger.error(f"Error killing process group: {e}")
        logger.error(traceback.format_exc())


def emergency_shutdown_monitor(timeout=60):
    """
    Start a watchdog process that will forcibly terminate if the main process
    doesn't exit within the timeout.
    """
    # Get main process PID
    main_pid = os.getpid()

    def watchdog():
        try:
            logger.info(f"Watchdog started for PID {main_pid} with {timeout}s timeout")
            start_time = time.time()

            # Wait for main process to exit
            while time.time() - start_time < timeout:
                try:
                    # Check if main process still exists
                    os.kill(main_pid, 0)  # This doesn't kill, just checks existence
                    time.sleep(1)
                except OSError:
                    # Main process is gone
                    logger.info(f"Main process {main_pid} has exited")
                    return

            # If we get here, timeout was reached
            logger.critical(f"Watchdog timeout ({timeout}s) reached, killing process forcibly")

            # Get the parent process ID and kill its group
            try:
                os.kill(main_pid, signal.SIGKILL)
                logger.info(f"Sent SIGKILL to {main_pid}")
            except OSError:
                pass
        except Exception as e:
            logger.error(f"Error in watchdog: {e}")
            logger.error(traceback.format_exc())

    # Start watchdog as a separate process that won't be affected by main process hangs
    p = Process(target=watchdog)
    p.daemon = True
    p.start()
    return p


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Emergency shutdown tool for ArbiRich")
    parser.add_argument("--timeout", type=int, default=10, help="Timeout in seconds before forcing kill (default: 10)")

    args = parser.parse_args()

    print(f"⚠️ WARNING: About to force kill ArbiRich processes in {args.timeout} seconds...")
    print("Press Ctrl+C to cancel.")

    try:
        time.sleep(args.timeout)
        force_kill_process_group()
        print("✅ Force kill signal sent")
    except KeyboardInterrupt:
        print("❌ Emergency shutdown cancelled")
