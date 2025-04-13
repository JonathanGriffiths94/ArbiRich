"""
Logging configuration for ArbiRich.
"""

import logging
import os
import sys
from datetime import datetime

# Default log format
DEFAULT_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

# Colored logging on terminals
COLORED_LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


def configure_logging(debug_mode=False, log_file=None):
    """
    Configure logging for the application.

    Args:
        debug_mode: Whether to enable debug logging
        log_file: Path to log file, if None, logs to stdout only
    """
    # Get the root logger
    root_logger = logging.getLogger()

    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Set the log level based on debug mode
    log_level = logging.DEBUG if debug_mode else logging.INFO
    root_logger.setLevel(log_level)

    # Create console handler with appropriate formatter
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    # We'll use the default formatter - could add colors later if desired
    formatter = logging.Formatter(DEFAULT_LOG_FORMAT)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Add file handler if log file is specified
    if log_file:
        # Create logs directory if it doesn't exist
        logs_dir = os.path.dirname(log_file)
        if logs_dir and not os.path.exists(logs_dir):
            os.makedirs(logs_dir)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

    # Set specific module log levels
    logging.getLogger("src.arbirich.core.trading").setLevel(log_level)
    logging.getLogger("src.arbirich.core.trading.flows").setLevel(log_level)
    logging.getLogger("src.arbirich.core.trading.strategy").setLevel(log_level)

    # Always keep Bytewax logging at INFO or above unless we're in debug mode
    bytewax_level = log_level if debug_mode else logging.INFO
    logging.getLogger("bytewax").setLevel(bytewax_level)

    # Log configuration applied
    root_logger.info(f"Logging configured with {'DEBUG' if debug_mode else 'INFO'} level")
    if log_file:
        root_logger.info(f"Logging to file: {log_file}")


def create_timestamped_log_file(base_dir="logs", prefix="arbirich"):
    """Create a timestamped log file path"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{prefix}_{timestamp}.log"
    return os.path.join(base_dir, filename)
