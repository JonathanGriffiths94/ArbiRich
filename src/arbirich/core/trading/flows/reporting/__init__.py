"""
Reporting flow components for the trading system.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)

# Variable to track the shared Redis client
_shared_redis_client = None


def reset_shared_redis_client() -> bool:
    """
    Reset the shared Redis client for reporting flows.

    Returns:
        bool: True if reset was successful, False otherwise
    """
    global _shared_redis_client

    try:
        if _shared_redis_client is not None:
            try:
                _shared_redis_client.close()
                logger.info("Closed reporting shared Redis client")
            except Exception as e:
                logger.warning(f"Error closing reporting shared Redis client: {e}")

            _shared_redis_client = None

        # Also attempt to reset Redis connection in message processor
        try:
            from src.arbirich.core.trading.flows.reporting.message_processor import reset_redis_connection

            reset_redis_connection()
        except (ImportError, AttributeError) as e:
            logger.debug(f"No message processor Redis connection to reset: {e}")

        return True
    except Exception as e:
        logger.error(f"Error in reset_shared_redis_client for reporting: {e}")
        return False
