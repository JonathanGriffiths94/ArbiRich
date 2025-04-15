"""
Trading system management functions for ArbiRich.

This module provides high-level functions to control the trading system.
"""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


async def activate_shutdown(
    reason: str = "user_requested", emergency: bool = False, timeout: Optional[int] = None
) -> bool:
    """
    Activate the trading system shutdown process.

    Args:
        reason: The reason for shutdown (for logging)
        emergency: Whether this is an emergency shutdown
        timeout: Maximum time to wait for shutdown (None for default)

    Returns:
        bool: True if shutdown was activated successfully
    """
    logger.warning(f"TRADING SYSTEM SHUTDOWN ACTIVATED: {reason} (emergency={emergency})")

    try:
        # First set the system shutdown flag
        from src.arbirich.core.state.system_state import mark_system_shutdown

        mark_system_shutdown(True)

        if emergency:
            # Use emergency shutdown for faster termination
            logger.critical("Initiating EMERGENCY shutdown sequence")
            from src.arbirich.core.lifecycle.emergency_shutdown import force_shutdown

            force_shutdown(timeout=timeout or 10.0)
            return True
        else:
            # Use phased shutdown for cleaner termination
            logger.info("Initiating phased shutdown sequence")
            from src.arbirich.core.lifecycle.shutdown_manager import execute_phased_shutdown

            # Execute phased shutdown asynchronously
            result = await execute_phased_shutdown(emergency_timeout=timeout or 30)
            return result

    except Exception as e:
        logger.error(f"Error activating trading system shutdown: {e}", exc_info=True)

        # Even if there's an error, try to set the shutdown flag
        try:
            from src.arbirich.core.state.system_state import mark_system_shutdown

            mark_system_shutdown(True)
        except Exception:
            pass

        return False


def activate_shutdown_sync(
    reason: str = "user_requested", emergency: bool = False, timeout: Optional[int] = None
) -> bool:
    """
    Synchronous version of activate_shutdown for use in non-async contexts.

    Args:
        reason: The reason for shutdown (for logging)
        emergency: Whether this is an emergency shutdown
        timeout: Maximum time to wait for shutdown (None for default)

    Returns:
        bool: True if shutdown was activated successfully
    """
    # In synchronous contexts, emergency shutdown is safer
    if not emergency:
        logger.info("Using emergency=True for synchronous shutdown to avoid event loop issues")
        emergency = True

    try:
        # Set the system shutdown flag
        from src.arbirich.core.state.system_state import mark_system_shutdown

        mark_system_shutdown(True)

        # For synchronous contexts, use emergency shutdown
        from src.arbirich.core.lifecycle.emergency_shutdown import force_shutdown

        result = force_shutdown(timeout=timeout or 10.0)
        return result

    except Exception as e:
        logger.error(f"Error in synchronous trading system shutdown: {e}", exc_info=True)
        return False
