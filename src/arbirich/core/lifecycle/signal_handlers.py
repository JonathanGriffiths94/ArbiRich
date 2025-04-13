"""
Signal handlers for managing application lifecycle events.
"""

import logging
import signal
import sys
import threading
import time

logger = logging.getLogger(__name__)

# Flag to track if we're already handling SIGINT
_handling_sigint = False
_sigint_lock = threading.Lock()

# Track number of SIGINT signals received
_sigint_count = 0
_last_sigint_time = 0


def setup_signal_handlers():
    """Set up signal handlers for the application."""
    # Set up SIGINT (Ctrl+C) handler
    signal.signal(signal.SIGINT, handle_sigint)

    # Set up SIGTERM handler
    signal.signal(signal.SIGTERM, handle_sigterm)

    logger.info("Signal handlers installed")


def handle_sigint(sig, frame):
    """
    Handle SIGINT (Ctrl+C) with progressive escalation.

    First press: Normal shutdown
    Second press within 5 seconds: Emergency shutdown
    Third press within 5 seconds: Force exit
    """
    global _handling_sigint, _sigint_count, _last_sigint_time

    current_time = time.time()

    # Check if this is a rapid sequence of SIGINTs
    if current_time - _last_sigint_time < 5:
        _sigint_count += 1
    else:
        _sigint_count = 1

    _last_sigint_time = current_time

    # Only proceed if we're not already handling a SIGINT
    with _sigint_lock:
        if _handling_sigint:
            # If we're already handling it, increment the counter and potentially escalate
            logger.warning(f"Received additional SIGINT (count: {_sigint_count})")

            if _sigint_count >= 3:
                # Third press - force exit immediately
                logger.critical("Received 3rd SIGINT in rapid succession - forcing immediate exit")
                sys.exit(1)
            elif _sigint_count >= 2:
                # Second press - use emergency shutdown
                logger.critical("Received 2nd SIGINT in rapid succession - initiating emergency shutdown")
                try:
                    from src.arbirich.core.lifecycle.emergency_shutdown import force_shutdown

                    force_shutdown(timeout=5.0)  # Short timeout for impatient users
                except Exception as e:
                    logger.error(f"Error during force shutdown: {e}")
                    sys.exit(1)

            return

        _handling_sigint = True

    try:
        logger.info("Received SIGINT (Ctrl+C) - starting normal shutdown")

        # Initiate application shutdown
        try:
            from src.arbirich.core.lifecycle.app_lifecyle import shutdown_application

            shutdown_application(reason="user_interrupt")
        except Exception as e:
            logger.error(f"Error during application shutdown: {e}")
            sys.exit(1)

    except Exception as e:
        logger.error(f"Error handling SIGINT: {e}")
        sys.exit(1)
    finally:
        with _sigint_lock:
            _handling_sigint = False


def handle_sigterm(sig, frame):
    """Handle SIGTERM signal."""
    logger.info("Received SIGTERM - starting graceful shutdown")

    try:
        # Initiate application shutdown
        from src.arbirich.core.lifecycle.app_lifecyle import shutdown_application

        shutdown_application(reason="sigterm")
    except Exception as e:
        logger.error(f"Error during application shutdown: {e}")
        sys.exit(1)
