import logging
import os
import platform
import sys
from datetime import datetime

logger = logging.getLogger(__name__)


def display_banner(extra_info=None, log_only=False, console_only=False):
    """Display a beautiful ArbiRich banner in the console."""
    # Get the current time
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Get app version
    app_version = os.getenv("APP_VERSION", "dev")

    # Create the banner
    banner = f"""
 $$$$$$\\            $$\\       $$\\ $$$$$$$\\  $$\\           $$\\       
$$  __$$\\           $$ |      \\__|$$  __$$\\ \\__|          $$ |      
$$ /  $$ | $$$$$$\\  $$$$$$$\\  $$\\ $$ |  $$ |$$\\  $$$$$$$\\ $$$$$$$\\  
$$$$$$$$ |$$  __$$\\ $$  __$$\\ $$ |$$$$$$$  |$$ |$$  _____|$$  __$$\\ 
$$  __$$ |$$ |  \\__|$$ |  $$ |$$ |$$  __$$ |$$ |$$ /      $$ |  $$ |
$$ |  $$ |$$ |      $$ |  $$ |$$ |$$ |  $$ |$$ |$$ |      $$ |  $$ |
$$ |  $$ |$$ |      $$$$$$$  |$$ |$$ |  $$ |$$ |\\$$$$$$$\\ $$ |  $$ |
\\__|  \\__|\\__|      \\_______/ \\__|\\__|  \\__|\\__| \\_______|\\___|  \\__|

======================================================================
Cryptocurrency Arbitrage Platform | Version: {app_version} | {current_time}
======================================================================"""

    # Add extra info if provided
    if extra_info:
        banner += f"\n{extra_info}"

    # Display the banner
    if not log_only:
        print(banner)

    # Log the banner
    if not console_only:
        logger.info(f"\n{banner}")

    return banner
