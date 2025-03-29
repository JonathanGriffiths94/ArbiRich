import logging
import os
from datetime import datetime

logger = logging.getLogger(__name__)

# Money green ANSI color codes
MONEY_GREEN = "\033[38;2;133;187;101m"  # RGB color for money green (#85BB65)
BRIGHT_GREEN = "\033[92m"  # Fallback bright green for terminals with limited color support
RESET_COLOR = "\033[0m"  # Reset to default color


def display_banner(extra_info=None, log_only=False, console_only=False, use_color=True, separator_style="double-line"):
    """
    Display a beautiful ArbiRich banner in the console.

    Args:
        extra_info: Additional information to display below the banner
        log_only: Only log the banner, don't print to console
        console_only: Only print to console, don't log
        use_color: Use ANSI color codes for the banner
        separator_style: Style for separator line ("double-line", "single-line",
                        "equals", "hash", "stars", or custom string)
    """
    # Get the current time
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Get app version
    app_version = os.getenv("APP_VERSION", "dev")

    # Choose color codes based on terminal support
    start_color = ""
    end_color = ""

    # Only apply colors if requested and not logging-only
    if use_color and not log_only:
        # Try to detect if terminal supports RGB colors
        if "COLORTERM" in os.environ and os.environ["COLORTERM"] in ("truecolor", "24bit"):
            start_color = MONEY_GREEN
        else:
            start_color = BRIGHT_GREEN
        end_color = RESET_COLOR

    # Set separator based on style parameter - make them longer (80 chars)
    separator_map = {
        "double-line": "════════════════════════════════════════════════════════════════════════════════",
        "single-line": "────────────────────────────────────────────────────────────────────────────────",
        "equals": "================================================================================",
        "hash": "################################################################################",
        "stars": "********************************************************************************",
    }

    separator = separator_map.get(separator_style, separator_style)

    # Apply money green color to separator if colors are enabled
    colored_separator = f"{start_color}{separator}{end_color}" if use_color and not log_only else separator

    # Info line with centered version and timestamp
    info_text = f"Cryptocurrency Arbitrage Platform | Version: {app_version} | {current_time}"

    # Create the banner with colors
    banner = f"""{start_color}
 $$$$$$\\            $$\\       $$\\ $$$$$$$\\  $$\\           $$\\       
$$  __$$\\           $$ |      \\__|$$  __$$\\ \\__|          $$ |      
$$ /  $$ | $$$$$$\\  $$$$$$$\\  $$\\ $$ |  $$ |$$\\  $$$$$$$\\ $$$$$$$\\  
$$$$$$$$ |$$  __$$\\ $$  __$$\\ $$ |$$$$$$$  |$$ |$$  _____|$$  __$$\\ 
$$  __$$ |$$ |  \\__|$$ |  $$ |$$ |$$  __$$ |$$ |$$ /      $$ |  $$ |
$$ |  $$ |$$ |      $$ |  $$ |$$ |$$ |  $$ |$$ |$$ |      $$ |  $$ |
$$ |  $$ |$$ |      $$$$$$$  |$$ |$$ |  $$ |$$ |\\$$$$$$$\\ $$ |  $$ |
\\__|  \\__|\\__|      \\_______/ \\__|\\__|  \\__|\\__| \\_______|\\__|  \\__|{end_color}

{colored_separator}
{start_color}{info_text}{end_color}
{colored_separator}"""

    # Add extra info if provided
    if extra_info:
        banner += f"\n{extra_info}"

    # Create a version without ANSI codes for logging
    log_banner = banner
    if use_color and not log_only:
        # Strip ANSI codes for logging
        log_banner = banner.replace(start_color, "").replace(end_color, "")

    # Display the banner
    if not log_only:
        print(banner)

    # Log the banner
    if not console_only:
        logger.info(f"\n{log_banner}")

    return banner


def display_money(amount, currency="USD", positive_color=True):
    """Display a monetary amount in green (positive) or red (negative)."""
    is_positive = amount >= 0
    color_code = MONEY_GREEN if is_positive and positive_color else "\033[91m"  # Red for negative

    formatted = f"{currency} {abs(amount):,.2f}"
    if not is_positive:
        formatted = f"-{formatted}"

    return f"{color_code}{formatted}{RESET_COLOR}"
