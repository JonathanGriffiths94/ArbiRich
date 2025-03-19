import logging
from datetime import datetime
from typing import Optional

logger = logging.getLogger(__name__)


def get_version() -> str:
    """Get version from pyproject.toml or return development version."""
    try:
        import toml

        with open("pyproject.toml", "r") as f:
            pyproject = toml.load(f)
            return pyproject.get("tool", {}).get("poetry", {}).get("version", "dev")
    except Exception:
        return "dev"


ASCII_BANNER = """
$$$$$$\\            $$\\       $$\\ $$$$$$$\\  $$\\           $$\\       
$$  __$$\\           $$ |      \\__|$$  __$$\\ \\__|          $$ |      
$$ /  $$ | $$$$$$\\  $$$$$$$\\  $$\\ $$ |  $$ |$$\\  $$$$$$$\\ $$$$$$$\\  
$$$$$$$$ |$$  __$$\\ $$  __$$\\ $$ |$$$$$$$  |$$ |$$  _____|$$  __$$\\ 
$$  __$$ |$$ |  \\__|$$ |  $$ |$$ |$$  __$$< $$ |$$ /      $$ |  $$ |
$$ |  $$ |$$ |      $$ |  $$ |$$ |$$ |  $$ |$$ |$$ |      $$ |  $$ |
$$ |  $$ |$$ |      $$$$$$$  |$$ |$$ |  $$ |$$ |\\$$$$$$$\\ $$ |  $$ |
\\__|  \\__|\\__|      \\_______/ \\__|\\__|  \\__|\\__| \\_______|\\___|  \\__|
"""


def display_banner(additional_info: Optional[str] = None, log_only: bool = True, console_only: bool = False) -> None:
    """
    Display a cool ASCII art banner for ArbiRich.

    Args:
        additional_info: Optional additional information to display
        log_only: If True, only log the banner, don't print to console
        console_only: If True, only print to console, don't log (overrides log_only)
    """
    version = get_version()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    footer = f"Cryptocurrency Arbitrage Platform | Version: {version} | {current_time}"
    separator = "=" * len(footer)

    # Combine banner parts
    banner = f"{ASCII_BANNER}\n{separator}\n{footer}\n{separator}"

    # Print the banner to console if not log_only or if console_only
    if not log_only or console_only:
        print(banner)

    # Add the banner to the log unless console_only is True
    if not console_only:
        for line in banner.split("\n"):
            if line.strip():
                logger.info(line)

    # Print additional information if provided
    if additional_info:
        info_lines = additional_info.strip().split("\n")
        for line in info_lines:
            # Log it unless console_only is True
            if not console_only:
                logger.info(line)

            # Print it if not log_only or if console_only
            if not log_only or console_only:
                print(line)
