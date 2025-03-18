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


def display_banner(additional_info: Optional[str] = None, log_only: bool = False) -> None:
    """
    Display a cool ASCII art banner for ArbiRich.

    Args:
        additional_info: Optional additional information to display
        log_only: If True, only log the banner, don't print to console
    """
    version = get_version()
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    banner = r"""
    ╔═══════════════════════════════════════════════════════════════════╗
    ║                                                                   ║
    ║     ___         __    _ ____  _      __                           ║
    ║    /   |  _____/ /_  (_) __ \(_)____/ /_                          ║
    ║   / /| | / ___/ __ \/ / /_/ / / ___/ __ \                         ║
    ║  / ___ |/ /  / /_/ / / _, _/ / /__/ / / /                         ║
    ║ /_/  |_/_/  /_.___/_/_/ |_/_/\___/_/ /_/                          ║
    ║                                                                   ║
    ║              Crypto Arbitrage Trading Platform                    ║
    ╟───────────────────────────────────────────────────────────────────╢
    ║  Version: {version:<10}                  Started: {time}  ║
    ╚═══════════════════════════════════════════════════════════════════╝
    """

    # Format the banner with version and time
    formatted_banner = banner.format(version=version, time=current_time)

    # Print the banner to console if not log_only
    if not log_only:
        print(formatted_banner)

    # Add the banner to the log
    for line in formatted_banner.split("\n"):
        if line.strip():
            logger.info(line)

    # Print additional information if provided
    if additional_info:
        info_lines = additional_info.strip().split("\n")
        for line in info_lines:
            logger.info(line)
            if not log_only:
                print(line)
