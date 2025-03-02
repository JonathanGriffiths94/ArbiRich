import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def normalise_timestamp(timestamp):
    """Convert timestamps from different exchanges to a uniform UNIX timestamp in seconds."""
    if isinstance(timestamp, str):  # Handle ISO 8601 timestamps (e.g., Coinbase)
        dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        return dt.timestamp()
    elif isinstance(timestamp, int) or isinstance(
        timestamp, float
    ):  # Milliseconds or seconds
        return timestamp / 1000 if timestamp > 10**10 else timestamp
    else:
        raise ValueError(f"Invalid timestamp format: {timestamp}")
