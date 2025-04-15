from datetime import datetime


def convert_unix_timestamp_for_db(timestamp_value):
    """
    Convert a Unix timestamp (float) to a datetime object for database insertion.

    Args:
        timestamp_value: Unix timestamp as float

    Returns:
        datetime object suitable for database insertion
    """
    if timestamp_value is None:
        return None

    if isinstance(timestamp_value, (int, float)):
        # Convert Unix timestamp to datetime
        return datetime.fromtimestamp(timestamp_value)

    # Already a datetime or string - return as is
    return timestamp_value
