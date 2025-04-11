"""
Template filters for Jinja2 templates.
"""

import random
from datetime import datetime


def timestamp_to_date(timestamp):
    """Convert UNIX timestamp to formatted date string."""
    try:
        if not timestamp:
            return "N/A"

        # Handle both float and int timestamps
        dt = datetime.fromtimestamp(float(timestamp))
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return str(timestamp)


def format_number(value, precision=2):
    """Format a number with the specified precision."""
    try:
        if value is None:
            return "N/A"
        return f"{float(value):.{precision}f}"
    except (ValueError, TypeError):
        return str(value)


def format_currency(value, symbol="$", precision=2):
    """Format a currency value with the specified precision."""
    try:
        if value is None:
            return "N/A"
        return f"{symbol}{float(value):.{precision}f}"
    except (ValueError, TypeError):
        return str(value)


def format_percentage(value, precision=2):
    """Format a value as a percentage."""
    try:
        if value is None:
            return "N/A"
        return f"{float(value):.{precision}f}%"
    except (ValueError, TypeError):
        return str(value)


def random_chart_data(length=30, min_val=0, max_val=100):
    """Generate random chart data for demonstrations."""
    return [random.randint(min_val, max_val) for _ in range(length)]


# Register the filters with the Jinja2 environment
def register_filters(app):
    """Register custom filters with the FastAPI application."""
    # Add filters to Jinja2 templates
    app.jinja2_env.filters["timestamp_to_date"] = timestamp_to_date
    app.jinja2_env.filters["format_number"] = format_number
    app.jinja2_env.filters["format_currency"] = format_currency
    app.jinja2_env.filters["format_percentage"] = format_percentage
    app.jinja2_env.filters["random_chart_data"] = random_chart_data
