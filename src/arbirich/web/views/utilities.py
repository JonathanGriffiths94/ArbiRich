import logging

from src.arbirich.models import enums

# Get the logger
logger = logging.getLogger(__name__)


def add_template_utilities(app):
    """
    Add utility functions and objects to Jinja templates.
    """

    @app.context_processor
    def utility_processor():
        """Add utilities to template context"""
        return {
            "enums": enums,
            # Add other utility functions/objects here
        }
