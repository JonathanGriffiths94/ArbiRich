"""
Trading system components package.
"""

# Explicitly make component classes available at the package level for easier imports
try:
    from .base import Component
    from .detection import DetectionComponent
    from .execution import ExecutionComponent
    from .ingestion import IngestionComponent
    from .reporting import ReportingComponent

    __all__ = [
        "Component",
        "DetectionComponent",
        "ExecutionComponent",
        "IngestionComponent",
        "ReportingComponent",
    ]
except ImportError as e:
    # Log the error but don't fail - this allows the package to still be imported
    import logging

    logging.getLogger(__name__).error(f"Error importing component modules: {e}")
