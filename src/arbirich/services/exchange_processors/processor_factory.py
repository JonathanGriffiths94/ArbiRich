import importlib
import logging
import pkgutil
from typing import Dict, Type

from src.arbirich.services.exchange_processors.base_processor import BaseOrderBookProcessor
from src.arbirich.services.exchange_processors.registry import get_processor_class

logger = logging.getLogger(__name__)

# Store discovered processor classes
_discovered_processors: Dict[str, Type[BaseOrderBookProcessor]] = {}


def discover_processors():
    """
    Discover all available exchange processors.
    """
    logger.info("Discovering exchange processors...")

    # Import all modules in the package
    import src.arbirich.services.exchange_processors as pkg

    for _, name, is_pkg in pkgutil.iter_modules(pkg.__path__):
        if not is_pkg and name != "processor_factory" and name != "registry" and name != "__init__":
            try:
                # Import the module to trigger decorator registration
                importlib.import_module(f"src.arbirich.services.exchange_processors.{name}")
                logger.info(f"Registering processor for exchange: {name}")
                _discovered_processors[name] = True
            except Exception as e:
                logger.error(f"Error importing processor module {name}: {e}")

    logger.info(f"Discovered {len(_discovered_processors)} exchange processors: {list(_discovered_processors.keys())}")
    return _discovered_processors


def get_processor_for_exchange(exchange: str) -> Type[BaseOrderBookProcessor]:
    """
    Get the processor class for a specific exchange.

    Parameters:
        exchange: The name of the exchange

    Returns:
        The processor class
    """
    # Force discovery of processors if not already done
    if not _discovered_processors:
        discover_processors()

    # Get the processor class from the registry
    from src.arbirich.services.exchange_processors.registry import _processor_registry

    processor_class = get_processor_class(exchange)

    if processor_class is None:
        # Try again with case-insensitive matching
        if _processor_registry:  # Check if registry has items
            for name, cls in _processor_registry.items():
                if name.lower() == exchange.lower():
                    logger.info(f"Found processor for {exchange} with case-insensitive match: {name}")
                    return cls

        # Fallback to default processor
        from src.arbirich.services.exchange_processors.default_processor import DefaultOrderBookProcessor

        logger.warning(f"No processor found for exchange {exchange}, using default processor")
        return DefaultOrderBookProcessor

    logger.info(f"Using {processor_class.__name__} for exchange {exchange}")
    return processor_class
