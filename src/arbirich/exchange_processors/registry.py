import importlib
import logging
import pkgutil
from typing import List

from src.arbirich.config import EXCHANGES

logger = logging.getLogger(__name__)


def register_all_processors() -> List[str]:
    """
    Ensure all exchange processors are imported and registered.

    Returns:
        List of registered processor names
    """
    logger.info("Registering all exchange processors...")

    # Get the module path for the exchange_processors package
    import src.arbirich.exchange_processors as processors_pkg

    # List of processor modules that were successfully imported
    imported_processors = []

    # Import each Python file in the exchange_processors directory
    for _, name, is_pkg in pkgutil.iter_modules(processors_pkg.__path__):
        if not is_pkg and name not in ("base_processor", "processor_factory", "registry"):
            try:
                module = importlib.import_module(f"src.arbirich.exchange_processors.{name}")
                logger.info(f"Successfully imported processor module: {name}")
                imported_processors.append(name)
            except Exception as e:
                logger.error(f"Failed to import processor module {name}: {e}")

    # Get the registered processor names by checking what processors are available for different exchanges
    from src.arbirich.exchange_processors.processor_factory import get_processor_for_exchange

    # Create a dictionary to map exchange names to processor class names
    registered_processors = {}

    for exchange in EXCHANGES:
        try:
            processor_class = get_processor_for_exchange(exchange)
            if processor_class:
                registered_processors[exchange] = processor_class.__name__
                logger.info(f"Processor for {exchange}: {processor_class.__name__}")
        except Exception as e:
            logger.error(f"Error getting processor for {exchange}: {e}")

    logger.info(f"Registered processors: {list(registered_processors.keys())}")

    # Return the list of processor keys
    return list(registered_processors.keys())


# Perform registration when module is imported
registered_processor_names = register_all_processors()
