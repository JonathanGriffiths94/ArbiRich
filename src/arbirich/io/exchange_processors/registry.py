import importlib
import logging
import pkgutil
from typing import Dict, Type

logger = logging.getLogger(__name__)

# Dictionary to store processor classes
_processor_registry: Dict[str, Type] = {}


def register(exchange_name):
    """
    Decorator to register a processor class for a specific exchange.

    Parameters:
        exchange_name: The name of the exchange this processor handles
    """

    def decorator(processor_class):
        _processor_registry[exchange_name] = processor_class
        logger.info(f"Registering processor for exchange: {exchange_name}")
        return processor_class

    return decorator


def get_processor_class(exchange_name):
    """
    Get the processor class for a specific exchange.

    Parameters:
        exchange_name: The name of the exchange

    Returns:
        The processor class or None if not found
    """
    return _processor_registry.get(exchange_name)


def register_all_processors():
    """
    Discover and register all processor classes in the package.
    """
    logger.info("Registering all exchange processors...")

    # First, we'll import all the processor modules to trigger the decorators
    import src.arbirich.io.exchange_processors as pkg

    for _, name, is_pkg in pkgutil.iter_modules(pkg.__path__):
        # Skip certain modules
        if not is_pkg and name not in ["registry", "processor_factory", "base_processor", "__init__"]:
            try:
                # Import the module to trigger the register decorators
                importlib.import_module(f"src.arbirich.io.exchange_processors.{name}")
                logger.info(f"Successfully imported processor module: {name}")
            except Exception as e:
                logger.error(f"Error importing processor module {name}: {e}")

    # Log registered processors
    if _processor_registry:
        for exchange, processor_class in _processor_registry.items():
            logger.info(f"Processor for {exchange}: {processor_class.__name__}")
    else:
        logger.warning("No processors were registered. This might indicate an import issue.")

    logger.info(f"Registered processors: {list(_processor_registry.keys())}")

    return _processor_registry


# Perform registration when module is imported
# Adding this at the end ensures that the functions are defined before we try to use them
registered_processors = register_all_processors()
