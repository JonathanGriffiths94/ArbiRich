import importlib
import inspect
import logging
import os
import pkgutil
from typing import Dict, Type

from src.arbirich.config.config import ALL_EXCHANGES, EXCHANGES

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
        # Register with exact exchange name from parameter
        _processor_registry[exchange_name] = processor_class
        logger.info(f"Registered processor for exchange: {exchange_name}")
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
    processor_class = _processor_registry.get(exchange_name)

    if processor_class is None:
        # Try case-insensitive match as fallback
        exchange_name_lower = exchange_name.lower()
        for name, cls in _processor_registry.items():
            if name.lower() == exchange_name_lower:
                logger.info(f"Found processor for {exchange_name} using case-insensitive match to {name}")
                return cls

    return processor_class


def list_exchange_modules():
    """
    List all potential exchange processor modules in the package.
    Helps troubleshoot registration issues.
    """
    import src.arbirich.services.exchange_processors as pkg

    module_dir = os.path.dirname(pkg.__file__)
    logger.info(f"Exchange processor directory: {module_dir}")

    existing_files = [f for f in os.listdir(module_dir) if f.endswith(".py")]
    logger.info(f"Python files in directory: {existing_files}")

    return [
        name
        for _, name, is_pkg in pkgutil.iter_modules(pkg.__path__)
        if not is_pkg
        and name
        not in ["registry", "processor_factory", "base_processor", "__init__", "default_processor", "processor_mapping"]
    ]


def register_all_processors():
    """
    Discover and register all processor classes in the package,
    filtering to only those exchanges defined in EXCHANGES config.
    """
    # Log and check registered exchanges
    configured_exchanges = list(EXCHANGES.keys() if isinstance(EXCHANGES, dict) else EXCHANGES)
    all_exchanges = list(ALL_EXCHANGES.keys() if isinstance(ALL_EXCHANGES, dict) else ALL_EXCHANGES)

    logger.info(f"Registering exchange processors for configured exchanges: {configured_exchanges}")
    logger.info(f"All available exchanges: {all_exchanges}")

    # Get all potential exchange modules
    available_modules = list_exchange_modules()
    logger.info(f"Available exchange modules: {available_modules}")

    # Check for exchange name mismatches
    for exchange in configured_exchanges:
        exchange_lower = exchange.lower()
        if exchange not in available_modules and f"{exchange_lower}_processor" not in available_modules:
            logger.warning(f"No processor module found for exchange: {exchange}")
            logger.warning(f"Looked for modules named '{exchange}' or '{exchange_lower}_processor'")

    # First, we'll import all the processor modules to trigger the decorators
    import src.arbirich.services.exchange_processors as pkg

    for _, name, is_pkg in pkgutil.iter_modules(pkg.__path__):
        # Skip certain modules
        if not is_pkg and name not in [
            "registry",
            "processor_factory",
            "base_processor",
            "__init__",
            "default_processor",
            "proccessor_mapping",
        ]:
            # Try to check if this module matches any configured exchange
            module_matches_exchange = False
            exchange_match = None

            # Check if module name directly matches an exchange name
            if name in configured_exchanges:
                module_matches_exchange = True
                exchange_match = name
            # Check if module name is a processor for a configured exchange
            else:
                for exchange in configured_exchanges:
                    # Try potential naming patterns
                    if (
                        name == f"{exchange}_processor"
                        or name == exchange.lower()
                        or name == f"{exchange.lower()}_processor"
                    ):
                        module_matches_exchange = True
                        exchange_match = exchange
                        break

            if module_matches_exchange or name in all_exchanges:
                try:
                    # Import the module to trigger the register decorators
                    module = importlib.import_module(f"src.arbirich.services.exchange_processors.{name}")
                    logger.info(f"Successfully imported processor module: {name}")

                    # Check if this module has any classes with the register decorator
                    has_processor = False
                    for attr_name in dir(module):
                        attr = getattr(module, attr_name)
                        if inspect.isclass(attr) and attr.__module__ == module.__name__:
                            has_processor = True

                    if not has_processor and exchange_match:
                        logger.warning(f"Module {name} imported but no processor class registered for {exchange_match}")

                except Exception as e:
                    logger.error(f"Error importing processor module {name}: {e}")
            else:
                logger.debug(f"Skipping exchange processor module {name} (not in configured exchanges)")

    # Log registered processors
    if _processor_registry:
        for exchange, processor_class in _processor_registry.items():
            logger.info(f"Processor for {exchange}: {processor_class.__name__}")
    else:
        logger.warning("No processors were registered. This might indicate an import issue.")

        # Provide a more helpful message
        logger.warning("Troubleshooting steps:")
        logger.warning("1. Check that processor files exist in the src/arbirich/services/exchange_processors directory")
        logger.warning("2. Ensure processor files use @register decorator with the correct exchange name")
        logger.warning("3. Verify exchange names in EXCHANGES config match the names used in @register")
        logger.warning("4. Check for import errors in the processor modules")

    logger.info(f"Registered processors: {list(_processor_registry.keys())}")

    return _processor_registry


# Perform registration when module is imported
# Adding this at the end ensures that the functions are defined before we try to use them
registered_processors = register_all_processors()
