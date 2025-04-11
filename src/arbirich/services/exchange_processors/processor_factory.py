import importlib
import logging
import pkgutil
from typing import Dict, Type

from src.arbirich.services.exchange_processors.base_processor import BaseOrderBookProcessor
from src.arbirich.services.exchange_processors.processor_mapping import get_processor_class_for_exchange
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

    # First try to get the processor from the mapping mechanism
    try:
        processor_class = get_processor_class_for_exchange(exchange)
        if processor_class:
            logger.info(f"Found processor for {exchange} using processor mapping")
            return processor_class
    except Exception as e:
        logger.warning(f"Error getting processor from mapping for {exchange}: {e}")

    # Then try to get from the registry
    try:
        processor_class = get_processor_class(exchange)
        if processor_class:
            logger.info(f"Found processor for {exchange} in registry")
            return processor_class
    except Exception as e:
        logger.warning(f"Error getting processor from registry for {exchange}: {e}")

    # Try case-insensitive matching with the registry
    from src.arbirich.services.exchange_processors.registry import _processor_registry

    if _processor_registry:  # Check if registry has items
        for name, cls in _processor_registry.items():
            if name.lower() == exchange.lower():
                logger.info(f"Found processor for {exchange} with case-insensitive match: {name}")
                return cls

    # Fallback to default processor
    from src.arbirich.services.exchange_processors.default_processor import DefaultOrderBookProcessor

    logger.warning(f"No processor found for exchange {exchange}, using default processor")
    return DefaultOrderBookProcessor


def create_processor(
    exchange: str,
    product: str,
    subscription_type: str = "snapshot",
    use_rest_snapshot: bool = False,
) -> BaseOrderBookProcessor:
    """
    Create an order book processor for the given exchange.

    Parameters:
        exchange: The exchange name
        product: The product symbol
        subscription_type: Either "snapshot" or "delta"
        use_rest_snapshot: Whether to use a REST API for initial snapshot

    Returns:
        An order book processor instance
    """
    try:
        # Get processor class
        processor_class = get_processor_for_exchange(exchange)

        # Ensure we have a valid processor class
        if not processor_class:
            # This should never happen as get_processor_for_exchange always returns a default,
            # but we check just to be safe
            from src.arbirich.services.exchange_processors.default_processor import DefaultOrderBookProcessor

            logger.error(f"Could not find a processor for {exchange}, falling back to default")
            processor_class = DefaultOrderBookProcessor

        # Create an instance of the processor
        processor = processor_class(
            exchange=exchange, product=product, subscription_type=subscription_type, use_rest_snapshot=use_rest_snapshot
        )

        logger.info(f"Created processor for {exchange}: {processor.__class__.__name__}")
        return processor

    except Exception as e:
        logger.error(f"Error creating processor for {exchange}: {e}")
        # In case of error, try to create a default processor as last resort
        try:
            from src.arbirich.services.exchange_processors.default_processor import DefaultOrderBookProcessor

            logger.warning(f"Creating default processor for {exchange} after error")
            return DefaultOrderBookProcessor(
                exchange=exchange,
                product=product,
                subscription_type=subscription_type,
                use_rest_snapshot=use_rest_snapshot,
            )
        except Exception as fallback_error:
            logger.error(f"Even default processor creation failed: {fallback_error}")
            raise
