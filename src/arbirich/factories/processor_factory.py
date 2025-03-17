import importlib
import logging
import pkgutil
from typing import Type

from src.arbirich.factories.factory_base import FactoryBase
from src.arbirich.io.exchange_processors.base_processor import BaseOrderBookProcessor

logger = logging.getLogger(__name__)

# Singleton instance
_instance = FactoryBase[BaseOrderBookProcessor]("ExchangeProcessor")

# Create registration decorator
register = _instance.create_decorator()


def get_processor_for_exchange(exchange: str) -> Type[BaseOrderBookProcessor]:
    """Get the processor class for a specific exchange."""
    # Force discovery of processors if not already done
    discover_processors()

    # Get processor (case-insensitive)
    processor_class = _instance.get_instance(exchange)

    if processor_class is None:
        # Try case-insensitive lookup
        for key in _instance._registry.keys():
            if key.lower() == exchange.lower():
                return _instance.get_instance(key)

        # Fallback to default
        from src.arbirich.io.exchange_processors.default_processor import DefaultOrderBookProcessor

        logger.warning(f"No processor found for exchange {exchange}, using default processor")
        return DefaultOrderBookProcessor

    return processor_class


def discover_processors():
    """Discover all available exchange processors."""
    if not _instance._registry:
        logger.info("Discovering exchange processors...")

        # Import all modules in the package
        import src.arbirich.io.exchange_processors as pkg

        for _, name, is_pkg in pkgutil.iter_modules(pkg.__path__):
            if not is_pkg and name not in ["processor_factory", "registry", "__init__"]:
                try:
                    importlib.import_module(f"src.arbirich.io.exchange_processors.{name}")
                    logger.info(f"Registered processor for exchange: {name}")
                except Exception as e:
                    logger.error(f"Error importing processor module {name}: {e}")
