import importlib
import logging
import pkgutil
import traceback
from typing import Dict, Type

from src.arbirich.io.websockets.base import BaseOrderBookProcessor

logger = logging.getLogger(__name__)

# Global registry of exchange processor classes
EXCHANGE_PROCESSORS: Dict[str, Type[BaseOrderBookProcessor]] = {}


def register_processor(exchange_name: str, processor_class: Type[BaseOrderBookProcessor]) -> None:
    """
    Register an exchange processor class for a specific exchange.

    Parameters:
        exchange_name: Name of the exchange (e.g., 'binance', 'coinbase')
        processor_class: The processor class to register
    """
    global EXCHANGE_PROCESSORS
    exchange_name = exchange_name.lower()
    logger.info(f"Registering processor for exchange: {exchange_name}")
    EXCHANGE_PROCESSORS[exchange_name] = processor_class


def get_processor_for_exchange(exchange_name: str) -> Type[BaseOrderBookProcessor]:
    """
    Get the appropriate processor class for a given exchange.

    Parameters:
        exchange_name: Name of the exchange (e.g., 'binance', 'coinbase')

    Returns:
        The processor class or DefaultOrderBookProcessor if not found
    """
    exchange_name = exchange_name.lower()

    try:
        # If we have a direct match, return it
        if exchange_name in EXCHANGE_PROCESSORS:
            logger.debug(f"Found registered processor for {exchange_name}")
            return EXCHANGE_PROCESSORS[exchange_name]

        # If we don't have a processor, use the default
        from src.arbirich.exchange_processors.default_processor import DefaultOrderBookProcessor

        logger.warning(f"No specific processor found for {exchange_name}, using DefaultOrderBookProcessor")
        return DefaultOrderBookProcessor
    except Exception as e:
        logger.error(f"Error getting processor for {exchange_name}: {e}\n{traceback.format_exc()}")
        # Ensure we always return a processor class, even in case of errors
        from src.arbirich.exchange_processors.default_processor import DefaultOrderBookProcessor

        return DefaultOrderBookProcessor


# Register decorator for processor classes
def register(exchange_name: str):
    """
    Decorator to register an exchange processor class.

    Example:
        @register('binance')
        class BinanceProcessor(BaseOrderBookProcessor):
            pass
    """

    def decorator(cls):
        try:
            register_processor(exchange_name, cls)
        except Exception as e:
            logger.error(f"Error registering processor for {exchange_name}: {e}")
        return cls

    return decorator


# Discover processor modules
def discover_processors() -> None:
    """
    Automatically discover and register all exchange processors.
    This function looks for modules in the exchange_processors package.
    """
    logger.info("Discovering exchange processors...")

    import src.arbirich.exchange_processors as processors_pkg

    # Find all modules in the exchange_processors package
    for _, name, is_pkg in pkgutil.iter_modules(processors_pkg.__path__):
        if not is_pkg and name not in ("processor_factory", "base_processor", "registry"):
            try:
                # Import the module
                module = importlib.import_module(f"src.arbirich.exchange_processors.{name}")
                logger.debug(f"Imported processor module: {name}")

                # The module should register its processors automatically via @register decorator
            except Exception as e:
                logger.error(f"Error importing processor module {name}: {e}")

    logger.info(f"Discovered {len(EXCHANGE_PROCESSORS)} exchange processors: {list(EXCHANGE_PROCESSORS.keys())}")


# Run discovery at module import time
discover_processors()

# For backwards compatibility - export names explicitly
__all__ = ["register", "get_processor_for_exchange", "EXCHANGE_PROCESSORS", "discover_processors"]
