import importlib
import logging
from typing import Dict, Type

logger = logging.getLogger(__name__)

# Registry of exchange processors
EXCHANGE_PROCESSORS: Dict[str, Type] = {}


def register_processor(exchange_name: str, processor_class: Type) -> None:
    """
    Register a processor for a specific exchange.

    Parameters:
        exchange_name: The name of the exchange to register the processor for
        processor_class: The processor class to register
    """
    EXCHANGE_PROCESSORS[exchange_name] = processor_class
    logger.info(f"Processor for {exchange_name}: {processor_class.__name__}")


def register_all_processors():
    """
    Register all available exchange processors.
    """
    logger.info("Registering all exchange processors...")

    try:
        # Import exchange processor modules
        package = "src.arbirich.io.exchange_processors"

        # Register for common exchanges
        register_common_processors()

        logger.info(f"Registered processors: {list(EXCHANGE_PROCESSORS.keys())}")

    except Exception as e:
        logger.error(f"Error registering processors: {e}")


def register_common_processors():
    """Register processors for common exchanges like Bybit and Crypto.com."""
    try:
        # Bybit processor
        logger.info("Registering processor for exchange: bybit")
        try:
            bybit_module = importlib.import_module("src.arbirich.io.exchange_processors.bybit")
            logger.info("Successfully imported processor module: bybit")
            register_processor("bybit", bybit_module.BybitOrderBookProcessor)
        except (ImportError, AttributeError) as e:
            logger.warning(f"Could not import bybit processor: {e}")

        # Crypto.com processor
        logger.info("Registering processor for exchange: cryptocom")
        try:
            cryptocom_module = importlib.import_module("src.arbirich.io.exchange_processors.cryptocom")
            logger.info("Successfully imported processor module: cryptocom")
            register_processor("cryptocom", cryptocom_module.CryptocomOrderBookProcessor)
        except (ImportError, AttributeError) as e:
            logger.warning(f"Could not import cryptocom processor: {e}")

        # Default processor as fallback
        logger.info("Registering processor for exchange: default")
        try:
            default_module = importlib.import_module("src.arbirich.io.exchange_processors.default_processor")
            logger.info("Successfully imported processor module: default_processor")
            register_processor("default", default_module.DefaultOrderBookProcessor)
        except (ImportError, AttributeError) as e:
            logger.warning(f"Could not import default processor: {e}")

    except Exception as e:
        logger.error(f"Error registering common processors: {e}")


# Register processors when this module is imported
register_all_processors()
