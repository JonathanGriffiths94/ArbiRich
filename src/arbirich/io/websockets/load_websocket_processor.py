import importlib
import logging

from src.arbirich.config import EXCHANGE_CONFIGS

logger = logging.getLogger(__name__)


def load_processor(exchange: str):
    config = EXCHANGE_CONFIGS.get(exchange)
    if not config:
        raise ValueError(f"No configuration found for exchange: {exchange}")
    # Get the processor name from the WS sub-config
    processor_name = config["ws"]["processor"]
    # Dynamically import the exchange-specific module.
    module_name = f"src.arbirich.io.websockets.{exchange.lower()}"
    try:
        module = importlib.import_module(module_name)
        logger.info(f"Module: {module}")
    except ImportError as e:
        logger.error(f"Error importing module {module_name}: {e}")
        raise
    # Retrieve the processor class from the module.
    try:
        processor_cls = getattr(module, processor_name)
    except AttributeError as e:
        logger.error(
            f"Module {module_name} does not have attribute {processor_name}: {e}"
        )
        raise
    return processor_cls
