import logging
from typing import Dict, Type

from src.arbirich.services.exchange_processors.base_processor import BaseOrderBookProcessor
from src.arbirich.services.exchange_processors.default_processor import DefaultOrderBookProcessor

logger = logging.getLogger(__name__)

# Import specific processor classes - add all your exchange processors here
try:
    from src.arbirich.services.exchange_processors.bybit import BybitOrderBookProcessor

    BYBIT_PROCESSOR_AVAILABLE = True
except ImportError:
    logger.warning("Bybit processor not available, will use default")
    BYBIT_PROCESSOR_AVAILABLE = False

try:
    from src.arbirich.services.exchange_processors.cryptocom import CryptocomOrderBookProcessor

    CRYPTOCOM_PROCESSOR_AVAILABLE = True
except ImportError:
    logger.warning("Crypto.com processor not available, will use default")
    CRYPTOCOM_PROCESSOR_AVAILABLE = False

# Define standard exchange name normalization
EXCHANGE_NAME_MAP = {
    # Standard names
    "bybit": "bybit",
    "cryptocom": "cryptocom",
    "binance": "binance",
    # Common variations (case insensitive mapping)
    "bybit.com": "bybit",
    "crypto_com": "cryptocom",
    "binance.com": "binance",
}

# Define exchange to processor class mapping
EXCHANGE_PROCESSOR_MAP: Dict[str, Type[BaseOrderBookProcessor]] = {
    # Default fallback processor
    "default": DefaultOrderBookProcessor,
}

# Add specific processors if available
if BYBIT_PROCESSOR_AVAILABLE:
    EXCHANGE_PROCESSOR_MAP["bybit"] = BybitOrderBookProcessor

if CRYPTOCOM_PROCESSOR_AVAILABLE:
    EXCHANGE_PROCESSOR_MAP["cryptocom"] = CryptocomOrderBookProcessor


def normalize_exchange_name(exchange_name: str) -> str:
    """
    Normalize exchange names to their standard form.
    """
    # Convert to lowercase for case-insensitive lookup
    name_lower = exchange_name.lower()

    # Check direct mapping
    if name_lower in EXCHANGE_NAME_MAP:
        return EXCHANGE_NAME_MAP[name_lower]

    # Check if any mapping key is a substring of the input
    for key, value in EXCHANGE_NAME_MAP.items():
        if key in name_lower:
            logger.info(f"Normalized exchange name '{exchange_name}' to '{value}' via substring match")
            return value

    # If no mapping found, return the original (lowercase)
    logger.warning(f"No normalization mapping found for '{exchange_name}', using as-is")
    return name_lower


def get_processor_class_for_exchange(exchange_name: str) -> Type[BaseOrderBookProcessor]:
    """
    Get the appropriate processor class for an exchange.

    Args:
        exchange_name: Exchange name (will be normalized)

    Returns:
        Processor class for the exchange
    """
    normalized_name = normalize_exchange_name(exchange_name)

    if normalized_name in EXCHANGE_PROCESSOR_MAP:
        processor_class = EXCHANGE_PROCESSOR_MAP[normalized_name]
        logger.info(f"Using {processor_class.__name__} for exchange {exchange_name}")
        return processor_class

    # Fallback to default processor
    logger.warning(f"No specific processor found for exchange '{exchange_name}', using default processor")
    return DefaultOrderBookProcessor
