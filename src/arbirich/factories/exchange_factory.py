from typing import Dict, Optional

from src.arbirich.exchange.base_exchange import ExchangeClient
from src.arbirich.factories.factory_base import FactoryBase

# Singleton instance
_instance = FactoryBase[ExchangeClient]("Exchange")

# Create registration decorator
register_exchange = _instance.create_decorator()


def get_exchange_client(exchange_name: str, config: Optional[Dict] = None) -> Optional[ExchangeClient]:
    """Get or create an exchange client instance"""
    return _instance.get_instance(exchange_name, True, exchange_name, config or {})


# Re-export the instance's methods for backward compatibility
register = _instance.register
