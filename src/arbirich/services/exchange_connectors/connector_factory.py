"""
Factory for creating exchange connectors.
"""

import logging
from typing import Dict, Optional, Type

from arbirich.services.exchange_connectors.connectors.base_connector import BaseExchangeConnector
from arbirich.services.exchange_connectors.connectors.binance_connector import BinanceConnector
from arbirich.services.exchange_connectors.connectors.bybit_connector import BybitConnector
from arbirich.services.exchange_connectors.connectors.cryptocom_connector import CryptocomConnector
from src.arbirich.models.enums import ExchangeType


class ConnectorFactory:
    """Factory class for creating exchange connectors."""

    _connectors: Dict[str, Type[BaseExchangeConnector]] = {
        ExchangeType.BYBIT.value: BybitConnector,
        ExchangeType.CRYPTOCOM.value: CryptocomConnector,
        ExchangeType.BINANCE.value: BinanceConnector,
    }

    @classmethod
    def get_connector(cls, exchange_name: str, config: dict) -> Optional[BaseExchangeConnector]:
        """
        Get a connector instance for the specified exchange.

        Args:
            exchange_name: Name of the exchange
            config: Exchange-specific configuration

        Returns:
            Exchange connector instance or None if not found
        """
        logger = logging.getLogger("arbirich.connectors.factory")

        # Normalize exchange name
        exchange_name = exchange_name.lower()

        if exchange_name in cls._connectors:
            connector_class = cls._connectors[exchange_name]
            logger.info(f"Creating {exchange_name} connector")
            return connector_class(config)

        logger.error(f"No connector found for exchange: {exchange_name}")
        return None

    @classmethod
    def register_connector(cls, exchange_name: str, connector_class: Type[BaseExchangeConnector]) -> None:
        """
        Register a new connector for an exchange.

        Args:
            exchange_name: Name of the exchange
            connector_class: Connector class to register
        """
        logger = logging.getLogger("arbirich.connectors.factory")

        # Normalize exchange name
        exchange_name = exchange_name.lower()

        cls._connectors[exchange_name] = connector_class
        logger.info(f"Registered connector for {exchange_name}")
