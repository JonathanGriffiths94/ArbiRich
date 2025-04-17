"""
Authentication manager for exchange connectors.

Handles secure storage and retrieval of API credentials.
"""

from typing import Any, Dict, Optional


class ExchangeAuthManager:
    """
    Manages authentication and credentials for exchange connectors.

    This class centralizes the storage and retrieval of API credentials
    for different exchanges, optionally with strategy-specific credentials.
    """

    def __init__(self, config_service):
        """
        Initialize the auth manager with a config service.

        Args:
            config_service: Service for retrieving configuration and credentials
        """
        self.config_service = config_service

    async def get_credentials(self, exchange_id: str, strategy_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get API credentials for an exchange, optionally specific to a strategy.

        Args:
            exchange_id: Identifier for the exchange
            strategy_id: Optional strategy identifier for strategy-specific credentials

        Returns:
            Dict containing credentials (api_key, api_secret, etc.)
        """
        # Fetch from secure storage through the config service
        credentials = await self.config_service.get_exchange_credentials(exchange_id, strategy_id)
        return credentials

    async def store_credentials(
        self, exchange_id: str, credentials: Dict[str, str], strategy_id: Optional[str] = None
    ) -> bool:
        """
        Store API credentials for an exchange.

        Args:
            exchange_id: Identifier for the exchange
            credentials: Dict containing credentials to store
            strategy_id: Optional strategy identifier for strategy-specific credentials

        Returns:
            Boolean indicating if the credentials were stored successfully
        """
        # Store in secure storage through the config service
        success = await self.config_service.store_exchange_credentials(exchange_id, credentials, strategy_id)
        return success

    async def validate_credentials(self, exchange_id: str, credentials: Dict[str, str]) -> bool:
        """
        Validate that credentials have all required fields for an exchange.

        Args:
            exchange_id: Identifier for the exchange
            credentials: Dict containing credentials to validate

        Returns:
            Boolean indicating if the credentials are valid
        """
        # Get requirements for this exchange
        requirements = await self.config_service.get_exchange_credential_requirements(exchange_id)

        # Check if all required fields are present
        for field in requirements.get("required_fields", []):
            if field not in credentials or not credentials[field]:
                return False

        return True
