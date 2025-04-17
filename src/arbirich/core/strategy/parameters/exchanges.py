from typing import Any, Dict, List


class ExchangeAndTradingPairs:
    """Exchange and trading pair configuration"""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize exchange and trading pair configuration

        Args:
            config: Dictionary of exchange configuration
        """
        # List of exchanges to use
        self.exchanges = config.get("exchanges", [])

        # List of trading pairs as tuples (base, quote)
        pairs_list = config.get("trading_pairs", [])
        self.trading_pairs = []

        # Process trading pairs into a consistent format
        for pair in pairs_list:
            if isinstance(pair, str):
                # Convert string format (e.g., "BTC-USDT") to tuple
                if "-" in pair:
                    base, quote = pair.split("-", 1)
                    self.trading_pairs.append((base, quote))
                elif "_" in pair:
                    base, quote = pair.split("_", 1)
                    self.trading_pairs.append((base, quote))
                else:
                    # Assume last 4-6 characters are the quote currency
                    # This is a simplification; real code would need more robust parsing
                    if len(pair) >= 6:
                        quote = pair[-4:]
                        base = pair[:-4]
                        self.trading_pairs.append((base, quote))
            elif isinstance(pair, tuple) and len(pair) == 2:
                # Already in tuple format
                self.trading_pairs.append(pair)

        # Exchange-specific settings
        self.exchange_settings = config.get("exchange_settings", {})

        # Credential references (e.g., keys to lookup credentials)
        self.credential_references = config.get("credential_references", {})

    def get_formatted_pairs(self) -> List[str]:
        """
        Get trading pairs in formatted string format (e.g., 'BTC-USDT')

        Returns:
            List of formatted trading pairs
        """
        return [f"{base}-{quote}" for base, quote in self.trading_pairs]

    def get_exchange_pairs(self) -> Dict[str, List[str]]:
        """
        Get mapping of exchanges to their trading pairs

        Returns:
            Dictionary mapping exchange names to lists of formatted pairs
        """
        result = {}
        formatted_pairs = self.get_formatted_pairs()

        for exchange in self.exchanges:
            # Get the pairs supported by this exchange (default to all)
            supported_pairs = self.exchange_settings.get(exchange, {}).get("supported_pairs", formatted_pairs)
            result[exchange] = supported_pairs

        return result

    def get_exchange_setting(self, exchange: str, setting: str, default: Any = None) -> Any:
        """
        Get a specific setting for an exchange

        Args:
            exchange: Exchange name
            setting: Setting name
            default: Default value if setting not found

        Returns:
            Setting value or default
        """
        return self.exchange_settings.get(exchange, {}).get(setting, default)

    def get_credential_key(self, exchange: str) -> str:
        """
        Get the credential reference key for an exchange

        Args:
            exchange: Exchange name

        Returns:
            Credential reference key or the exchange name
        """
        return self.credential_references.get(exchange, exchange)
