import logging
from typing import Dict

logger = logging.getLogger(__name__)

# Default risk profiles for common exchanges
# Higher value = lower risk (1.0 = no risk adjustment, 0.5 = 50% risk reduction)
DEFAULT_EXCHANGE_RISK_PROFILES = {
    # Major exchanges - generally reliable
    "binance": 0.95,
    "coinbase": 0.95,
    "kraken": 0.90,
    "ftx": 0.85,  # Lower due to recent events
    # Mid-tier exchanges - moderate reliability
    "kucoin": 0.85,
    "huobi": 0.80,
    "okex": 0.80,
    "bitfinex": 0.80,
    "bybit": 0.80,
    # Smaller exchanges - higher risk
    "gate": 0.75,
    "bittrex": 0.70,
    "mexc": 0.70,
    "ascendex": 0.65,
    # Default for any unknown exchange
    "default": 0.60,
}

# Risk factors by exchange feature
RISK_FACTORS = {
    "api_reliability": {"high": 1.0, "medium": 0.9, "low": 0.7},
    "liquidity": {"high": 1.0, "medium": 0.85, "low": 0.6},
    "withdrawal_speed": {"high": 1.0, "medium": 0.95, "low": 0.8},
    "trading_fees": {"low": 1.0, "medium": 0.95, "high": 0.9},
}


class ExchangeRiskProfiler:
    """Manages risk profiles for different exchanges"""

    def __init__(self, custom_profiles: Dict[str, float] = None):
        """
        Initialize with custom exchange risk profiles

        Args:
            custom_profiles: Optional custom risk profiles to override defaults
        """
        self.profiles = DEFAULT_EXCHANGE_RISK_PROFILES.copy()

        # Override with custom profiles if provided
        if custom_profiles:
            self.profiles.update(custom_profiles)

        # Dynamic adjustments that can change during runtime
        self.dynamic_adjustments = {}

    def get_risk_factor(self, exchange: str) -> float:
        """
        Get the risk factor for an exchange

        Args:
            exchange: Exchange name

        Returns:
            Risk factor value (0.0-1.0)
        """
        # Start with base profile
        base = self.profiles.get(exchange.lower(), self.profiles.get("default", 0.6))

        # Apply any dynamic adjustments
        dynamic_factor = self.dynamic_adjustments.get(exchange.lower(), 1.0)

        return base * dynamic_factor

    def adjust_risk_temporarily(self, exchange: str, adjustment_factor: float, reason: str = "unspecified") -> None:
        """
        Apply a temporary risk adjustment factor to an exchange

        Args:
            exchange: Exchange name
            adjustment_factor: Factor to multiply risk by (e.g. 0.8 = reduce risk to 80%)
            reason: Reason for adjustment (for logging)
        """
        exchange = exchange.lower()
        current = self.dynamic_adjustments.get(exchange, 1.0)
        new_factor = current * adjustment_factor

        # Ensure factor stays in reasonable range
        new_factor = max(0.1, min(1.0, new_factor))

        self.dynamic_adjustments[exchange] = new_factor
        logger.info(
            f"Temporary risk adjustment for {exchange}: {current:.2f} -> {new_factor:.2f} "
            f"(factor: {adjustment_factor:.2f}, reason: {reason})"
        )

    def reset_adjustment(self, exchange: str = None):
        """
        Reset temporary risk adjustments

        Args:
            exchange: Exchange to reset, or None to reset all
        """
        if exchange:
            if exchange.lower() in self.dynamic_adjustments:
                del self.dynamic_adjustments[exchange.lower()]
                logger.info(f"Reset risk adjustment for {exchange}")
        else:
            self.dynamic_adjustments.clear()
            logger.info("Reset all exchange risk adjustments")

    def build_exchange_profile(self, exchange: str, features: Dict[str, str]) -> float:
        """
        Build a custom risk profile based on exchange features

        Args:
            exchange: Exchange name
            features: Dictionary of feature category -> rating (high/medium/low)

        Returns:
            Calculated risk factor
        """
        # Start with default base value
        base_value = 0.8

        # Apply feature-specific adjustments
        for feature, rating in features.items():
            if feature in RISK_FACTORS and rating in RISK_FACTORS[feature]:
                base_value *= RISK_FACTORS[feature][rating]

        # Log the calculated profile
        logger.info(f"Built custom risk profile for {exchange}: {base_value:.2f} based on {len(features)} features")

        return base_value
