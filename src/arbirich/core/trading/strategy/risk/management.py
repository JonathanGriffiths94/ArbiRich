import logging
import time
from typing import Any, Dict

from ..parameters.configuration import ConfigurationParameters
from ..parameters.performance import PerformanceMetrics

logger = logging.getLogger(__name__)


class RiskManagement:
    """Risk management component for arbitrage strategies"""

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize risk management with configuration

        Args:
            config: Risk management configuration
        """
        self.config = config

        # Risk limits
        self.max_position_size = config.get("max_position_size", 1.0)
        self.max_daily_loss = config.get("max_daily_loss", 5.0)  # % of account
        self.max_drawdown = config.get("max_drawdown", 10.0)  # % of account
        self.max_consecutive_losses = config.get("max_consecutive_losses", 3)

        # Circuit breakers
        self.circuit_breaker_cooldown = config.get("circuit_breaker_cooldown", 3600)  # 1 hour
        self.last_circuit_break_time = 0
        self.circuit_breaker_active = False

        # Volume scaling
        self.scale_by_spread = config.get("scale_by_spread", True)
        self.base_spread_threshold = config.get("base_spread_threshold", 0.001)  # 0.1%
        self.max_spread_multiple = config.get("max_spread_multiple", 5.0)  # Scale up to 5x for very good spreads

        # Exchange risk adjustments
        self.exchange_risk_factors = config.get("exchange_risk_factors", {})

    def validate_trade(self, opportunity: Any, spread: float) -> bool:
        """
        Validate if a trade meets risk criteria

        Args:
            opportunity: The arbitrage opportunity
            spread: The calculated spread/profit

        Returns:
            True if trade is acceptable, False otherwise
        """
        # Check if circuit breaker is active
        if self.circuit_breaker_active:
            cooldown_elapsed = time.time() - self.last_circuit_break_time
            if cooldown_elapsed < self.circuit_breaker_cooldown:
                logger.warning(
                    f"Circuit breaker active, {self.circuit_breaker_cooldown - cooldown_elapsed:.1f}s "
                    f"remaining in cooldown"
                )
                return False
            else:
                # Reset circuit breaker after cooldown
                self.circuit_breaker_active = False

        # Check exchanges risk factors
        buy_exchange = getattr(opportunity, "buy_exchange", None)
        sell_exchange = getattr(opportunity, "sell_exchange", None)

        if buy_exchange and buy_exchange in self.exchange_risk_factors:
            risk_factor = self.exchange_risk_factors[buy_exchange]
            if risk_factor < 0.5:  # Example threshold
                logger.warning(f"Buy exchange {buy_exchange} has high risk factor: {risk_factor}")
                return False

        if sell_exchange and sell_exchange in self.exchange_risk_factors:
            risk_factor = self.exchange_risk_factors[sell_exchange]
            if risk_factor < 0.5:  # Example threshold
                logger.warning(f"Sell exchange {sell_exchange} has high risk factor: {risk_factor}")
                return False

        # All checks passed
        return True

    def calculate_position_size(self, opportunity: Any, params: ConfigurationParameters) -> float:
        """
        Calculate appropriate position size based on risk parameters

        Args:
            opportunity: The arbitrage opportunity
            params: Strategy configuration parameters

        Returns:
            Calculated position size
        """
        # Get base position size
        position_size = min(getattr(opportunity, "volume", 0.0) or 0.0, params.max_volume)

        # Respect minimum position size
        if position_size < params.min_volume:
            return 0.0  # Too small, don't trade

        # Scale by spread if enabled
        if self.scale_by_spread:
            spread = getattr(opportunity, "spread", 0.0)
            if spread > self.base_spread_threshold:
                # Calculate scaling factor (higher spread = larger position)
                spread_multiple = min(spread / self.base_spread_threshold, self.max_spread_multiple)
                position_size = min(position_size * spread_multiple, params.max_volume)

        # Apply exchange-specific scaling
        buy_exchange = getattr(opportunity, "buy_exchange", None)
        sell_exchange = getattr(opportunity, "sell_exchange", None)

        if buy_exchange and buy_exchange in self.exchange_risk_factors:
            position_size *= self.exchange_risk_factors[buy_exchange]

        if sell_exchange and sell_exchange in self.exchange_risk_factors:
            position_size *= self.exchange_risk_factors[sell_exchange]

        # Ensure position size is between min and max
        position_size = max(min(position_size, self.max_position_size), params.min_volume)

        logger.debug(f"Calculated position size: {position_size}")
        return position_size

    def check_circuit_breakers(self, metrics: PerformanceMetrics) -> bool:
        """
        Check if any circuit breakers have been triggered

        Args:
            metrics: Current performance metrics

        Returns:
            True if trading can continue, False if circuit breaker triggered
        """
        # Check consecutive losses
        if metrics.consecutive_losses >= self.max_consecutive_losses:
            logger.warning(
                f"Circuit breaker triggered: {metrics.consecutive_losses} consecutive losses "
                f"(max allowed: {self.max_consecutive_losses})"
            )
            self.trigger_circuit_breaker()
            return False

        # Check daily loss limit
        account_value = 1000.0  # This would be fetched from account in a real implementation
        daily_loss_limit = account_value * (self.max_daily_loss / 100.0)

        if -metrics.daily_profit > daily_loss_limit:
            logger.warning(
                f"Circuit breaker triggered: Daily loss ${-metrics.daily_profit:.2f} exceeds "
                f"limit ${daily_loss_limit:.2f}"
            )
            self.trigger_circuit_breaker()
            return False

        # All checks passed
        return True

    def trigger_circuit_breaker(self) -> None:
        """Trigger the circuit breaker to pause trading"""
        self.circuit_breaker_active = True
        self.last_circuit_break_time = time.time()
        logger.warning(
            f"Circuit breaker activated. Trading paused for {self.circuit_breaker_cooldown / 60.0:.1f} minutes"
        )
