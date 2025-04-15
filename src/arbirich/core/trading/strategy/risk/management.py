import logging
import time
from typing import Any, Dict, Optional

from arbirich.models.config_models import RiskConfig
from arbirich.models.models import RiskProfile, TradeOpportunity

from ..parameters.configuration import ConfigurationParameters
from ..parameters.performance import PerformanceMetrics

logger = logging.getLogger(__name__)


class RiskManagement:
    """Risk management component for arbitrage strategies"""

    def __init__(self, risk_config: Dict[str, Any], risk_profile: Optional[RiskProfile] = None):
        """
        Initialize risk management with configuration

        Args:
            risk_config: Risk management configuration from config file or dictionary
            risk_profile: Optional risk profile from database
        """
        # Convert dictionary to RiskConfig if needed
        if isinstance(risk_config, dict):
            logger.debug("Converting risk_config dictionary to RiskConfig model")
            risk_config = RiskConfig(**risk_config)

        self.risk_config = risk_config
        self.risk_profile = risk_profile

        # Risk limits
        self.max_position_size = risk_config.max_position_size
        self.max_daily_loss = risk_config.max_daily_loss
        self.max_drawdown = risk_config.max_drawdown
        self.max_consecutive_losses = risk_config.max_consecutive_losses

        # Circuit breakers
        self.circuit_breaker_cooldown = risk_config.circuit_breaker_cooldown
        self.last_circuit_break_time = 0
        self.circuit_breaker_active = False

        # Volume scaling
        self.scale_by_spread = risk_config.scale_by_spread
        self.base_spread_threshold = risk_config.base_spread_threshold
        self.max_spread_multiple = risk_config.max_spread_multiple

        # Exchange risk adjustments
        self.exchange_risk_factors = risk_config.exchange_risk_factors

        # Use risk profile values if available (override config)
        if risk_profile:
            if risk_profile.max_position_size_percentage:
                # Convert percentage to absolute value (assuming 1.0 = 100%)
                self.max_position_size = risk_profile.max_position_size_percentage / 100.0

            if risk_profile.max_drawdown_percentage:
                self.max_drawdown = risk_profile.max_drawdown_percentage

            if risk_profile.circuit_breaker_conditions:
                cb_conditions = risk_profile.circuit_breaker_conditions
                self.max_consecutive_losses = cb_conditions.get("max_consecutive_losses", self.max_consecutive_losses)
                self.circuit_breaker_cooldown = cb_conditions.get("cooldown_seconds", self.circuit_breaker_cooldown)

    def validate_trade(self, opportunity: TradeOpportunity, spread: float) -> bool:
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
        buy_exchange = opportunity.buy_exchange
        sell_exchange = opportunity.sell_exchange

        if buy_exchange in self.exchange_risk_factors:
            risk_factor = self.exchange_risk_factors[buy_exchange]
            if risk_factor < 0.5:  # Example threshold
                logger.warning(f"Buy exchange {buy_exchange} has high risk factor: {risk_factor}")
                return False

        if sell_exchange in self.exchange_risk_factors:
            risk_factor = self.exchange_risk_factors[sell_exchange]
            if risk_factor < 0.5:  # Example threshold
                logger.warning(f"Sell exchange {sell_exchange} has high risk factor: {risk_factor}")
                return False

        # All checks passed
        return True

    def calculate_position_size(self, opportunity: TradeOpportunity, params: ConfigurationParameters) -> float:
        """
        Calculate appropriate position size based on risk parameters

        Args:
            opportunity: The arbitrage opportunity
            params: Strategy configuration parameters

        Returns:
            Calculated position size
        """
        # Get base position size
        position_size = min(opportunity.volume, params.max_volume)

        # Respect minimum position size
        if position_size < params.min_volume:
            return 0.0  # Too small, don't trade

        # Scale by spread if enabled
        if self.scale_by_spread:
            spread = opportunity.spread
            if spread > self.base_spread_threshold:
                # Calculate scaling factor (higher spread = larger position)
                spread_multiple = min(spread / self.base_spread_threshold, self.max_spread_multiple)
                position_size = min(position_size * spread_multiple, params.max_volume)

        # Apply exchange-specific scaling
        buy_exchange = opportunity.buy_exchange
        sell_exchange = opportunity.sell_exchange

        if buy_exchange in self.exchange_risk_factors:
            position_size *= self.exchange_risk_factors[buy_exchange]

        if sell_exchange in self.exchange_risk_factors:
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
