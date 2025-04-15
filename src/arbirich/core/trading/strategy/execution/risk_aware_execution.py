import logging
import time
from typing import Optional

from src.arbirich.models.models import TradeExecution, TradeOpportunity

from ..parameters.configuration import ConfigurationParameters
from ..risk.management import RiskManagement
from .method import ExecutionMethod

logger = logging.getLogger(__name__)


class RiskAwareExecution:
    """
    Risk-aware execution wrapper that integrates risk management with any execution method
    """

    def __init__(
        self, execution_method: ExecutionMethod, risk_management: RiskManagement, config_params: ConfigurationParameters
    ):
        """
        Initialize the risk-aware execution wrapper

        Args:
            execution_method: The underlying execution method to use
            risk_management: Risk management instance to apply
            config_params: Strategy configuration parameters
        """
        self.execution_method = execution_method
        self.risk_management = risk_management
        self.config_params = config_params
        self.pre_execution_hooks = []
        self.post_execution_hooks = []

    async def execute(self, opportunity: TradeOpportunity, position_size: Optional[float] = None) -> TradeExecution:
        """
        Execute a trade with risk management checks and adjustments

        Args:
            opportunity: The opportunity to execute
            position_size: Optional position size (will be calculated if None)

        Returns:
            TradeExecution result
        """
        execution_start = time.time()

        try:
            # === PRE-EXECUTION RISK CHECKS ===

            # 1. Validate the opportunity
            if not self.risk_management.validate_trade(opportunity, opportunity.spread):
                logger.warning(f"Risk validation failed for opportunity {opportunity.id}")
                return TradeExecution(
                    id=f"failed-{int(time.time())}",
                    opportunity_id=opportunity.id,
                    success=False,
                    partial=False,
                    error="Failed risk validation",
                    buy_exchange=opportunity.buy_exchange,
                    sell_exchange=opportunity.sell_exchange,
                    pair=opportunity.pair,
                    execution_timestamp=time.time(),
                    execution_time=0,
                    profit=0.0,
                    volume=0.0,
                    strategy=opportunity.strategy,
                    executed_buy_price=opportunity.buy_price,
                    executed_sell_price=opportunity.sell_price,
                    spread=opportunity.spread,
                    details={"risk_check": "failed"},
                )

            # 2. Calculate risk-adjusted position size if not provided
            if position_size is None:
                position_size = self.risk_management.calculate_position_size(opportunity, self.config_params)

            if position_size <= 0:
                logger.warning(f"Risk-adjusted position size too small: {position_size}")
                return TradeExecution(
                    id=f"failed-{int(time.time())}",
                    opportunity_id=opportunity.id,
                    success=False,
                    partial=False,
                    error="Position size too small after risk adjustment",
                    buy_exchange=opportunity.buy_exchange,
                    sell_exchange=opportunity.sell_exchange,
                    pair=opportunity.pair,
                    execution_timestamp=time.time(),
                    execution_time=0,
                    profit=0.0,
                    volume=0.0,
                    strategy=opportunity.strategy,
                    executed_buy_price=opportunity.buy_price,
                    executed_sell_price=opportunity.sell_price,
                    spread=opportunity.spread,
                    details={"risk_check": "position_size_too_small"},
                )

            # 3. Run custom pre-execution hooks
            for hook in self.pre_execution_hooks:
                modified = hook(opportunity, position_size)
                if isinstance(modified, dict) and "abort" in modified and modified["abort"]:
                    logger.warning(f"Execution aborted by pre-execution hook: {modified.get('reason', 'unknown')}")
                    return TradeExecution(
                        id=f"aborted-{int(time.time())}",
                        opportunity_id=opportunity.id,
                        success=False,
                        partial=False,
                        error=f"Aborted by pre-execution hook: {modified.get('reason', 'unknown')}",
                        buy_exchange=opportunity.buy_exchange,
                        sell_exchange=opportunity.sell_exchange,
                        pair=opportunity.pair,
                        execution_timestamp=time.time(),
                        execution_time=0,
                        profit=0.0,
                        volume=0.0,
                        strategy=opportunity.strategy,
                        executed_buy_price=opportunity.buy_price,
                        executed_sell_price=opportunity.sell_price,
                        spread=opportunity.spread,
                        details={"hook_abort": True, "reason": modified.get("reason", "unknown")},
                    )

                # Allow hooks to modify position size
                if isinstance(modified, dict) and "position_size" in modified:
                    position_size = modified["position_size"]
                    logger.info(f"Position size adjusted by hook to {position_size}")

            # === EXECUTION ===
            logger.info(f"Executing opportunity {opportunity.id} with position size {position_size}")
            result = await self.execution_method.execute(opportunity, position_size)

            # === POST-EXECUTION RISK ANALYSIS ===
            execution_time = (time.time() - execution_start) * 1000  # ms

            # 1. Update result with execution time
            if result.execution_time == 0:
                result.execution_time = execution_time

            # 2. Run custom post-execution hooks
            for hook in self.post_execution_hooks:
                hook(opportunity, result)

            # 3. Handle partial executions or failures with risk-aware approach
            if not result.success and result.partial:
                await self.handle_partial_execution(result)
            elif not result.success:
                await self.handle_failure(result)

            return result

        except Exception as e:
            logger.error(f"Error in risk-aware execution: {e}", exc_info=True)
            execution_time = (time.time() - execution_start) * 1000  # ms

            return TradeExecution(
                id=f"error-{int(time.time())}",
                opportunity_id=opportunity.id,
                success=False,
                partial=False,
                error=str(e),
                buy_exchange=opportunity.buy_exchange,
                sell_exchange=opportunity.sell_exchange,
                pair=opportunity.pair,
                execution_timestamp=time.time(),
                execution_time=execution_time,
                profit=0.0,
                volume=0.0,
                strategy=opportunity.strategy,
                executed_buy_price=opportunity.buy_price,
                executed_sell_price=opportunity.sell_price,
                spread=opportunity.spread,
                details={"exception": str(e)},
            )

    async def handle_partial_execution(self, result: TradeExecution) -> None:
        """
        Handle cases where only part of a trade was executed, with risk awareness

        Args:
            result: The result of the partially executed trade
        """
        logger.warning(f"Handling partial execution for {result.opportunity_id}")

        # First let the base execution method handle it
        await self.execution_method.handle_partial_execution(result)

        # Now apply risk-specific handling
        if result.profit < 0:
            # If we lost money on a partial execution, consider adjusting risk factors
            exchange1 = result.buy_exchange
            exchange2 = result.sell_exchange

            # Temporarily reduce risk factors for these exchanges
            if exchange1 in self.risk_management.exchange_risk_factors:
                current = self.risk_management.exchange_risk_factors[exchange1]
                self.risk_management.exchange_risk_factors[exchange1] = max(0.5, current * 0.9)
                logger.info(
                    f"Temporarily reduced risk factor for {exchange1} to {self.risk_management.exchange_risk_factors[exchange1]}"
                )

            if exchange2 in self.risk_management.exchange_risk_factors:
                current = self.risk_management.exchange_risk_factors[exchange2]
                self.risk_management.exchange_risk_factors[exchange2] = max(0.5, current * 0.9)
                logger.info(
                    f"Temporarily reduced risk factor for {exchange2} to {self.risk_management.exchange_risk_factors[exchange2]}"
                )

    async def handle_failure(self, result: TradeExecution) -> None:
        """
        Handle cases where a trade failed to execute, with risk awareness

        Args:
            result: The result of the failed trade
        """
        logger.warning(f"Handling execution failure for {result.opportunity_id}")

        # First let the base execution method handle it
        await self.execution_method.handle_failure(result)

        # Now apply risk-specific handling
        exchange1 = result.buy_exchange
        exchange2 = result.sell_exchange

        # More aggressively reduce risk factors for complete failures
        if exchange1 in self.risk_management.exchange_risk_factors:
            current = self.risk_management.exchange_risk_factors[exchange1]
            self.risk_management.exchange_risk_factors[exchange1] = max(0.25, current * 0.75)
            logger.info(
                f"Reduced risk factor for {exchange1} to {self.risk_management.exchange_risk_factors[exchange1]} after failure"
            )

        if exchange2 in self.risk_management.exchange_risk_factors:
            current = self.risk_management.exchange_risk_factors[exchange2]
            self.risk_management.exchange_risk_factors[exchange2] = max(0.25, current * 0.75)
            logger.info(
                f"Reduced risk factor for {exchange2} to {self.risk_management.exchange_risk_factors[exchange2]} after failure"
            )

    def add_pre_execution_hook(self, hook_function):
        """
        Add a pre-execution hook function

        The hook function should take (opportunity, position_size) and return either:
        - None (to continue execution unchanged)
        - Dict with "abort": True to stop execution
        - Dict with "position_size": new_size to modify position size
        """
        self.pre_execution_hooks.append(hook_function)
        return self  # For chaining

    def add_post_execution_hook(self, hook_function):
        """
        Add a post-execution hook function

        The hook function should take (opportunity, result) and perform any
        necessary analysis or logging.
        """
        self.post_execution_hooks.append(hook_function)
        return self  # For chaining
