import inspect
import logging
import time
import uuid
from typing import Optional

from src.arbirich.core.trading.strategy.strategy_factory import get_strategy
from src.arbirich.models.models import TradeExecution, TradeOpportunity

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def log_opportunity(opportunity: TradeOpportunity) -> TradeOpportunity:
    """Log opportunity for debugging"""
    logger.info(f"🔎 OPPORTUNITY: {opportunity.id} - {opportunity.pair}")
    return opportunity


def filter_for_strategy(opportunity: TradeOpportunity, strategy_name: str) -> bool:
    """Filter opportunities by strategy name"""
    # Get the strategy from the opportunity
    opp_strategy = opportunity.strategy if hasattr(opportunity, "strategy") else None

    # If the opportunity has no strategy, allow it to pass through
    if not opp_strategy:
        return True

    # Otherwise only let matching strategies through
    return opp_strategy == strategy_name


def execute_trade(opportunity: TradeOpportunity, strategy_name: str) -> Optional[TradeExecution]:
    """
    Execute a trade based on an opportunity.

    Args:
        opportunity: The opportunity to execute
        strategy_name: The strategy to use for execution

    Returns:
        TradeExecution: The executed trade result
    """
    start_time = time.time()
    logger.info(f"🚀 Starting trade execution for opportunity: {opportunity.id}")

    # Use the provided strategy_name or get it from opportunity
    effective_strategy_name = strategy_name or opportunity.strategy

    # Get the strategy implementation
    strategy = get_strategy(effective_strategy_name)
    logger.info(f"⚙️ Executing trade using strategy: {effective_strategy_name}")

    try:
        # Inspect the signature of the execute_trade method
        sig = inspect.signature(strategy.execute_trade)
        parameters = list(sig.parameters.keys())

        logger.info(f"Strategy execute_trade signature: {parameters}")

        # Base implementation expects (self, opportunity, position_size=None)
        execution = None

        if "execution_id" in parameters and "execution_ts" in parameters:
            # New style with execution_id and execution_ts
            execution_id = str(uuid.uuid4())
            execution_ts = time.time()

            logger.info("Using extended signature with execution_id and execution_ts")
            execution = strategy.execute_trade(
                opportunity=opportunity,
                execution_id=execution_id,
                execution_ts=execution_ts,
            )
        elif "position_size" in parameters:
            # Base implementation with position_size
            logger.info("Using base signature with position_size")
            execution = strategy.execute_trade(
                opportunity=opportunity,
                position_size=None,  # Use default calculation in the strategy
            )
        else:
            # Minimal implementation with just opportunity
            logger.info("Using minimal signature with opportunity only")
            execution = strategy.execute_trade(opportunity=opportunity)

        if not execution:
            logger.error("Strategy execution returned None")
            return None

        # Calculate profit if not already set
        if hasattr(execution, "profit") and execution.profit == 0:
            buy_cost = execution.executed_buy_price * execution.volume
            sell_revenue = execution.executed_sell_price * execution.volume
            execution.profit = sell_revenue - buy_cost

        # Set execution time if not already set
        execution_time = time.time() - start_time
        if hasattr(execution, "execution_time") and execution.execution_time == 0:
            execution.execution_time = execution_time

        profit_emoji = "💰" if execution.profit > 0 else "📉"
        logger.info(
            f"✅ Trade execution complete: {execution.id} with profit: {profit_emoji} {execution.profit}, ⏱️ time taken: {execution_time:.4f}s"
        )
        return execution

    except Exception as e:
        logger.error(f"❌ Error executing trade: {e}", exc_info=True)
        return None
