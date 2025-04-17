import asyncio
import logging
import time
import uuid
from typing import Optional

from src.arbirich.models import TradeExecution, TradeOpportunity
from src.arbirich.services.execution.execution_service import ExecutionService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def log_opportunity(opportunity: TradeOpportunity) -> TradeOpportunity:
    """Log opportunity for debugging"""
    if not opportunity:
        logger.warning("Attempting to log null opportunity")
        return opportunity

    logger.info(f"🔎 OPPORTUNITY: {opportunity.id} - {opportunity.pair}")
    # Add more detailed logging
    logger.debug(f"  Strategy: {opportunity.strategy}")
    logger.debug(f"  Buy: {opportunity.buy_exchange} @ {opportunity.buy_price}")
    logger.debug(f"  Sell: {opportunity.sell_exchange} @ {opportunity.sell_price}")
    logger.debug(f"  Spread: {opportunity.spread:.6%}")
    logger.debug(f"  Volume: {opportunity.volume}")
    logger.debug(f"  Est. Profit: {opportunity.spread * opportunity.volume * opportunity.buy_price:.8f}")
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
    Execute a trade based on an opportunity using the execution service.

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

    try:
        # Get execution service instance
        execution_service = ExecutionService.get_instance()

        # Create a new event loop if one doesn't exist in this thread
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            # No event loop exists in this thread, create a new one
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            logger.info("Created new event loop for execution thread")

        # Initialize the service if not already done
        if not hasattr(execution_service, "_initialized") or not execution_service._initialized:
            logger.info(f"Initializing execution service for opportunity: {opportunity.id}")
            loop.run_until_complete(execution_service.initialize())

        # Execute the trade using the execution service
        logger.info(f"⚙️ Executing trade using execution service for strategy: {effective_strategy_name}")

        # Execute the trade using the loop
        execution = loop.run_until_complete(execution_service.execute_trade(opportunity))

        if not execution:
            logger.error("Execution service returned None")
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

        # Create an error execution object for consistent return type
        error_execution = TradeExecution(
            id=str(uuid.uuid4()),
            strategy=effective_strategy_name,
            pair=opportunity.pair,
            buy_exchange=opportunity.buy_exchange,
            sell_exchange=opportunity.sell_exchange,
            executed_buy_price=opportunity.buy_price,
            executed_sell_price=opportunity.sell_price,
            spread=opportunity.spread,
            volume=opportunity.volume,
            execution_timestamp=time.time(),
            success=False,
            partial=False,
            profit=0.0,
            execution_time=time.time() - start_time,
            error=str(e),
            opportunity_id=opportunity.id,
            details={"error": str(e)},
        )

        return error_execution
