import logging
import time
import uuid

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


def execute_trade(opportunity: TradeOpportunity, strategy_name: str) -> TradeExecution:
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

    # Execute the trade using the strategy
    execution_id = str(uuid.uuid4())
    execution_ts = time.time()

    execution = strategy.execute_trade(
        opportunity=opportunity,
        execution_id=execution_id,
        execution_ts=execution_ts,
    )

    # Calculate profit
    buy_cost = execution.executed_buy_price * execution.volume
    sell_revenue = execution.executed_sell_price * execution.volume
    execution.profit = sell_revenue - buy_cost

    # Set execution time
    execution_time = time.time() - start_time
    execution.execution_time = execution_time

    profit_emoji = "💰" if execution.profit > 0 else "📉"
    logger.info(
        f"✅ Trade execution complete: {execution.id} with profit: {profit_emoji} {execution.profit}, ⏱️ time taken: {execution_time:.4f}s"
    )
    return execution
