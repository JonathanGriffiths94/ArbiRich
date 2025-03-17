import asyncio
import logging
from typing import Dict, Optional

from src.arbirich.execution_strategies.execution_strategy_factory import ExecutionStrategyFactory
from src.arbirich.models.models import TradeExecution, TradeOpportunity
from src.arbirich.services.redis_service import RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

redis_client = RedisService()


async def execute_trade_with_strategy(opportunity: TradeOpportunity) -> Optional[TradeExecution]:
    """
    Execute a trade using the appropriate execution strategy.

    Parameters:
        opportunity: The trade opportunity to execute

    Returns:
        TradeExecution if successful, None if execution failed
    """
    try:
        # Get the appropriate execution strategy
        strategy_name = opportunity.strategy
        execution_strategy = ExecutionStrategyFactory.get_strategy_for_arbitrage(strategy_name)

        if not execution_strategy:
            logger.error(f"No execution strategy available for {strategy_name}")
            return None

        # Check if we should execute this opportunity
        if not execution_strategy.should_execute(opportunity):
            logger.info(f"Opportunity {opportunity.id} does not meet execution criteria")
            return None

        # Execute the trade using the selected strategy
        logger.info(f"Executing trade for {opportunity.pair} using {execution_strategy.name} strategy")
        execution_result = await execution_strategy.execute_trade(opportunity)

        if execution_result:
            trade_msg = (
                f"Executed trade for {execution_result.pair}: "
                f"Buy from {execution_result.buy_exchange} at {execution_result.executed_buy_price}, "
                f"Sell on {execution_result.sell_exchange} at {execution_result.executed_sell_price}, "
                f"Spread: {execution_result.spread:.4%}, Volume: {execution_result.volume}"
            )
            logger.critical(trade_msg)

            # Publish or store the trade execution in Redis
            try:
                redis_client.publish_trade_execution(execution_result, strategy_name)
                logger.debug("Trade execution stored successfully in Redis.")
            except Exception as e:
                logger.error(f"Error storing trade execution: {e}")

            return execution_result
        else:
            logger.warning(f"Trade execution failed for opportunity {opportunity.id}")
            return None

    except Exception as e:
        logger.error(f"Error in execute_trade_with_strategy: {e}", exc_info=True)
        return None


def execute_trade(opportunity_raw: Dict) -> Optional[Dict]:
    """
    Convert raw trade opportunity data into a TradeOpportunity model,
    execute a trade using the appropriate strategy, and return a TradeExecution model (as dict).

    This function is compatible with bytewax's operators.map.

    Parameters:
        opportunity_raw: The raw trade opportunity data

    Returns:
        TradeExecution as dict if successful, empty dict if execution failed
    """
    try:
        # Convert raw data to TradeOpportunity model
        try:
            opportunity = TradeOpportunity(**opportunity_raw)
        except Exception as e:
            logger.error(f"Error parsing trade opportunity: {e}")
            return {}

        # Create a new event loop for async execution
        loop = asyncio.new_event_loop()
        try:
            # Execute trade using the appropriate strategy
            execution_result = loop.run_until_complete(execute_trade_with_strategy(opportunity))

            # Return as dict for downstream serialization if successful
            if execution_result:
                return execution_result.model_dump()
            else:
                return {}
        finally:
            loop.close()

    except Exception as e:
        logger.error(f"Error in execute_trade: {e}", exc_info=True)
        return {}
