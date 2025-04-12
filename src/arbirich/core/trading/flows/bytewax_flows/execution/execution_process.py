import logging
import time
import uuid
from typing import Dict, Optional

from src.arbirich.models.models import TradeExecution
from src.arbirich.services.database.database_service import DatabaseService
from src.arbirich.services.redis.redis_channel_manager import RedisChannelManager
from src.arbirich.services.redis.redis_service import get_shared_redis_client

logger = logging.getLogger(__name__)

# Use a shared Redis service instance
_redis_service = None


def get_redis():
    """Get shared Redis service"""
    global _redis_service
    if _redis_service is None:
        _redis_service = get_shared_redis_client()
    return _redis_service


def log_opportunity(opportunity):
    """Log opportunity for debugging"""
    try:
        logger.info(f"OPPORTUNITY: {opportunity.get('id', 'unknown')} - {opportunity.get('pair', 'unknown')}")
    except Exception as e:
        logger.error(f"Error logging opportunity: {e}")
    return opportunity


def filter_for_strategy(opportunity, strategy_name):
    """Filter opportunities by strategy name"""
    try:
        # Check strategy field in the opportunity
        if isinstance(opportunity, dict):
            opp_strategy = opportunity.get("strategy")
        else:
            opp_strategy = getattr(opportunity, "strategy", None)

        # If the opportunity has no strategy, allow it to pass through
        if not opp_strategy:
            return True

        # Otherwise only let matching strategies through
        return opp_strategy == strategy_name
    except Exception as e:
        logger.error(f"Error in filter_for_strategy: {e}")
        return False


def execute_trade(opportunity) -> Optional[Dict]:
    """
    Execute a trade based on an opportunity.

    This is a placeholder implementation that simulates trade execution.
    In a real implementation, this would interact with exchange APIs.

    Args:
        opportunity: The opportunity to execute

    Returns:
        Trade execution result or None if execution failed
    """
    try:
        if not opportunity:
            logger.warning("Received empty opportunity")
            return None

        logger.info(f"Starting trade execution for opportunity: {opportunity.get('id', 'unknown')}")
        logger.info(
            f"Trade details: {opportunity.get('pair', 'unknown')} - Buy: {opportunity.get('buy_exchange', 'unknown')} @ {opportunity.get('buy_price')}, Sell: {opportunity.get('sell_exchange', 'unknown')} @ {opportunity.get('sell_price')}"
        )

        # Get opportunity details
        opp_id = opportunity.get("id")
        if not opp_id:
            logger.warning("Opportunity missing ID, generating a new one")
            opp_id = str(uuid.uuid4())

        strategy = opportunity.get("strategy", "unknown")
        pair = opportunity.get("pair", "unknown")
        buy_exchange = opportunity.get("buy_exchange", "unknown")
        sell_exchange = opportunity.get("sell_exchange", "unknown")
        buy_price = float(opportunity.get("buy_price", 0))
        sell_price = float(opportunity.get("sell_price", 0))
        spread = float(opportunity.get("spread", 0))
        volume = float(opportunity.get("volume", 0))

        logger.info(f"Calculated spread: {spread:.4f}%, Volume: {volume}")

        # In a real implementation, this would place orders on exchanges
        # For now, just simulate a successful execution with minimal slippage
        executed_buy_price = buy_price * 1.001  # Simulate 0.1% slippage
        executed_sell_price = sell_price * 0.999  # Simulate 0.1% slippage

        logger.info(f"Simulated execution - Buy price: {executed_buy_price}, Sell price: {executed_sell_price}")

        # Calculate expected profit
        estimated_profit = (executed_sell_price - executed_buy_price) * volume
        logger.info(f"Estimated profit: ${estimated_profit:.4f}")

        # Create execution record
        execution = TradeExecution(
            id=str(uuid.uuid4()),
            strategy=strategy,
            pair=pair,
            buy_exchange=buy_exchange,
            sell_exchange=sell_exchange,
            executed_buy_price=executed_buy_price,
            executed_sell_price=executed_sell_price,
            spread=spread,
            volume=volume,
            execution_timestamp=time.time(),
            opportunity_id=opp_id,
        )

        logger.info(f"Created execution record with ID: {execution.id}")

        # Save execution to database
        try:
            with DatabaseService() as db:
                saved_execution = db.create_trade_execution(execution)
                logger.info(f"Successfully saved execution {execution.id} to database")
        except Exception as db_error:
            logger.error(f"Database error saving execution: {db_error}", exc_info=True)
            return None

        # Publish execution to Redis
        try:
            redis = get_redis()
            channel_manager = RedisChannelManager(redis)
            subscribers = channel_manager.publish_execution(execution)
            logger.info(f"Published execution to {subscribers} subscribers")
        except Exception as redis_error:
            logger.error(f"Redis error publishing execution: {redis_error}", exc_info=True)
            # Don't return None here - we already saved to DB

        # Return the execution
        logger.info(f"Trade execution complete: {execution.id}")
        return execution.model_dump()

    except Exception as e:
        logger.error(f"Error executing trade: {e}", exc_info=True)
        return None
