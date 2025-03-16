import logging
import time

from src.arbirich.models.models import TradeExecution, TradeOpportunity
from src.arbirich.services.redis_service import RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

redis_client = RedisService()


def execute_trade(opportunity_raw: dict) -> dict:
    """
    Convert raw trade opportunity data into a TradeOpportunity model,
    simulate a trade execution, and return a TradeExecution model (as dict).
    """
    try:
        opp = TradeOpportunity(**opportunity_raw)
    except Exception as e:
        logger.error(f"Error parsing trade opportunity: {e}")
        return {}

    # Simulate trade execution (for example, call an exchange API here).
    execution_ts = time.time()

    trade_exec = TradeExecution(
        strategy=opp.strategy,
        pair=opp.pair,
        buy_exchange=opp.buy_exchange,
        sell_exchange=opp.sell_exchange,
        executed_buy_price=opp.buy_price,
        executed_sell_price=opp.sell_price,
        spread=opp.spread,
        volume=opp.volume,
        execution_timestamp=execution_ts,
        opportunity_id=opp.id,
    )

    trade_msg = (
        f"Executed trade for {trade_exec.pair}: "
        f"Buy from {trade_exec.buy_exchange} at {trade_exec.executed_buy_price}, "
        f"Sell on {trade_exec.sell_exchange} at {trade_exec.executed_sell_price}, "
        f"Spread: {trade_exec.spread:.4f}, Volume: {trade_exec.volume}, "
        f"Timestamp: {trade_exec.execution_timestamp}"
    )
    logger.critical(trade_msg)

    try:
        # Publish or store the trade execution in Redis (as an example).
        redis_client.publish_trade_execution(trade_exec)
        logger.debug("Trade execution stored successfully in Redis.")
    except Exception as e:
        logger.error(f"Error storing trade execution: {e}")

    return trade_exec.model_dump()  # Return as a dict for downstream serialization.
