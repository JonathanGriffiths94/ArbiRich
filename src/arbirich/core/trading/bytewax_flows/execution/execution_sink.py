import logging
import time

from src.arbirich.models.models import TradeExecution, TradeOpportunity
from src.arbirich.services.redis.redis_service import TRADE_EXECUTIONS_CHANNEL, RedisService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

redis_client = RedisService()


def execute_trade(opportunity_raw: dict) -> dict:
    """
    Convert raw trade opportunity data into a TradeOpportunity model,
    execute a trade, and return a TradeExecution model (as dict).
    """
    try:
        opp = TradeOpportunity(**opportunity_raw)
    except Exception as e:
        logger.error(f"Error parsing trade opportunity: {e}")
        return {}

    # Execute the trade by calling exchange API here
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
    logger.info(f"Created execution with opportunity_id={trade_exec.opportunity_id}, execution_id={trade_exec.id}")

    trade_msg = (
        f"Executed trade for {trade_exec.pair}: "
        f"Buy from {trade_exec.buy_exchange} at {trade_exec.executed_buy_price}, "
        f"Sell on {trade_exec.sell_exchange} at {trade_exec.executed_sell_price}, "
        f"Spread: {trade_exec.spread:.4f}, Volume: {trade_exec.volume}, "
        f"Timestamp: {trade_exec.execution_timestamp}"
    )
    logger.critical(trade_msg)

    try:
        # Publish or store the trade execution in Redis, explicitly using the channel
        strategy_name = trade_exec.strategy
        channel = f"{TRADE_EXECUTIONS_CHANNEL}:{strategy_name}" if strategy_name else TRADE_EXECUTIONS_CHANNEL

        # Publish execution
        logger.info(f"About to publish trade execution {trade_exec.id} to channel {channel}")
        redis_client.publish_trade_execution(trade_exec, strategy_name)
        logger.info(f"Trade execution published to {channel}")
    except Exception as e:
        logger.error(f"Error storing trade execution: {e}")

    return trade_exec.model_dump()  # Return as a dict for downstream serialization.
