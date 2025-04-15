import logging

from src.arbirich.core.trading.flows.bytewax_flows.common.redis_utils import get_redis_client
from src.arbirich.models.enums import ChannelName
from src.arbirich.models.models import TradeExecution

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Redis service for publishing executions
redis_client = None


def get_execution_redis_client():
    """Get the Redis client for execution sink"""
    global redis_client
    if redis_client is None:
        redis_client = get_redis_client("execution_sink")
    return redis_client


def publish_execution(execution: TradeExecution) -> int:
    """
    Publish a trade execution to Redis.

    Args:
        execution: The trade execution to publish

    Returns:
        int: The number of clients that received the message
    """
    logger.info(f"📤 Publishing execution {execution.id} to Redis")

    # Get Redis client
    client = get_execution_redis_client()

    # Create channel name using the enum
    strategy_name = execution.strategy
    channel = f"{ChannelName.TRADE_EXECUTIONS.value}:{strategy_name}"

    # Publish using Redis client
    result = client.publish(channel, execution.json())

    logger.info(f"📢 Execution {execution.id} published to channel {channel}, received by {result} clients")
    return result
