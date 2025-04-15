import argparse
import sys
import time
import uuid
from pathlib import Path

# Add the project root to the Python path BEFORE any project imports
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

# Now import project modules
import redis

from src.arbirich.constants import TRADE_EXECUTIONS_CHANNEL, TRADE_OPPORTUNITIES_CHANNEL
from src.arbirich.models.models import TradeExecution, TradeOpportunity
from src.arbirich.services.redis.redis_channel_manager import get_channel_manager
from src.arbirich.services.redis.redis_service import get_shared_redis_client


def generate_opportunity():
    """Generate a sample trade opportunity following the TradeOpportunity model"""
    opportunity_id = str(uuid.uuid4())
    # Use Unix timestamp as the model expects
    current_timestamp = time.time()

    return TradeOpportunity(
        id=opportunity_id,
        strategy="basic_arbitrage",
        pair="LINK-USDT",
        buy_exchange="cryptocom",  # Changed from bybit to cryptocom
        sell_exchange="bybit",  # Changed from cryptocom to bybit
        buy_price=13.206,  # Updated price to match example
        sell_price=13.21,  # Updated price to match example
        spread=0.000302892624564693,  # Updated spread to match example
        volume=35.76,  # Updated volume to match example
        opportunity_timestamp=current_timestamp,
    )


def generate_execution(opportunity_id=None):
    """Generate a sample trade execution following the TradeExecution model"""
    if not opportunity_id:
        opportunity_id = str(uuid.uuid4())

    execution_id = str(uuid.uuid4())
    # Use Unix timestamp as the model expects
    current_timestamp = time.time()

    return TradeExecution(
        id=execution_id,
        strategy="basic_arbitrage",
        pair="LINK-USDT",
        buy_exchange="cryptocom",  # Changed from bybit to cryptocom
        sell_exchange="bybit",  # Changed from cryptocom to bybit
        executed_buy_price=13.219206,  # Updated price to match example
        executed_sell_price=13.19679,  # Updated price to match example
        spread=0.000302892624564693,  # Updated spread to match example
        volume=35.76,  # Updated volume to match example
        execution_timestamp=current_timestamp,
        execution_id=None,  # Set to null as in example
        opportunity_id=opportunity_id,
    )


def main():
    parser = argparse.ArgumentParser(description="Inject test messages into Redis")
    parser.add_argument("--host", default="localhost", help="Redis host")
    parser.add_argument("--port", type=int, default=6379, help="Redis port")
    parser.add_argument("--db", type=int, default=0, help="Redis database")
    parser.add_argument(
        "--type", choices=["opportunity", "execution", "both"], default="both", help="Type of message to inject"
    )
    parser.add_argument("--count", type=int, default=1, help="Number of messages to inject")
    parser.add_argument("--sql-mode", action="store_true", help="Use SQL compatible timestamp format")

    args = parser.parse_args()

    # Initialize Redis client and channel manager
    redis_client = get_shared_redis_client() or redis.Redis(host=args.host, port=args.port, db=args.db)
    channel_manager = get_channel_manager()

    if not channel_manager:
        print("Failed to get channel manager, creating one directly...")
        from src.arbirich.services.redis.redis_channel_manager import RedisChannelManager

        channel_manager = RedisChannelManager(redis_client)

    print(f"Connected to Redis at {args.host}:{args.port}")
    print(f"Using channel names: {TRADE_OPPORTUNITIES_CHANNEL}, {TRADE_EXECUTIONS_CHANNEL}")

    for i in range(args.count):
        if args.type in ["opportunity", "both"]:
            opportunity = generate_opportunity()
            # Use channel manager to publish (keep original numeric timestamps)
            result = channel_manager.publish_opportunity(opportunity)
            print(f"Published opportunity: {opportunity.id} (received by {result} subscribers)")

        if args.type in ["execution", "both"]:
            # For "both", use the same opportunity ID
            opportunity_id = opportunity.id if args.type == "both" else None
            execution = generate_execution(opportunity_id)
            # Use channel manager to publish (keep original numeric timestamps)
            result = channel_manager.publish_execution(execution)
            print(f"Published execution: {execution.id} (received by {result} subscribers)")

    print("Done!")


if __name__ == "__main__":
    main()
