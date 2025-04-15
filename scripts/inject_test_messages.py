import argparse
import json
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


def generate_opportunity(strategy_name):
    """Generate a sample trade opportunity following the TradeOpportunity model"""
    opportunity_id = str(uuid.uuid4())
    # Use Unix timestamp as the model expects
    current_timestamp = time.time()

    return TradeOpportunity(
        id=opportunity_id,
        strategy=strategy_name,
        pair="LINK-USDT",
        buy_exchange="cryptocom",  # Changed from bybit to cryptocom
        sell_exchange="bybit",  # Changed from cryptocom to bybit
        buy_price=13.206,  # Updated price to match example
        sell_price=13.21,  # Updated price to match example
        spread=0.000302892624564693,  # Updated spread to match example
        volume=35.76,  # Updated volume to match example
        opportunity_timestamp=current_timestamp,
    )


def generate_execution(strategy_name, opportunity_id=None):
    """Generate a sample trade execution following the TradeExecution model"""
    if not opportunity_id:
        opportunity_id = str(uuid.uuid4())

    execution_id = str(uuid.uuid4())
    # Use Unix timestamp as the model expects
    current_timestamp = time.time()

    return TradeExecution(
        id=execution_id,
        strategy=strategy_name,
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


def publish_with_logs(channel_manager: TradeOpportunity, opportunity, use_direct=False, redis_client=None):
    """Publish opportunity with detailed logging about channels"""
    # Main opportunity channel
    main_channel = TRADE_OPPORTUNITIES_CHANNEL

    # Strategy-specific channel
    strategy_channel = f"{TRADE_OPPORTUNITIES_CHANNEL}:{opportunity.strategy}"

    if use_direct and redis_client:
        # Direct publishing to Redis (bypassing channel manager)
        # Convert opportunity to JSON
        opportunity_dict = opportunity.model_dump() if hasattr(opportunity, "model_dump") else opportunity.dict()
        opportunity_json = json.dumps(opportunity_dict)

        # Publish to both channels
        main_result = redis_client.publish(main_channel, opportunity_json)
        strategy_result = redis_client.publish(strategy_channel, opportunity_json)

        print(f"Published opportunity: {opportunity.id} directly to Redis:")
        print(f"  - Main channel: {main_channel} (received by {main_result} subscribers)")
        print(f"  - Strategy channel: {strategy_channel} (received by {strategy_result} subscribers)")
        return main_result + strategy_result
    else:
        # Use channel manager to publish (it handles both channels)
        result = channel_manager.publish_opportunity(opportunity)

        print(f"Published opportunity: {opportunity.id} to channels:")
        print(f"  - Main channel: {main_channel}")
        print(f"  - Strategy channel: {strategy_channel}")
        print(f"  - Received by {result} subscribers")
        return result


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
    parser.add_argument("--strategy", default="basic_arbitrage", help="Strategy name to use for messages")
    # New arguments
    parser.add_argument(
        "--startup-delay",
        type=float,
        default=0,
        help="Delay in seconds before publishing (to allow subscribers to connect)",
    )
    parser.add_argument("--interval", type=float, default=0, help="Interval between messages in seconds (0 = no delay)")
    parser.add_argument("--loop", action="store_true", help="Run in continuous loop mode (use with interval)")
    parser.add_argument("--direct", action="store_true", help="Publish directly to Redis (bypass channel manager)")
    parser.add_argument("--verbose", action="store_true", help="Print detailed debugging information")

    args = parser.parse_args()

    # Initialize Redis client and correctly handle RedisService
    redis_client = None
    try:
        redis_service = get_shared_redis_client()
        if redis_service:
            # When we get a RedisService object
            if hasattr(redis_service, "is_healthy"):
                if redis_service.is_healthy():
                    # Use the underlying Redis client for direct operations
                    redis_client = redis_service.client
                    print("Using Redis client from RedisService")
                else:
                    raise Exception("Redis service reports unhealthy connection")
            else:
                # Assume it's a direct Redis client
                redis_client = redis_service
                if hasattr(redis_client, "ping"):
                    redis_client.ping()  # Test connection directly

        # If we still don't have a client, create one directly
        if not redis_client:
            redis_client = redis.Redis(host=args.host, port=args.port, db=args.db, decode_responses=False)
            # Test connection
            redis_client.ping()
    except Exception as e:
        print(f"Error connecting to Redis: {e}")
        print("Trying to create a direct Redis client instead...")
        try:
            redis_client = redis.Redis(host=args.host, port=args.port, db=args.db, decode_responses=False)
            redis_client.ping()
        except Exception as e2:
            print(f"Failed to create direct Redis client: {e2}")
            sys.exit(1)

    # Initialize channel manager
    channel_manager = get_channel_manager()
    if not channel_manager and not args.direct:
        print("Failed to get channel manager, creating one directly...")
        from src.arbirich.services.redis.redis_channel_manager import RedisChannelManager

        channel_manager = RedisChannelManager(redis_client)

    # Extract host, port from client info for display purposes
    host = args.host
    port = args.port
    db = args.db
    try:
        if hasattr(redis_client, "connection_pool") and hasattr(redis_client.connection_pool, "connection_kwargs"):
            connection_kwargs = redis_client.connection_pool.connection_kwargs
            host = connection_kwargs.get("host", args.host)
            port = connection_kwargs.get("port", args.port)
            db = connection_kwargs.get("db", args.db)
    except Exception:
        # Use defaults if there's any error extracting connection info
        pass

    print(f"Connected to Redis at {host}:{port} (database {db})")
    print(f"Using strategy: {args.strategy}")
    print(f"Base channel names: {TRADE_OPPORTUNITIES_CHANNEL}, {TRADE_EXECUTIONS_CHANNEL}")
    print(f"Strategy-specific channel: {TRADE_OPPORTUNITIES_CHANNEL}:{args.strategy}")

    # Show subscriber info
    if args.verbose:
        try:
            # Get subscriber counts for both channels
            subscribers = redis_client.pubsub_numsub(
                TRADE_OPPORTUNITIES_CHANNEL, f"{TRADE_OPPORTUNITIES_CHANNEL}:{args.strategy}"
            )

            print("Current subscribers:")
            for i, channel in enumerate(subscribers[::2]):
                count = subscribers[i * 2 + 1]
                channel_name = channel.decode("utf-8") if isinstance(channel, bytes) else channel
                print(f"  - {channel_name}: {count} subscribers")

        except Exception as e:
            print(f"Error getting subscriber info: {e}")

    if args.startup_delay > 0:
        print(f"Waiting {args.startup_delay} seconds before publishing to allow subscribers to connect...")
        time.sleep(args.startup_delay)

    iteration = 0
    try:
        while True:
            iteration += 1
            if args.loop:
                print(f"\n--- Publishing iteration {iteration} ---")

            for i in range(args.count):
                if args.type in ["opportunity", "both"]:
                    opportunity = generate_opportunity(args.strategy)
                    # Use our helper function with better logging
                    publish_with_logs(channel_manager, opportunity, args.direct, redis_client)

                if args.type in ["execution", "both"]:
                    # For "both", use the same opportunity ID
                    opportunity_id = opportunity.id if args.type == "both" and "opportunity" in locals() else None
                    execution = generate_execution(args.strategy, opportunity_id)
                    # Use channel manager to publish
                    if args.direct and redis_client:
                        # Direct publishing
                        execution_dict = (
                            execution.model_dump() if hasattr(execution, "model_dump") else execution.dict()
                        )
                        execution_json = json.dumps(execution_dict)
                        result = redis_client.publish(TRADE_EXECUTIONS_CHANNEL, execution_json)
                        print(
                            f"Published execution: {execution.id} directly to Redis (received by {result} subscribers)"
                        )
                    else:
                        result = channel_manager.publish_execution(execution)
                        print(f"Published execution: {execution.id} (received by {result} subscribers)")

                # Add delay between messages if specified
                if args.interval > 0 and i < args.count - 1:
                    time.sleep(args.interval)

            # Break if not in loop mode
            if not args.loop:
                break

            # Add delay between iterations
            if args.interval > 0:
                time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\nExiting on keyboard interrupt...")

    print("Done!")


if __name__ == "__main__":
    main()
