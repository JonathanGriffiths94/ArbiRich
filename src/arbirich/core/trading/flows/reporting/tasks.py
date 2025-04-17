# reporting/tasks.py

import asyncio
import logging
import time
from typing import Dict, Optional

import sqlalchemy as sa  # Add this import for the text() function

from src.arbirich.core.trading.flows.reporting.redis_client import get_shared_redis_client
from src.arbirich.services.redis.redis_service import check_redis_health

logger = logging.getLogger(__name__)


async def persist_data():
    """Task for persisting data to the database"""
    from src.arbirich.services.database.database_service import DatabaseService

    try:
        # Verify database connection
        with DatabaseService() as db:
            if db.engine is None:
                logger.error("❌ Database connection not available for persistence task")
                return

            # Log successful database connection
            logger.info("🔌 Database connection successful for persistence task")

            # Check for any pending trade opportunities or executions that need processing
            # This could be implemented by checking Redis for messages that haven't been processed
            redis_client = get_shared_redis_client()

            if redis_client:
                # Check main opportunity and execution channels
                from src.arbirich.constants import TRADE_EXECUTIONS_CHANNEL, TRADE_OPPORTUNITIES_CHANNEL

                pubsub = redis_client.client.pubsub()

                # Log channel subscription check
                channels = [TRADE_OPPORTUNITIES_CHANNEL, TRADE_EXECUTIONS_CHANNEL]

                # Subscribe to channels to check Redis functionality
                for channel in channels:
                    pubsub.subscribe(channel)
                    logger.debug(f"🔄 Subscribed to {channel} for health check")

                # Cleanup after checking
                pubsub.unsubscribe()
                pubsub.close()

        logger.info("✅ Data persistence task completed successfully")
    except Exception as e:
        logger.error(f"❌ Error in data persistence task: {e}", exc_info=True)


async def monitor_health() -> None:
    """Monitor system health"""
    try:
        # Check database connection directly instead of using check_health()
        from src.arbirich.services.database.database_service import DatabaseService

        db_healthy = False
        with DatabaseService() as db:
            try:
                # Test the connection directly
                with db.engine.connect() as conn:
                    # Execute a simple query to verify connection
                    result = conn.execute(sa.text("SELECT 1")).scalar()  # Use sa.text() to create executable SQL
                    db_healthy = result == 1
            except Exception as db_e:
                logger.warning(f"⚠️ Database connection test failed: {db_e}")

        # Check Redis connection
        redis = get_shared_redis_client()
        redis_healthy = False
        if redis:
            try:
                redis_healthy = redis.client.ping()
            except Exception as redis_e:
                logger.warning(f"⚠️ Redis health check failed: {redis_e}")

        # Log any issues
        if not db_healthy or not redis_healthy:
            logger.warning(f"🚦 Health check: DB={db_healthy}, Redis={redis_healthy}")
        else:
            logger.debug("✅ All systems healthy")

    except Exception as e:
        logger.error(f"❌ Health monitoring error: {e}")
        raise


async def report_performance() -> None:
    """Generate performance reports"""
    try:
        # Create simplified version that doesn't rely on the missing method
        from src.arbirich.services.database.database_service import DatabaseService

        # Get basic stats from database
        with DatabaseService() as db:
            # Get strategies
            strategies = db.get_all_strategies()

            # Log basic performance metrics
            for strategy in strategies:
                if hasattr(strategy, "net_profit"):
                    logger.info(f"📊 Strategy {strategy.name}: Net profit = {strategy.net_profit}")

                # Get executions for this strategy
                try:
                    executions = db.get_executions_by_strategy(strategy.name)
                    logger.info(f"📊 Strategy {strategy.name}: {len(executions)} executions")
                except Exception as exec_e:
                    logger.warning(f"⚠️ Error getting executions for {strategy.name}: {exec_e}")

        logger.debug("✅ Performance reporting completed")
    except Exception as e:
        logger.error(f"❌ Performance reporting error: {e}")
        raise


async def process_redis_messages(pubsub, redis_client, active, stop_event, debug_mode=False) -> None:
    """
    Process messages from Redis PubSub channels.
    This replaces the Bytewax flow functionality with pure asyncio.
    """
    if not pubsub:
        logger.error("❌ Redis PubSub not initialized")
        return

    # Initialize tracking variables
    last_activity = time.time()
    next_log_time = time.time() + 300  # First log after 5 minutes

    logger.info("🚀 Starting Redis message processing loop")

    try:
        while active and not stop_event.is_set():
            current_time = time.time()

            # Periodic health check
            if current_time - last_activity > 60:  # 1 minute timeout
                logger.info("🔍 Performing Redis health check")
                last_activity = current_time

                channels = []  # Get channels from pubsub if possible

                # Only perform health check if redis_client is not None
                if redis_client:
                    if not await check_redis_health(redis_client, pubsub, channels):
                        # Wait a bit before trying again
                        await asyncio.sleep(5.0)
                        continue
                else:
                    logger.warning("⚠️ Redis client is None, skipping health check")
                    redis_client = get_shared_redis_client()
                    # If still None, wait a bit before continuing
                    if not redis_client:
                        await asyncio.sleep(5.0)
                        continue

            # Periodic logging to show we're alive
            if current_time > next_log_time:
                logger.debug("✅ Reporting component is active and waiting for messages")
                next_log_time = current_time + 300  # Log every 5 minutes

            # Get the next message with a timeout
            message = await get_next_message(pubsub)

            if message is None:
                continue

            # Process the message - FIX FOR TYPE ERROR
            # Get channel and handle different types properly
            channel = message.get("channel", "")
            if isinstance(channel, bytes):
                channel = channel.decode("utf-8")
            elif not isinstance(channel, str):
                # Convert to string if it's neither bytes nor string
                channel = str(channel)

            data = message.get("data")

            if isinstance(data, bytes):
                try:
                    # Process binary data - often JSON
                    import json

                    data = json.loads(data.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    # If not JSON, just keep as string
                    data = data.decode("utf-8", errors="replace")

            # Skip subscription confirmation messages
            if isinstance(data, (int, str)) and data in (1, 2, "1", "2"):
                continue

            # Process the message using the process_message function
            # Import inside the function to avoid circular imports
            from .message_processor import process_message

            result = await process_message(channel, data)

            # Update activity timestamp
            last_activity = time.time()

            # Debug output similar to Bytewax inspect
            if debug_mode and result:
                logger.debug(f"🔄 Processed message from {channel}: {result}")

    except asyncio.TimeoutError:
        # This is expected during normal operation
        pass
    except asyncio.CancelledError:
        logger.info("❌ Redis message processing cancelled")
        raise
    except Exception as e:
        logger.error(f"❌ Error processing Redis messages: {e}", exc_info=True)
        raise


async def get_next_message(pubsub) -> Optional[Dict]:
    """Get the next message from Redis PubSub with async support"""
    try:
        # Check if we have a message available
        message = pubsub.get_message(timeout=0.1)

        # If we got a message, return it immediately
        if message:
            return message

        # If no message is available, wait a brief moment before returning None
        # This avoids excessive CPU usage while still being responsive
        await asyncio.sleep(0.01)
        return None
    except Exception as e:
        logger.error(f"Error getting message from Redis PubSub: {e}")
        return None
