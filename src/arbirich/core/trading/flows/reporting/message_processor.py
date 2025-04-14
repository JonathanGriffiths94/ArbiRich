import asyncio
import json
import logging
import traceback
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict

from arbirich.core.state.system_state import is_system_shutting_down
from src.arbirich.models.models import TradeExecution, TradeOpportunity
from src.arbirich.services.database.database_service import DatabaseService

from .db_functions import check_opportunity_exists

logger = logging.getLogger(__name__)

# Create a unique processor instance ID for tracking in logs
PROCESSOR_ID = str(uuid.uuid4())[:8]


# Add a function that dynamically imports and forwards to process_redis_messages
async def process_redis_messages(pubsub, redis_client, active, stop_event, debug_mode=False):
    """
    Forward to the process_redis_messages function in tasks.py.
    This avoids circular imports by dynamically importing at runtime.
    """
    # Dynamically import the function from tasks when needed
    from src.arbirich.core.trading.flows.reporting.tasks import process_redis_messages as _process_redis_messages

    # Forward the call
    return await _process_redis_messages(pubsub, redis_client, active, stop_event, debug_mode)


async def process_message(channel: str, data: Any) -> Dict[str, Any]:
    """
    Process a message from Redis.
    This implements the db_sink functionality from the original code.
    """
    # Very first thing - check system shutdown state
    if is_system_shutting_down():
        logger.info("System is shutting down - skipping message processing")
        return {"processed": False, "reason": "system_shutting_down"}

    try:
        logger.info(f"[Processor-{PROCESSOR_ID}] Processing message from channel: {channel}")

        if not data:
            logger.debug("Received empty data, skipping")
            return {}

        # Check shutdown state again - don't process new messages during shutdown
        if is_system_shutting_down():
            logger.info("System is shutting down - skipping message processing")
            return {"processed": False, "reason": "system_shutting_down"}

        # Parse the data based on its type
        parsed_data = None
        if isinstance(data, str):
            # If it's a string, try to parse as JSON
            try:
                parsed_data = json.loads(data)
            except json.JSONDecodeError:
                logger.error(f"Failed to parse data as JSON: {data[:100]}...")
                return {"processed": False, "reason": "invalid_json"}
        elif isinstance(data, dict):
            # If it's already a dict, use it directly
            parsed_data = data
        else:
            logger.error(f"Unsupported data type: {type(data)}")
            return {}

        # Log received data
        if isinstance(parsed_data, dict):
            logger.info(
                f"[Processor-{PROCESSOR_ID}] Received message with ID: {parsed_data.get('id')} and fields: {list(parsed_data.keys())}"
            )
            if "opportunity_id" in parsed_data:
                logger.info(
                    f"[Processor-{PROCESSOR_ID}] Message has opportunity_id: {parsed_data['opportunity_id']} (should be processed as execution)"
                )
            if "strategy" in parsed_data:
                logger.info(f"[Processor-{PROCESSOR_ID}] Message has strategy: {parsed_data['strategy']}")

        # FIRST check if it's an opportunity and process it immediately
        if (
            isinstance(parsed_data, dict)
            and "strategy" in parsed_data
            and "pair" in parsed_data
            and ("buy_price" in parsed_data or "sell_price" in parsed_data)
            and "opportunity_id" not in parsed_data
        ):
            logger.info(f"[Processor-{PROCESSOR_ID}] Processing as TRADE OPPORTUNITY")
            return await process_opportunity(parsed_data)

        # THEN check if it's an execution (with opportunity_id)
        elif "opportunity_id" in parsed_data:
            logger.info(f"[Processor-{PROCESSOR_ID}] Processing as TRADE EXECUTION")
            return await process_execution(parsed_data, channel, tx_id=str(uuid.uuid4())[:8])
        else:
            logger.warning(
                f"[Processor-{PROCESSOR_ID}] UNRECOGNIZED DATA FORMAT with keys: {list(parsed_data.keys()) if isinstance(parsed_data, dict) else 'not a dict'}"
            )
            return {"type": "unknown", "processed": False}

    except Exception as e:
        logger.error(f"[Processor-{PROCESSOR_ID}] Unexpected error in processing message: {e}")
        logger.error(traceback.format_exc())
        return {"type": "error", "error": str(e), "processed": False}


async def process_opportunity(data: Dict) -> Dict[str, Any]:
    """Process a trade opportunity message"""
    transaction_id = str(uuid.uuid4())[:8]
    logger.info(f"[TX-{transaction_id}] IDENTIFIED AS OPPORTUNITY: {data.get('id')}")
    try:
        opportunity = TradeOpportunity(**data)
        logger.info(
            f"[TX-{transaction_id}] Processing trade opportunity: {opportunity.id} for strategy {opportunity.strategy}"
        )

        # Save to database
        with DatabaseService() as db:
            try:
                logger.info(f"[TX-{transaction_id}] Attempting database save for opportunity {opportunity.id}")
                saved = db.create_trade_opportunity(opportunity)
                logger.info(f"[TX-{transaction_id}] Trade opportunity saved: {saved.id} for {saved.strategy}")

                # Log success message with clear indication of DB persistence
                logger.info(f"[TX-{transaction_id}] ✅ Successfully persisted trade opportunity {saved.id} to database")

                return {"type": "opportunity", "id": opportunity.id, "processed": True}
            except Exception as db_error:
                logger.error(f"[TX-{transaction_id}] Database error saving opportunity: {db_error}", exc_info=True)
                return {"type": "opportunity", "error": str(db_error), "processed": False}
    except Exception as e:
        logger.error(f"[TX-{transaction_id}] Error creating opportunity model: {e}", exc_info=True)
        return {"type": "opportunity", "error": str(e), "processed": False}


def create_execution(execution, db, tx_id):
    """
    Create a trade execution using the repository pattern
    """
    try:
        # Use the TradeExecutionRepository directly
        from src.arbirich.services.database.repositories.trade_execution_repository import TradeExecutionRepository

        # Initialize the repository with the engine
        repo = TradeExecutionRepository(engine=db.engine)

        # Use the repository to create the execution
        saved_execution = repo.create(execution)

        logger.info(f"[{tx_id}] Execution {saved_execution.id} successfully saved to database")
        return saved_execution
    except Exception as e:
        logger.error(f"[{tx_id}] Error creating execution: {e}", exc_info=True)
        return None


async def process_execution(data, channel, tx_id=None):
    """Process an execution message from Redis"""
    transaction_id = tx_id or str(uuid.uuid4())[:8]
    opportunity_id = data.get("opportunity_id")
    logger.info(
        f"[TX-{transaction_id}] Processing execution with ID: {data.get('id')} linked to opportunity {opportunity_id}"
    )

    # Add an initial delay before looking up the opportunity
    await asyncio.sleep(1.0)

    # Try to save the execution with retries
    max_retries = 3
    retry_delay = 2.0  # seconds

    for attempt in range(1, max_retries + 1):
        try:
            # First check if the opportunity exists
            with DatabaseService() as db:
                logger.info(f"[TX-{transaction_id}] Checking if opportunity {opportunity_id} exists in database")
                # Fix the parameter order - pass opportunity_id first, then db
                opportunity_exists = check_opportunity_exists(opportunity_id, db)

                if opportunity_exists:
                    logger.info(f"[TX-{transaction_id}] Found opportunity {opportunity_id} in database")
                else:
                    logger.warning(f"[TX-{transaction_id}] Opportunity {opportunity_id} not found in database")

            if not opportunity_exists:
                if attempt < max_retries:
                    # Exponential backoff for retries
                    wait_time = retry_delay * attempt
                    logger.warning(
                        f"[TX-{transaction_id}] Opportunity {opportunity_id} not found, retrying in {wait_time}s (attempt {attempt}/{max_retries})"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.warning(
                        f"[TX-{transaction_id}] Opportunity {opportunity_id} not found after {max_retries} retries, setting to NULL"
                    )
                    data["opportunity_id"] = None

            # Create and save the execution
            execution = TradeExecution(**data)
            logger.info(f"[TX-{transaction_id}] Created execution model, now saving to database")

            with DatabaseService() as db:
                # Instead of calling db.create_trade_execution, use our new function
                saved = create_execution(execution, db, transaction_id)
                if saved:
                    # Log success and return
                    logger.info(f"[TX-{transaction_id}] Successfully saved execution {saved.id} on attempt {attempt}")
                    return {
                        "status": "success",
                        "execution_id": saved.id,
                        "opportunity_id": saved.opportunity_id,
                    }
                else:
                    logger.warning(
                        f"[TX-{transaction_id}] Failed to save execution on attempt {attempt}, returned None"
                    )

        except Exception as e:
            logger.error(f"[TX-{transaction_id}] Error on attempt {attempt} to save execution: {e}", exc_info=True)
            if attempt < max_retries:
                logger.info(f"[TX-{transaction_id}] Retrying execution save, attempt {attempt + 1}/3")
                await asyncio.sleep(0.5)  # Brief pause before retry

    # If we got here, all attempts failed
    logger.error(f"[TX-{transaction_id}] Failed to process execution after 3 attempts")
    return {"status": "error", "message": "Failed to save execution after 3 attempts"}


async def update_strategy_metrics(db, strategy_name, transaction_id):
    """Update strategy metrics"""
    try:
        from src.arbirich.services.metrics.strategy_metrics_service import StrategyMetricsService

        # Get the strategy ID
        strategy = db.get_strategy_by_name(strategy_name)
        if not strategy:
            logger.warning(f"[TX-{transaction_id}] Strategy {strategy_name} not found, skipping metrics calculation")
            return None

        # Create metrics service
        metrics_service = StrategyMetricsService(db_service=db)

        # Calculate daily metrics
        now = datetime.now()
        day_start = datetime(now.year, now.month, now.day)

        metrics = metrics_service.calculate_strategy_metrics(
            session=db.session,
            strategy_id=strategy.id,
            period_start=day_start,
            period_end=now,
        )

        if metrics:
            logger.info(f"[TX-{transaction_id}] Updated daily metrics for strategy {strategy.name}")

        # Optionally calculate weekly metrics too
        week_start = now - timedelta(days=now.weekday())
        week_start = datetime(week_start.year, week_start.month, week_start.day)

        weekly_metrics = metrics_service.calculate_strategy_metrics(
            session=db.session,
            strategy_id=strategy.id,
            period_start=week_start,
            period_end=now,
        )

        if weekly_metrics:
            logger.info(f"[TX-{transaction_id}] Updated weekly metrics for strategy {strategy.name}")

        return {"daily": metrics, "weekly": weekly_metrics}

    except Exception as metrics_error:
        logger.error(f"[TX-{transaction_id}] Error calculating strategy metrics: {metrics_error}")
        # Don't fail the whole function if metrics calculation fails
        return None
