# reporting/message_processor.py

import asyncio
import json
import logging
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict

from arbirich.core.state.system_state import is_system_shutting_down
from src.arbirich.models.models import TradeExecution, TradeOpportunity
from src.arbirich.services.database.database_service import DatabaseService

from .db_functions import check_opportunity_exists

logger = logging.getLogger(__name__)


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
        logger.info(f"Processing message from channel: {channel}")

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
            logger.info(f"Received message with ID: {parsed_data.get('id')} and fields: {list(parsed_data.keys())}")
            if "opportunity_id" in parsed_data:
                logger.info(
                    f"Message has opportunity_id: {parsed_data['opportunity_id']} (should be processed as execution)"
                )
            if "strategy" in parsed_data:
                logger.info(f"Message has strategy: {parsed_data['strategy']}")

        # FIRST check if it's an opportunity and process it immediately
        if (
            isinstance(parsed_data, dict)
            and "strategy" in parsed_data
            and "pair" in parsed_data
            and ("buy_price" in parsed_data or "sell_price" in parsed_data)
            and "opportunity_id" not in parsed_data
        ):
            return await process_opportunity(parsed_data)

        # THEN check if it's an execution (with opportunity_id)
        elif "opportunity_id" in parsed_data:
            return await process_execution(parsed_data)
        else:
            logger.warning(
                f"UNRECOGNIZED DATA FORMAT with keys: {list(parsed_data.keys()) if isinstance(parsed_data, dict) else 'not a dict'}"
            )
            return {"type": "unknown", "processed": False}

    except Exception as e:
        logger.error(f"Unexpected error in processing message: {e}")
        logger.error(traceback.format_exc())
        return {"type": "error", "error": str(e), "processed": False}


async def process_opportunity(data: Dict) -> Dict[str, Any]:
    """Process a trade opportunity message"""
    logger.info(f"IDENTIFIED AS OPPORTUNITY: {data.get('id')}")
    try:
        opportunity = TradeOpportunity(**data)
        logger.info(f"Processing trade opportunity: {opportunity.id} for strategy {opportunity.strategy}")

        # Save to database
        with DatabaseService() as db:
            try:
                saved = db.create_trade_opportunity(opportunity)
                logger.info(f"Trade opportunity saved: {saved.id} for {saved.strategy}")
                return {"type": "opportunity", "id": opportunity.id, "processed": True}
            except Exception as db_error:
                logger.error(f"Database error saving opportunity: {db_error}", exc_info=True)
                return {"type": "opportunity", "error": str(db_error), "processed": False}
    except Exception as e:
        logger.error(f"Error creating opportunity model: {e}", exc_info=True)
        return {"type": "opportunity", "error": str(e), "processed": False}


async def process_execution(data: Dict) -> Dict[str, Any]:
    """Process a trade execution message"""
    opportunity_id = data.get("opportunity_id")
    logger.info(f"Processing execution with ID: {data.get('id')} linked to opportunity {opportunity_id}")

    # Add an initial delay before looking up the opportunity
    await asyncio.sleep(1.0)

    # Try to save the execution with retries
    max_retries = 3
    retry_delay = 2.0  # seconds

    for attempt in range(1, max_retries + 1):
        try:
            # First check if the opportunity exists
            with DatabaseService() as db:
                opportunity_exists = check_opportunity_exists(db, opportunity_id)

            if not opportunity_exists:
                if attempt < max_retries:
                    # Exponential backoff for retries
                    wait_time = 2**attempt  # 2, 4, 8 seconds
                    logger.warning(
                        f"Opportunity {opportunity_id} not found, retrying in {wait_time}s (attempt {attempt}/{max_retries})"
                    )
                    await asyncio.sleep(wait_time)
                    continue
                else:
                    logger.warning(
                        f"Opportunity {opportunity_id} not found after {max_retries} retries, setting to NULL"
                    )
                    data["opportunity_id"] = None

            # Create and save the execution
            execution = TradeExecution(**data)
            logger.info("Created execution model, now saving to database")

            with DatabaseService() as db:
                # Log what we're about to do
                logger.info(f"Calling create_trade_execution for {execution.id}")

                # Try the operation
                saved = db.create_trade_execution(execution)
                logger.info(f"Trade execution saved to database: {saved.id}")

                # Calculate profit/loss
                profit = execution.executed_sell_price - execution.executed_buy_price
                total_value = profit * execution.volume

                logger.info(f"Calculating P&L: {profit} per unit × {execution.volume} units = {total_value}")

                # Update strategy stats
                try:
                    db.update_strategy_stats(
                        strategy_name=execution.strategy,
                        profit=total_value if total_value > 0 else 0,
                        loss=abs(total_value) if total_value < 0 else 0,
                        trade_count=1,
                    )
                    logger.info(f"Updated strategy stats for {execution.strategy}")
                except Exception as e:
                    # Log but don't fail the entire function
                    logger.error(f"Error updating strategy stats: {e}")

                # Update metrics
                await update_strategy_metrics(db, execution.strategy)

                return {"type": "execution", "id": saved.id, "processed": True}

        except Exception as e:
            if attempt < max_retries:
                logger.error(f"Error processing execution (attempt {attempt}/{max_retries}): {e}")
                await asyncio.sleep(retry_delay)
            else:
                logger.error(f"Failed to process execution after {max_retries} attempts: {e}", exc_info=True)
                return {"type": "execution", "error": str(e), "processed": False}

    return {"type": "execution", "error": "Max retries exceeded", "processed": False}


async def update_strategy_metrics(db, strategy_name):
    """Update strategy metrics"""
    try:
        from src.arbirich.services.metrics.strategy_metrics_service import StrategyMetricsService

        # Get the strategy ID
        strategy = db.get_strategy_by_name(strategy_name)
        if not strategy:
            logger.warning(f"Strategy {strategy_name} not found, skipping metrics calculation")
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
            logger.info(f"Updated daily metrics for strategy {strategy.name}")

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
            logger.info(f"Updated weekly metrics for strategy {strategy.name}")

        return {"daily": metrics, "weekly": weekly_metrics}

    except Exception as metrics_error:
        logger.error(f"Error calculating strategy metrics: {metrics_error}")
        # Don't fail the whole function if metrics calculation fails
        return None
