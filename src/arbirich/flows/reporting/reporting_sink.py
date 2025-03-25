import json
import logging
import time
import traceback
import uuid
from datetime import datetime, timedelta

from sqlalchemy import select

from src.arbirich.models.models import TradeExecution, TradeOpportunity
from src.arbirich.models.schema import trade_opportunities
from src.arbirich.services.database.database_service import DatabaseService

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a database service instance
db_service = DatabaseService()


def db_sink(item):
    """Process item and save to database"""
    try:
        logger.info(f"DB_SINK RECEIVED: {type(item)}, Channel info: {getattr(item, 'channel', 'Unknown')}")

        if not item:
            logger.debug("Received empty item, skipping")
            return {}

        # Parse the data based on its type
        data = None
        if isinstance(item, str):
            # If it's a string, try to parse as JSON
            data = json.loads(item)
        elif isinstance(item, dict):
            # If it's already a dict, use it directly
            data = item
        else:
            logger.error(f"Unsupported data type: {type(item)}")
            return {}

        # Log received data
        if isinstance(data, dict):
            logger.info(f"Received message with ID: {data.get('id')} and fields: {list(data.keys())}")
            if "opportunity_id" in data:
                logger.info(f"Message has opportunity_id: {data['opportunity_id']} (should be processed as execution)")
            if "strategy" in data:
                logger.info(f"Message has strategy: {data['strategy']}")

        # FIRST check if it's an opportunity and process it immediately
        if (
            isinstance(data, dict)
            and "strategy" in data
            and "pair" in data
            and ("buy_price" in data or "sell_price" in data)
            and "opportunity_id" not in data
        ):
            logger.info(f"IDENTIFIED AS OPPORTUNITY: {data.get('id')}")
            try:
                opportunity = TradeOpportunity(**data)
                logger.info(f"Processing trade opportunity: {opportunity.id} for strategy {opportunity.strategy}")

                # Save to database
                with db_service as db:
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

        # THEN check if it's an execution (with opportunity_id)
        elif "opportunity_id" in data:
            opportunity_id = data.get("opportunity_id")
            logger.info(f"Processing execution with ID: {data.get('id')} linked to opportunity {opportunity_id}")

            # Add an initial delay before looking up the opportunity
            time.sleep(1.0)

            # Try to save the execution with retries
            max_retries = 3
            retry_delay = 2.0  # seconds - increased from original

            for attempt in range(1, max_retries + 1):
                try:
                    # First check if the opportunity exists
                    with db_service as db:
                        opportunity_exists = check_opportunity_exists(db, opportunity_id)

                    if not opportunity_exists:
                        if attempt < max_retries:
                            # Exponential backoff for retries
                            wait_time = 2**attempt  # 2, 4, 8 seconds
                            logger.warning(
                                f"Opportunity {opportunity_id} not found, retrying in {wait_time}s (attempt {attempt}/{max_retries})"
                            )
                            time.sleep(wait_time)
                            continue
                        else:
                            logger.warning(
                                f"Opportunity {opportunity_id} not found after {max_retries} retries, setting to NULL"
                            )
                            data["opportunity_id"] = None

                    # Create and save the execution
                    execution = TradeExecution(**data)
                    logger.info("Created execution model, now saving to database")

                    with db_service as db:
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

                        try:
                            from src.arbirich.services.metrics.strategy_metrics_service import (
                                StrategyMetricsService,
                            )

                            # Get the strategy ID
                            strategy = db.get_strategy_by_name(execution.strategy)
                            if strategy:
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
                            else:
                                logger.warning(f"Strategy {execution.strategy} not found, skipping metrics calculation")
                        except Exception as metrics_error:
                            logger.error(f"Error calculating strategy metrics: {metrics_error}")
                            # Don't fail the whole function if metrics calculation fails
                        except Exception as e:
                            # Log but don't fail the entire function
                            logger.error(f"Error updating strategy stats: {e}")

                        return {"type": "execution", "id": saved.id, "processed": True}

                except Exception as e:
                    if attempt < max_retries:
                        logger.error(f"Error processing execution (attempt {attempt}/{max_retries}): {e}")
                        time.sleep(retry_delay)
                    else:
                        logger.error(f"Failed to process execution after {max_retries} attempts: {e}", exc_info=True)
                        return {"type": "execution", "error": str(e), "processed": False}

            return {"type": "execution", "error": "Max retries exceeded", "processed": False}

        else:
            logger.warning(
                f"UNRECOGNIZED DATA FORMAT with keys: {list(data.keys()) if isinstance(data, dict) else 'not a dict'}"
            )
            return {"type": "unknown", "processed": False}

    except Exception as e:
        logger.error(f"Unexpected error in db_sink: {e}")
        logger.error(traceback.format_exc())
        return {"type": "error", "error": str(e), "processed": False}


def check_opportunity_exists(db, opportunity_id: str) -> bool:
    """Check if an opportunity with the given ID exists in the database"""
    try:
        with db.engine.begin() as conn:
            query = select(trade_opportunities.c.id).where(trade_opportunities.c.id == uuid.UUID(opportunity_id))
            result = conn.execute(query).first()
            return result is not None
    except Exception as e:
        logger.error(f"Error checking opportunity existence: {e}")
        return False
