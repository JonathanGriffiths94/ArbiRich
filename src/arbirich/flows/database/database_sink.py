import json
import logging

from pydantic import ValidationError

from arbirich.services.database.database_service import DatabaseService
from src.arbirich.models.models import TradeExecution, TradeOpportunity

logger = logging.getLogger(__name__)


def db_sink(state, item):
    try:
        data = json.loads(item)
        logger.debug(f"Received data: {data}")
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON: {e}")
        return state

    # Get database service from state or create a new one
    db_service = state.get("db_service")
    if not db_service:
        logger.info("Creating new database service")
        db_service = DatabaseService()
        state["db_service"] = db_service

    try:
        with db_service:
            if "buy_price" in data and "strategy" in data and "pair" in data:
                # Handle trade opportunity
                opportunity = TradeOpportunity(**data)
                logger.info(f"Processing trade opportunity: {opportunity.id}")

                # Save to database
                db_service.create_trade_opportunity(opportunity)
                logger.info(f"Trade opportunity saved: {opportunity.id}")

            elif "executed_buy_price" in data and "strategy" in data and "pair" in data:
                # Handle trade execution
                execution = TradeExecution(**data)
                logger.info(f"Processing trade execution: {execution.id}")

                # Save to database
                db_service.create_trade_execution(execution)
                logger.info(f"Trade execution saved: {execution.id}")

                # Update strategy metrics if profit/loss information is available
                profit = execution.executed_sell_price - execution.executed_buy_price
                if profit != 0 and execution.strategy:
                    db_service.update_strategy_stats(
                        strategy_name=execution.strategy,  # Use strategy name instead of ID
                        profit=profit if profit > 0 else 0,
                        loss=profit if profit < 0 else 0,
                    )
            else:
                logger.error(f"Invalid data format: {data}")
    except ValidationError as e:
        logger.error(f"Validation error: {e}")
    except Exception as e:
        logger.exception(f"Error processing data: {e}")

    return state
