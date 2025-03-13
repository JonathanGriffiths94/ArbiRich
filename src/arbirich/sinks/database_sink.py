import json
import logging

from pydantic import ValidationError

from src.arbirich.models.dtos import TradeExecution, TradeOpportunity

logger = logging.getLogger(__name__)


def db_sink(state, item):
    try:
        data = json.loads(item)
    except json.JSONDecodeError as e:
        logger.error(f"Error parsing JSON: {e}")
        return state

    db_manager = state["db_manager"]

    if "buy_price" in data:
        try:
            opportunity = TradeOpportunity(**data)
            db_manager.create_trade_opportunity(
                trading_pair_id=opportunity.asset,
                buy_exchange_id=opportunity.buy_exchange,
                sell_exchange_id=opportunity.sell_exchange,
                buy_price=opportunity.buy_price,
                sell_price=opportunity.sell_price,
                spread=opportunity.spread,
                volume=opportunity.volume,
                opportunity_timestamp=opportunity.opportunity_timestamp,
            )
        except ValidationError as e:
            logger.error(f"Validation error for trade opportunity: {e}")
    elif "executed_buy_price" in data:
        try:
            execution = TradeExecution(**data)
            db_manager.create_trade_execution(
                trading_pair_id=execution.asset,
                buy_exchange_id=execution.buy_exchange,
                sell_exchange_id=execution.sell_exchange,
                executed_buy_price=execution.executed_buy_price,
                executed_sell_price=execution.executed_sell_price,
                spread=execution.spread,
                volume=execution.volume,
                execution_timestamp=execution.execution_timestamp,
                execution_id=execution.execution_id,
                opportunity_id=execution.opportunity_id,
            )
        except ValidationError as e:
            logger.error(f"Validation error for trade execution: {e}")
    else:
        logger.error(f"Invalid data: {data}")
    return state
