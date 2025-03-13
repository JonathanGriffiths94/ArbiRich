import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def db_sink(state, item):
    # Assume item is a JSON string representing either a trade opportunity or execution.
    try:
        data = json.loads(item)
    except Exception as e:
        print(f"Error parsing JSON: {e}")
        return state

    # Determine the type of message based on its contents
    db_manager = state["db_manager"]
    if "buy_price" in data:
        # Process trade opportunity
        db_manager.create_trade_opportunity(
            trading_pair_id=data["trading_pair_id"],
            buy_exchange_id=data["buy_exchange_id"],
            sell_exchange_id=data["sell_exchange_id"],
            buy_price=data["buy_price"],
            sell_price=data["sell_price"],
            spread=data["spread"],
            volume=data["volume"],
            opportunity_timestamp=datetime.fromisoformat(data["opportunity_timestamp"]),
        )
    elif "executed_buy_price" in data:
        # Process trade execution
        db_manager.create_trade_execution(
            trading_pair_id=data["trading_pair_id"],
            buy_exchange_id=data["buy_exchange_id"],
            sell_exchange_id=data["sell_exchange_id"],
            executed_buy_price=data["executed_buy_price"],
            executed_sell_price=data["executed_sell_price"],
            spread=data["spread"],
            volume=data["volume"],
            execution_timestamp=datetime.fromisoformat(data["execution_timestamp"]),
            execution_id=data.get("execution_id"),
            opportunity_id=data.get("opportunity_id"),
        )
    return state
