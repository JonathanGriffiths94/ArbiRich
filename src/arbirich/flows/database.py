import json
from datetime import datetime

from bytewax.dataflow import Dataflow
from bytewax.run import cli_main

from src.arbirich.db_manager import DatabaseManager


# Define a sink function that writes to the database.
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


def build_flow():
    flow = Dataflow()

    # Assume you have a source that emits messages from Redis or any other source.
    # For demonstration, we use a simple list of JSON strings.
    messages = [
        '{"trading_pair_id":1, "buy_exchange_id":1, "sell_exchange_id":2, "buy_price":50000, "sell_price":50500, "spread":0.001, "volume":0.1, "opportunity_timestamp": "2023-03-01T12:00:00"}',
        '{"trading_pair_id":1, "buy_exchange_id":1, "sell_exchange_id":2, "executed_buy_price":50000, "executed_sell_price":50500, "spread":0.001, "volume":0.1, "execution_timestamp": "2023-03-01T12:00:05", "execution_id": "exec123"}',
    ]

    # For a real-world scenario, replace this with your actual source
    flow.input("inp", messages)

    # Here we directly send the messages to our sink
    flow.sink("db_sink", db_sink)

    return flow


if __name__ == "__main__":
    # Initialize the database manager and pass it as part of the state.
    initial_state = {"db_manager": DatabaseManager()}
    try:
        cli_main(build_flow, initial_state=initial_state)
    finally:
        initial_state["db_manager"].close()
