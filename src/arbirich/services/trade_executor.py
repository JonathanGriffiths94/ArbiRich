import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer

from src.arbirich.core.logging_config import logger as trade_logger

logger = logging.getLogger(__name__)


async def execute_trade(
    exchange: str,
    symbol: str,
    side: str,
    quantity: float,
    price: float,
    status="SUCCESS",
    message=None,
):
    # Log trade details to the database using the trade_logger
    trade_logger.info(
        message or "Trade executed",
        extra={
            "exchange": exchange,
            "symbol": symbol,
            "side": side,
            "quantity": quantity,
            "price": price,
            "status": status,
        },
    )


async def consume_arbitrage_alerts(
    kafka_topic: str = "arbitrage_alerts",
    kafka_bootstrap_servers: str = "localhost:9092",
    group_id: str = "trade-executor-group",
):
    """
    Consume arbitrage alert messages from Kafka and execute trades accordingly.
    """
    consumer = AIOKafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    await consumer.start()
    logger.info("Trade executor Kafka consumer started.")
    try:
        async for message in consumer:
            data = message.value
            logger.info(f"Arbitrage alert received: {data}")
            # Extract trade details from the alert message.
            # Adjust these fields based on your alert message structure.
            exchange = data.get("sell_exchange") or data.get("buy_exchange")
            symbol = data.get("symbol", "UNKNOWN")
            side = "buy"  # Example logic: decide which side to execute; this may vary.
            quantity = data.get("quantity", 1)  # Default quantity (adjust as needed)
            # Decide the price to use. You might use buy_price if executing a buy trade.
            price = data.get("buy_price") or data.get("sell_price")

            # Execute the trade; the execute_trade function logs the trade in the database.
            await execute_trade(
                exchange, symbol, side, quantity, price, message=data.get("message")
            )
    except asyncio.CancelledError:
        logger.info("Trade executor consumer task cancelled.")
    except Exception as e:
        logger.error(f"Error in trade executor consumer: {e}")
    finally:
        await consumer.stop()
        logger.info("Trade executor Kafka consumer stopped.")


# if __name__ == "__main__":
#     asyncio.run(consume_arbitrage_alerts())
