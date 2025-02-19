import asyncio
import json
import logging

from aiokafka import AIOKafkaConsumer

logger = logging.getLogger(__name__)


async def consume_market_data(
    price_service,
    arbitrage_service,
    alert_producer=None,
    alert_topic: str = "arbitrage_alerts",
    kafka_topic: str = "market_data",
    kafka_bootstrap_servers: str = "localhost:9092",
    group_id: str = "market-data-consumer",
):
    """
    Consume market data messages from Kafka and process them with PriceService and ArbitrageService.
    Additionally, if an arbitrage opportunity is detected and an alert_producer is provided,
    publish an alert message to the specified Kafka topic.
    """
    consumer = AIOKafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        group_id=group_id,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    await consumer.start()
    logger.info("Kafka consumer started.")
    try:
        async for message in consumer:
            data = message.value
            logger.info(f"Received message: {data}")

            # Extract necessary fields from the message
            exchange = data.get("exchange")
            price = data.get("price")

            if exchange is None or price is None:
                logger.error("Malformed message; missing 'exchange' or 'price'.")
                continue

            # Update the price using PriceService
            await price_service.update_price(exchange, price)
            # Trigger arbitrage detection using ArbitrageService.
            # Assume detect_opportunity() returns an alert message dict if an opportunity is detected, otherwise None.
            alert = arbitrage_service.detect_opportunity()

            # Publish arbitrage alert to Kafka if available
            if alert and alert_producer:
                try:
                    await alert_producer.send_and_wait(alert_topic, alert)
                    logger.info(f"Published arbitrage alert to Kafka: {alert}")
                except Exception as e:
                    logger.error(f"Error publishing arbitrage alert: {e}")

    except asyncio.CancelledError:
        logger.info("Kafka consumer task cancelled.")
    except Exception as e:
        logger.error(f"Error in Kafka consumer: {e}")
    finally:
        await consumer.stop()
        logger.info("Kafka consumer stopped.")
