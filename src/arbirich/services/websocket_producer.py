import asyncio
import json
import logging
import random  # For jitter
from contextlib import suppress

from aiokafka import AIOKafkaProducer

logger = logging.getLogger(__name__)


class WebSocketManager:
    def __init__(
        self,
        exchange_clients: list,
        kafka_topic: str,
        kafka_bootstrap_servers: str = "localhost:9092",
    ):
        self.exchange_clients = exchange_clients
        self.kafka_topic = kafka_topic
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.websocket_tasks = []
        self.producer = None  # AIOKafkaProducer instance

    async def start(self):
        """
        Start the Kafka producer and WebSocket connections.
        """
        logger.info("Starting Kafka producer...")
        self.producer = AIOKafkaProducer(
            bootstrap_servers=self.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await self.producer.start()
        logger.info("Kafka producer started.")

        logger.info("Starting WebSocket connections...")
        for client in self.exchange_clients:
            task = asyncio.create_task(self._run_client(client))
            self.websocket_tasks.append(task)

    async def _run_client(self, client):
        """
        Runs a single exchange client's WebSocket connection with reconnection handling.
        """

        async def price_callback(price):
            # Create a message to publish to Kafka
            message = {
                "exchange": client.name,
                "symbol": client.symbol,  # Assuming each client has a symbol attribute
                "price": price,
            }
            try:
                await self.producer.send_and_wait(self.kafka_topic, message)
                logger.info(f"Published message to Kafka: {message}")
            except Exception as e:
                logger.error(f"Error publishing message to Kafka: {e}")

        delay = 1  # Initial delay for reconnection backoff

        while True:
            try:
                logger.info(f"Connecting {client.name} WebSocket...")
                await client.connect_websocket(price_callback)
                logger.info(f"WebSocket for {client.name} connected successfully.")
                delay = 1  # Reset delay on successful connection
            except Exception as e:
                logger.error(f"Error in {client.name} WebSocket: {e}")
                # Exponential backoff with random jitter
                wait_time = min(delay * 2, 60) + (random.random() * 2)
                logger.info(
                    f"Retrying {client.name} WebSocket in {wait_time:.2f} seconds..."
                )
                await asyncio.sleep(wait_time)
                delay = wait_time

    async def stop(self):
        """
        Stop all WebSocket connections and the Kafka producer.
        """
        logger.info("Stopping WebSocket connections...")
        for task in self.websocket_tasks:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        logger.info("All WebSocket connections stopped.")

        if self.producer:
            logger.info("Stopping Kafka producer...")
            await self.producer.stop()
            logger.info("Kafka producer stopped.")
