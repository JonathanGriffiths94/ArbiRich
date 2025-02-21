import json
import logging

import asyncpg
import redis.asyncio as aioredis  # Use an async Redis client

logger = logging.getLogger(__name__)


class DBManager:
    def __init__(self, pg_dsn, redis_url):
        self.pg_dsn = pg_dsn
        self.redis_url = redis_url
        self.redis = None
        self.pool = None

    async def init(self):
        # Initialize Postgres connection pool
        self.pool = await asyncpg.create_pool(dsn=self.pg_dsn)
        # Initialize Redis client
        self.redis = aioredis.from_url(self.redis_url, decode_responses=True)

    async def process_message(self, message):
        """
        Process a message from the pub/sub channel and write to the DB
        depending on certain conditions.
        """
        try:
            data = json.loads(message)
            # Perform condition checks, for example:
            if data.get("type") == "trade_execution":
                await self.store_trade_execution(data)
            elif data.get("type") == "trade_opportunity":
                await self.store_trade_opportunity(data)
            # Add more conditions as needed
        except Exception as e:
            logger.error(f"Error processing message: {e}")

    async def store_trade_execution(self, data):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Insert into your trade_execution table
                await conn.execute(
                    "INSERT INTO trade_execution (buy_order, sell_order, profit, timestamp) VALUES ($1, $2, $3, $4)",
                    data["buy_order"],
                    data["sell_order"],
                    data["profit"],
                    data["timestamp"],
                )
                logger.info("Trade execution stored successfully.")

    async def store_trade_opportunity(self, data):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Insert into your trade_opportunity table
                await conn.execute(
                    "INSERT INTO trade_opportunity (buy_exchange, sell_exchange, pair, buy_price, sell_price, spread, timestamp) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                    data["buy_exchange"],
                    data["sell_exchange"],
                    data["pair"],
                    data["buy_price"],
                    data["sell_price"],
                    data["spread"],
                    data.get("timestamp", None),
                )
                logger.info("Trade opportunity stored successfully.")

    async def listen(self):
        pubsub = self.redis.pubsub()
        await pubsub.subscribe("db_events")  # Assuming you publish messages here
        logger.info("DBManager subscribed to 'db_events'")
        async for message in pubsub.listen():
            if message["type"] == "message":
                await self.process_message(message["data"])


# async def main():
#     db_manager = DBManager(pg_dsn="postgresql://user:password@localhost/db", redis_url="redis://localhost:6379")
#     await db_manager.init()
#     await db_manager.listen()

# if __name__ == "__main__":
#     logging.basicConfig(level=logging.DEBUG)
#     asyncio.run(main())
