import logging
import os

import redis
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

# Channels to monitor
CHANNELS = ["trade_opportunities", "order_book"]


def monitor_channels():
    try:
        # Connect to Redis
        redis_client = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True)
        pubsub = redis_client.pubsub()

        # Subscribe to channels
        pubsub.subscribe(CHANNELS)
        logger.info(f"Subscribed to channels: {CHANNELS}")

        # Monitor channels
        for message in pubsub.listen():
            if message["type"] == "message":
                logger.info(f"Received message from channel {message['channel']}: {message['data']}")

    except Exception as e:
        logger.error(f"Error monitoring channels: {e}")


if __name__ == "__main__":
    monitor_channels()
