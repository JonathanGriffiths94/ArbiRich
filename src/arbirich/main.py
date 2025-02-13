from dotenv import load_dotenv
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Bot started successfully!")

load_dotenv()

api_key = os.getenv("API_KEY")
logger.info(f"API key: {api_key}")
