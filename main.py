import logging
import os
import time  # Import time for the sleep function

import uvicorn
from dotenv import load_dotenv

from src.arbirich.core.app import make_app
from src.arbirich.utils.banner import display_banner

if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
        ],
    )

    logger = logging.getLogger(__name__)

    # Load environment variables
    load_dotenv()

    # Display the banner at startup
    display_banner(
        f"Environment: {os.getenv('ENV', 'development').upper()}", log_only=False
    )  # Set log_only=False to see the banner

    # Add a pause to admire the banner (3 seconds)
    banner_pause = float(os.getenv("BANNER_PAUSE", "1.5"))  # Configurable via environment variable
    if banner_pause > 0:
        logger.info(f"Starting application in {banner_pause} seconds...")
        time.sleep(banner_pause)

    # Create and run the FastAPI application
    app = make_app()

    host = os.getenv("WEB_HOST", "0.0.0.0")
    port = int(os.getenv("WEB_PORT", 8080))

    logger.info(f"Starting ArbiRich FastAPI server at http://{host}:{port}")
    uvicorn.run(app, host=host, port=port)
