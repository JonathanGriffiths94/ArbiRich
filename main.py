import logging
import os

import uvicorn
from dotenv import load_dotenv

from src.arbirich.core.app import make_app
from src.arbirich.utils.banner import display_banner  # Changed to use the basic banner

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

    # Display the basic banner at startup - now with log_only=True to prevent duplication
    display_banner(f"Environment: {os.getenv('ENV', 'development').upper()}", log_only=True)

    # Create and run the FastAPI application
    app = make_app()

    host = os.getenv("WEB_HOST", "0.0.0.0")
    port = int(os.getenv("WEB_PORT", 8080))

    logger.info(f"Starting ArbiRich FastAPI server at http://{host}:{port}")
    uvicorn.run(app, host=host, port=port)
