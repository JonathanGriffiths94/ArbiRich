import logging
import os

import uvicorn

from arbirich.core.app import make_app

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
        ],
    )

    logger = logging.getLogger(__name__)

    logger.info("Starting the application...")
    app = make_app()

    host = os.getenv("WEB_HOST", "0.0.0.0")
    port = int(os.getenv("WEB_PORT", 8088))

    logger.info(f"Running the application on {host}:{port}")

    uvicorn.run(app, host=host, port=port)
