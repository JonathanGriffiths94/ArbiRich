import asyncio
import logging

from src.arbirich.flows.ingestion import run_ingestion

logger = logging.getLogger(__name__)

# Global variable to hold our background task, so we can cancel it on shutdown.
bytewax_task = None


async def startup_event():
    """Logic to run on application startup."""
    global bytewax_task
    logger.info("Starting up application...")

    # Start the Bytewax dataflow in the background.
    # run_flow should be an async function defined in main_flow.py that starts your Bytewax flow.
    bytewax_task = asyncio.create_task(run_ingestion())
    logger.info("Bytewax flow has been started.")


async def shutdown_event():
    """Logic to run on application shutdown."""
    global bytewax_task
    logger.info("Shutting down application...")

    if bytewax_task:
        # Cancel the background task gracefully.
        bytewax_task.cancel()
        try:
            await bytewax_task
        except asyncio.CancelledError:
            logger.info("Bytewax flow task cancelled successfully.")

    logger.info("Application shutdown complete.")
