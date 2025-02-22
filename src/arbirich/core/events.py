import logging

from src.arbirich.core.flow_manager import FlowManager

logger = logging.getLogger(__name__)

# Global variables to hold background tasks, so we can cancel them on shutdown.
ingestion_task = None
execution_task = None


async def startup_event():
    global flow_manager
    flow_manager = FlowManager()
    await flow_manager.__aenter__()
    logger.info("Flows started via FlowManager.")


async def shutdown_event():
    global flow_manager
    if flow_manager:
        await flow_manager.__aexit__(None, None, None)
        logger.info("Flows shut down via FlowManager.")
