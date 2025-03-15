import logging

from src.arbirich.core.flow_manager import FlowManager

logger = logging.getLogger(__name__)

flow_manager = FlowManager()


async def startup_event():
    """Event handler for application startup"""
    logger.info("Application startup event triggered")
    try:
        # Use the flow manager directly - it's now an async context manager
        await flow_manager.startup()
        logger.info("Flows started via FlowManager.")
    except Exception as e:
        logger.error(f"Error during startup: {e}")
        # Let the error propagate to show in logs, but don't crash the app
        logger.error("Startup completed with errors")


async def shutdown_event():
    """Event handler for application shutdown"""
    logger.info("Application shutdown event triggered")
    try:
        await flow_manager.shutdown()
        logger.info("Flows stopped via FlowManager.")
    except Exception as e:
        logger.error(f"Error during shutdown: {e}")
