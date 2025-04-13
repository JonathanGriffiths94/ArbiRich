"""
Command-line interface utilities for flow management.

This module provides CLI-related functionality for interacting with flows.
"""

import logging

logger = logging.getLogger(__name__)


def get_flow_status(flow_id: str) -> dict:
    """
    Get the status of a flow.

    Args:
        flow_id: The ID of the flow to check

    Returns:
        A dictionary with flow status information
    """
    logger.info(f"Getting status for flow: {flow_id}")
    return {"id": flow_id, "status": "unknown", "message": "Flow status check is not fully implemented"}


def list_flows() -> list:
    """
    List all registered flows.

    Returns:
        A list of flow IDs
    """
    logger.info("Listing all flows")
    return []


def start_flow(flow_id: str) -> bool:
    """
    Start a flow.

    Args:
        flow_id: The ID of the flow to start

    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Starting flow: {flow_id}")

    try:
        # Get the appropriate flow manager based on the flow ID
        from src.arbirich.core.trading.flows.flow_manager import FlowManager

        # First try to get an existing manager
        flow_manager = FlowManager.get_manager(flow_id)

        if flow_manager:
            logger.info(f"Found existing flow manager for {flow_id}, starting flow...")
            import asyncio

            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(flow_manager.run_flow())
                return True
            else:
                return loop.run_until_complete(flow_manager.run_flow())

        # If no manager found, try direct module imports based on flow type
        if "detection" in flow_id:
            from src.arbirich.core.trading.flows.bytewax_flows.detection.detection_flow import run_detection_flow

            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(run_detection_flow())
                return True
            else:
                return loop.run_until_complete(run_detection_flow())

        elif "ingestion" in flow_id:
            from src.arbirich.core.trading.flows.bytewax_flows.ingestion.ingestion_flow import run_ingestion_flow

            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(run_ingestion_flow())
                return True
            else:
                return loop.run_until_complete(run_ingestion_flow())

        elif "execution" in flow_id:
            from src.arbirich.core.trading.flows.bytewax_flows.execution.execution_flow import run_execution_flow

            # Extract strategy name from flow_id if present
            strategy_name = None
            if "_" in flow_id:
                parts = flow_id.split("_")
                if len(parts) > 1:
                    strategy_name = parts[1]

            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(run_execution_flow(strategy_name=strategy_name))
                return True
            else:
                return loop.run_until_complete(run_execution_flow(strategy_name=strategy_name))

        elif "reporting" in flow_id:
            from src.arbirich.core.trading.flows.reporting.reporting_flow import start_flow as start_reporting_flow

            loop = asyncio.get_event_loop()
            if loop.is_running():
                asyncio.create_task(start_reporting_flow())
                return True
            else:
                return loop.run_until_complete(start_reporting_flow())

        else:
            logger.warning(f"No specific handler for flow type: {flow_id}")
            return False

    except Exception as e:
        logger.error(f"Error starting flow {flow_id}: {e}", exc_info=True)
        return False


def stop_flow(flow_id: str) -> bool:
    """
    Stop a flow.

    Args:
        flow_id: The ID of the flow to stop

    Returns:
        True if successful, False otherwise
    """
    logger.info(f"Stopping flow: {flow_id}")

    # Get the appropriate flow manager based on the flow ID
    from src.arbirich.core.state.system_state import mark_system_shutdown
    from src.arbirich.core.trading.flows.flow_manager import FlowManager

    # Mark system as shutting down to ensure all components respect it
    mark_system_shutdown(True)

    # First try to get an existing manager
    flow_manager = FlowManager.get_manager(flow_id)

    if flow_manager:
        logger.info(f"Found existing flow manager for {flow_id}, stopping flow...")
        return flow_manager.stop_flow()

    # If no manager found, try direct module imports based on flow type
    if "detection" in flow_id:
        from src.arbirich.core.trading.flows.bytewax_flows.detection.detection_flow import stop_detection_flow
        from src.arbirich.core.trading.flows.bytewax_flows.detection.detection_source import (
            mark_detection_force_kill,
        )

        # Force kill detection flows first
        mark_detection_force_kill()
        logger.info(f"Marked detection flow {flow_id} for force kill")

        # Then try normal stop - don't catch exceptions
        result = stop_detection_flow()
        logger.info(f"Stopped detection flow {flow_id}: {result}")
        return result

    elif "ingestion" in flow_id:
        from src.arbirich.core.trading.flows.bytewax_flows.ingestion.ingestion_flow import stop_ingestion_flow

        result = stop_ingestion_flow()
        logger.info(f"Stopped ingestion flow {flow_id}: {result}")
        return result

    elif "reporting" in flow_id:
        from src.arbirich.core.trading.flows.reporting.reporting_flow import stop_flow as stop_reporting_flow

        result = stop_reporting_flow()
        logger.info(f"Stopped reporting flow {flow_id}: {result}")
        return result

    else:
        # Raise error instead of returning False
        raise ValueError(f"No specific handler for flow type: {flow_id}")


def run_reporting(config=None):
    """
    Run the reporting flow with the given configuration.

    Args:
        config (dict, optional): Configuration settings for the reporting flow.
            Defaults to None, which will use default configuration.

    Returns:
        object: The initialized and running reporting flow instance
    """
    try:
        # Get the flow manager for reporting
        from src.arbirich.core.trading.flows.reporting.reporting_flow import get_flow_manager

        flow_manager = get_flow_manager()
        if config:
            flow_manager.config = config

        # Start the flow using the manager
        import asyncio

        loop = asyncio.get_event_loop()

        if loop.is_running():
            asyncio.create_task(flow_manager.run_flow())
            logger.info("Reporting flow started asynchronously")
            return flow_manager.reporting_flow
        else:
            success = loop.run_until_complete(flow_manager.run_flow())
            if success:
                logger.info("Reporting flow started successfully")
                return flow_manager.reporting_flow
            else:
                logger.error("Failed to start reporting flow")
                return None

    except Exception as e:
        logger.error(f"Failed to run reporting flow: {str(e)}", exc_info=True)
        return None


def initialize_reporting(config=None):
    """
    Initialize the reporting system for trading operations.

    Args:
        config (dict, optional): Configuration settings for the reporting system.
            Defaults to None, which will use default configuration.

    Returns:
        object: The initialized reporting system instance
    """
    from src.arbirich.services.reporting.reporting_service import ReportingService

    try:
        reporting_service = ReportingService(config)
        reporting_service.initialize()
        return reporting_service
    except Exception as e:
        import logging

        logger = logging.getLogger(__name__)
        logger.error(f"Failed to initialize reporting: {str(e)}")
        return None
