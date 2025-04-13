import logging

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/api/system", tags=["system"])


class DebugConfig(BaseModel):
    enabled: bool = True


class LogLevelConfig(BaseModel):
    level: str = "DEBUG"


@router.post("/debug")
async def set_debug_mode(config: DebugConfig):
    """
    Enable or disable debug mode for the system.
    """
    try:
        # Set root logger level
        root_logger = logging.getLogger()
        level = logging.DEBUG if config.enabled else logging.INFO
        root_logger.setLevel(level)

        # Set specific module loggers
        logging.getLogger("src.arbirich").setLevel(level)

        # Set bytewax logger level (keep at INFO unless debug is enabled)
        bytewax_level = logging.DEBUG if config.enabled else logging.INFO
        logging.getLogger("bytewax").setLevel(bytewax_level)

        # Update detection component's debug mode
        from src.arbirich.core.trading.components.detection import DetectionComponent

        for component in DetectionComponent._instances.values():
            if hasattr(component, "debug_mode"):
                component.debug_mode = config.enabled
                logging.info(f"Updated {component.name} debug_mode to {config.enabled}")

        return {"success": True, "debug_enabled": config.enabled}
    except Exception as e:
        logging.error(f"Error setting debug mode: {e}")
        raise HTTPException(status_code=500, detail=f"Error setting debug mode: {str(e)}")


@router.post("/force_debug")
async def force_debug_mode():
    """
    Force enable debug mode and restart detection flow with debug enabled.
    """
    try:
        # First set debug mode
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        logging.getLogger("src.arbirich").setLevel(logging.DEBUG)
        logging.getLogger("bytewax").setLevel(logging.DEBUG)

        # Force debug mode in detection component
        from src.arbirich.core.trading.components.detection import DetectionComponent

        for component in DetectionComponent._instances.values():
            if hasattr(component, "debug_mode"):
                component.debug_mode = True
                logging.info(f"Forced debug mode ON for {component.name}")

        # Restart detection flows with debug mode
        try:
            from src.arbirich.core.trading.flows.bytewax_flows.detection.detection_flow import (
                run_detection_flow,
                stop_detection_flow_async,
            )

            # Stop current flow
            await stop_detection_flow_async()

            # Start with debug mode
            for component in DetectionComponent._instances.values():
                if hasattr(component, "active_flows"):
                    for flow_id, flow_info in component.active_flows.items():
                        strategy_name = flow_info.get("strategy_name")
                        if strategy_name:
                            logging.info(
                                f"🚀 Restarting detection flow {flow_id} with DEBUG mode for strategy {strategy_name}"
                            )
                            await run_detection_flow(strategy_name=strategy_name, debug_mode=True)
        except Exception as e:
            logging.error(f"Error restarting flows: {e}")

        return {"success": True, "message": "Debug mode forced and detection flows restarted"}
    except Exception as e:
        logging.error(f"Error forcing debug mode: {e}")
        raise HTTPException(status_code=500, detail=f"Error forcing debug mode: {str(e)}")


@router.post("/log_level")
async def set_log_level(config: LogLevelConfig):
    """
    Set the log level for the system.
    """
    try:
        # Map string level to logging level
        level_map = {
            "DEBUG": logging.DEBUG,
            "INFO": logging.INFO,
            "WARNING": logging.WARNING,
            "ERROR": logging.ERROR,
            "CRITICAL": logging.CRITICAL,
        }

        level = level_map.get(config.level.upper(), logging.INFO)

        # Set root logger level
        root_logger = logging.getLogger()
        root_logger.setLevel(level)

        # Set specific module loggers
        logging.getLogger("src.arbirich").setLevel(level)

        # Don't set bytewax below INFO unless it's DEBUG
        bytewax_level = level if level == logging.DEBUG or level > logging.INFO else logging.INFO
        logging.getLogger("bytewax").setLevel(bytewax_level)

        return {"success": True, "log_level": config.level}
    except Exception as e:
        logging.error(f"Error setting log level: {e}")
        raise HTTPException(status_code=500, detail=f"Error setting log level: {str(e)}")


@router.post("/restart_detection")
async def restart_detection():
    """
    Restart the detection flow.
    """
    try:
        # Import and use the detection flow functions
        from src.arbirich.core.trading.components.detection import DetectionComponent
        from src.arbirich.core.trading.flows.bytewax_flows.detection.detection_flow import (
            run_detection_flow,
            stop_detection_flow_async,
        )

        # Stop the current flow
        await stop_detection_flow_async()
        logging.info("Detection flow stopped")

        # Restart with current settings
        for component in DetectionComponent._instances.values():
            if hasattr(component, "active_flows"):
                for flow_id, flow_info in component.active_flows.items():
                    strategy_name = flow_info.get("strategy_name")
                    if strategy_name:
                        debug_mode = getattr(component, "debug_mode", False)
                        logging.info(
                            f"🚀 Restarting detection flow {flow_id} with debug={debug_mode} for strategy {strategy_name}"
                        )
                        await run_detection_flow(strategy_name=strategy_name, debug_mode=debug_mode)

        return {"success": True, "message": "Detection flow restarted"}
    except Exception as e:
        logging.error(f"Error restarting detection flow: {e}")
        raise HTTPException(status_code=500, detail=f"Error restarting detection flow: {str(e)}")
