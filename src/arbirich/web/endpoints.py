import asyncio
import logging

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from starlette.responses import JSONResponse

from src.arbirich.core.state.system_state import is_system_shutting_down
from src.arbirich.web.websockets import handle_websocket_message, manager, websocket_broadcast_task

logger = logging.getLogger(__name__)

router = APIRouter(tags=["websockets"])


@router.websocket("/ws/dashboard")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for dashboard real-time updates."""
    await manager.connect(websocket)

    # Create task for broadcasting data
    broadcast_task = asyncio.create_task(websocket_broadcast_task())

    try:
        # Keep the connection open and handle client messages
        while True:
            # Check if system is shutting down
            if is_system_shutting_down():
                logger.info("System shutdown detected in WebSocket endpoint, closing connection")
                await websocket.close(code=1001, reason="System shutting down")
                break

            # Wait for any messages from the client with a timeout
            try:
                message = await asyncio.wait_for(websocket.receive_text(), timeout=1.0)
                await handle_websocket_message(websocket, message)
            except asyncio.TimeoutError:
                # No message received within timeout, continue loop to check shutdown
                continue

    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error(f"Error in WebSocket endpoint: {e}", exc_info=True)
    finally:
        # Always disconnect and clean up
        manager.disconnect(websocket)

        # Cancel the broadcast task
        if broadcast_task and not broadcast_task.done():
            logger.info("Cancelling WebSocket broadcast task")
            broadcast_task.cancel()
            try:
                await asyncio.wait_for(broadcast_task, timeout=2.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                # Task cancellation is expected
                pass
            except Exception as e:
                logger.error(f"Error cancelling broadcast task: {e}")


@router.post("/emergency_stop")
async def emergency_stop():
    """Emergency stop endpoint - force shutdown the system."""
    try:
        # Import the emergency shutdown function

        # Log the request
        logger.critical("EMERGENCY STOP requested via API endpoint")

        # Initiate emergency shutdown in a background task so the response can be sent
        asyncio.create_task(async_emergency_shutdown())

        return JSONResponse({"status": "emergency_shutdown_initiated"})
    except Exception as e:
        logger.error(f"Error initiating emergency shutdown: {e}")
        raise HTTPException(status_code=500, detail="Failed to initiate emergency shutdown")


async def async_emergency_shutdown():
    """Run emergency shutdown asynchronously."""
    try:
        # Small delay to allow the HTTP response to be sent
        await asyncio.sleep(0.5)

        # Import and call force_shutdown
        from src.arbirich.core.lifecycle.emergency_shutdown import force_shutdown

        force_shutdown(timeout=8.0)
    except Exception as e:
        logger.error(f"Error in async emergency shutdown: {e}")
