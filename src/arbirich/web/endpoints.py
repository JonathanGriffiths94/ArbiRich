import asyncio

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from src.arbirich.web.websockets import manager, websocket_broadcast_task

router = APIRouter(tags=["websockets"])


@router.websocket("/ws/dashboard")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for dashboard real-time updates."""
    await manager.connect(websocket)

    # Create task for broadcasting data - no need for db dependency anymore
    broadcast_task = asyncio.create_task(websocket_broadcast_task())

    try:
        # Keep the connection open
        while True:
            # Wait for any messages from the client (can be used for filtering or commands)
            data = await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        broadcast_task.cancel()  # Cancel the broadcast task when client disconnects
