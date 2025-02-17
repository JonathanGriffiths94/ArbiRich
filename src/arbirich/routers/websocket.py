import asyncio
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

logger = logging.getLogger(__name__)
router = APIRouter()


@router.websocket("/")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        price_service = websocket.app.state.price_service

        while True:
            # You might choose to push updates as they come in instead of polling.
            data = {"prices": price_service.prices}
            await websocket.send_json(data)
            await asyncio.sleep(1)  # Send updates every second
    except WebSocketDisconnect:
        logger.error("Client disconnected")
