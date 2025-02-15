import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from src.arbirich.services.price_service import PriceService

logger = logging.getLogger(__name__)

router = APIRouter()

price_service = PriceService()


@app.websocket("/ws/arbitrage")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            # Receive message from client (could be for strategy, etc.)
            data = await websocket.receive_text()

            # Simulating arbitrage data processing
            arbitrage_opportunity = price_service.get_arbitrage_opportunity()

            # Send back the result
            await websocket.send_text(str(arbitrage_opportunity))

    except WebSocketDisconnect:
        print("Client disconnected")
