from dotenv import load_dotenv
import os
import logging
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from app.services.price_service import PriceService


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Bot started successfully!")

load_dotenv()

api_key = os.getenv("API_KEY")
logger.info(f"API key: {api_key}")

app = FastAPI()

# Initialize price service to fetch data from exchanges
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
