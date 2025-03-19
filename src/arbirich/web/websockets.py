import asyncio
import json
from typing import Any, Dict, List

from fastapi import WebSocket

from src.arbirich.services.database.database_service import DatabaseService
from src.arbirich.web.frontend import get_dashboard_stats


class ConnectionManager:
    """Manager for WebSocket connections."""

    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)

    async def broadcast(self, message: Dict[str, Any]):
        """Send a message to all connected clients."""
        for connection in self.active_connections:
            await connection.send_text(json.dumps(message))


# Create a connection manager instance
manager = ConnectionManager()


async def get_dashboard_data():
    """Get dashboard data from the database to broadcast to clients."""
    with DatabaseService() as db_service:
        # Get statistics using the helper function from frontend.py
        stats = get_dashboard_stats(db_service)

    return {"type": "stats", "stats": stats}


async def websocket_broadcast_task():
    """Background task to broadcast data to connected clients."""
    while True:
        try:
            # Get data from database
            data = await get_dashboard_data()

            # Broadcast to all connected clients
            await manager.broadcast(data)

            # Wait before sending next update
            await asyncio.sleep(5)  # Update every 5 seconds
        except Exception as e:
            print(f"Error in websocket broadcast: {e}")
            await asyncio.sleep(5)  # Wait before retrying
