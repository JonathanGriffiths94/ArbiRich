import asyncio
import json
import logging
from typing import Any, Dict, List

from fastapi import WebSocket

from src.arbirich.services.database.database_service import DatabaseService
from src.arbirich.web.frontend import (
    get_dashboard_stats,
    get_recent_executions,
    get_recent_opportunities,
    get_strategy_leaderboard,
)

logger = logging.getLogger(__name__)


class ConnectionManager:
    """Manager for WebSocket connections."""

    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"Client connected. Total connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info(f"Client disconnected. Remaining connections: {len(self.active_connections)}")

    async def broadcast(self, message: Dict[str, Any]):
        """Send a message to all connected clients."""
        for connection in self.active_connections:
            try:
                await connection.send_text(json.dumps(message))
            except Exception as e:
                logger.error(f"Error sending message to client: {e}")


# Create a connection manager instance
manager = ConnectionManager()


async def get_full_dashboard_data():
    """Get complete dashboard data from the database to send to clients."""
    with DatabaseService() as db_service:
        # Get statistics
        stats = get_dashboard_stats(db_service)
        # Get only 5 opportunities for the dashboard
        opportunities = get_recent_opportunities(db_service, limit=5)
        # Get only 5 executions for the dashboard
        executions = get_recent_executions(db_service, limit=5)
        # Get strategy leaderboard
        strategies = get_strategy_leaderboard(db_service)

        # Prepare chart data
        labels = []
        data = []
        for opp in reversed(opportunities):  # Reverse to get chronological order
            if opp.get("created_at"):
                labels.append(
                    opp["created_at"].strftime("%H:%M:%S") if hasattr(opp["created_at"], "strftime") else "N/A"
                )
                data.append(opp.get("profit_percent", 0))

        chart_data = {"labels": labels, "data": data}

    return {
        "type": "full_update",
        "stats": stats,
        "opportunities": opportunities,
        "executions": executions,
        "strategies": strategies,
        "chart_data": chart_data,
    }


async def handle_websocket_message(websocket: WebSocket, message: str):
    """Handle messages from WebSocket clients."""
    try:
        data = json.loads(message)
        action = data.get("action")

        if action == "ping":
            # Just acknowledge the ping
            await websocket.send_text(json.dumps({"type": "pong"}))

        elif action == "get_data":
            # Send full dashboard data
            full_data = await get_full_dashboard_data()
            await websocket.send_text(json.dumps(full_data))

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON received: {message}")
    except Exception as e:
        logger.error(f"Error handling WebSocket message: {e}")


async def websocket_broadcast_task():
    """
    Background task to broadcast updates via WebSockets.

    This task runs continuously, polling for updates to send to connected clients.
    """
    from src.arbirich.services.redis.redis_service import RedisService

    logger = logging.getLogger(__name__)
    logger.info("Starting WebSocket broadcast task")

    # Use a try-except block to catch and log exceptions
    try:
        # Initialize Redis for pub/sub
        redis = RedisService()
        logger.info("Redis service initialized for WebSocket broadcasts")

        # Subscribe to relevant channels
        pubsub = redis.client.pubsub()
        pubsub.subscribe("broadcast")
        pubsub.subscribe("status_updates")
        pubsub.subscribe("trade_opportunities")
        pubsub.subscribe("trade_executions")
        logger.info("Subscribed to Redis channels for broadcasts")

        # Run broadcast loop
        while True:
            # Process messages with a timeout to allow for clean shutdown
            message = pubsub.get_message(ignore_subscribe_messages=True, timeout=0.1)
            if message:
                try:
                    # Process message and broadcast to clients
                    channel = message.get("channel", b"unknown").decode("utf-8")
                    data = message.get("data")

                    if isinstance(data, bytes):
                        data = data.decode("utf-8")

                    # Log the received message type but not the full content to avoid log spam
                    logger.debug(f"Broadcasting message from channel {channel}")

                    # Broadcast to active connections
                    await broadcast_to_connections(channel, data)
                except Exception as e:
                    logger.error(f"Error processing WebSocket message: {e}", exc_info=True)

            # Periodic broadcast of system status
            await asyncio.sleep(0.1)  # Small sleep to prevent CPU hogging
    except asyncio.CancelledError:
        logger.info("WebSocket broadcast task cancelled, shutting down")
        raise  # Re-raise to allow proper cleanup
    except Exception as e:
        logger.error(f"WebSocket broadcast task encountered an error: {e}", exc_info=True)
        # Sleep briefly before exiting to avoid immediate restarts causing CPU spikes
        await asyncio.sleep(1)
        # Return normally to allow the application to continue running
    finally:
        # Clean up resources
        try:
            pubsub.unsubscribe()
            pubsub.close()
            logger.info("Cleaned up WebSocket broadcast resources")
        except Exception as e:
            logger.error(f"Error cleaning up WebSocket resources: {e}")


async def broadcast_to_connections(channel: str, data: str):
    try:
        for connection in manager.active_connections:
            try:
                # Send the message to the client if they're subscribed to this channel
                if channel in connection.channels:
                    await connection.websocket.send_text(data)
            except Exception as e:
                logger.error(f"Error sending message to client: {e}")
                # Don't remove the connection here - do that only when receive_text throws
    except Exception as e:
        logger.error(f"Error preparing broadcast message: {e}")
