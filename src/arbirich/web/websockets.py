import asyncio
import json
import logging
from datetime import datetime
from typing import Any, Dict, List

import redis
from broadcaster import Broadcast
from fastapi import WebSocket

from src.arbirich.config.config import REDIS_DB, REDIS_HOST, REDIS_PASSWORD, REDIS_PORT
from src.arbirich.services.database.database_service import DatabaseService
from src.arbirich.web.frontend import (
    get_dashboard_stats,
    get_recent_executions,
    get_recent_opportunities,
    get_strategy_leaderboard,
)

logger = logging.getLogger(__name__)

# Define channels to subscribe to for WebSocket updates
WEBSOCKET_CHANNELS = [
    "trade_opportunities",
    "trade_executions",
    "status_updates",
    "broadcast",
]


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


async def websocket_broadcast_task(broadcast: Broadcast):
    """
    Task to listen for Redis messages and broadcast them to WebSocket clients.
    """
    logger.info("Starting WebSocket broadcast task")
    redis_client = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD, decode_responses=False
    )
    pubsub = redis_client.pubsub()

    try:
        # Subscribe to channels
        pubsub.subscribe(*WEBSOCKET_CHANNELS)
        logger.info(f"Subscribed to channels: {WEBSOCKET_CHANNELS}")

        while True:
            try:
                message = pubsub.get_message(timeout=1.0)
                if message is None:
                    # No message, just sleep briefly
                    await asyncio.sleep(0.01)
                    continue

                if message["type"] != "message":
                    # Skip non-message events like subscribe confirmations
                    continue

                # Handle both string and bytes channel names
                channel = message.get("channel")
                if isinstance(channel, bytes):
                    channel = channel.decode("utf-8")
                elif not isinstance(channel, str):
                    channel = "unknown"

                # Handle both string and bytes data
                data = message.get("data")
                if isinstance(data, bytes):
                    data = data.decode("utf-8")

                logger.debug(f"Received message from channel: {channel}")

                # Process based on channel type
                if channel == "trade_opportunities":
                    await process_opportunity(data, broadcast)
                elif channel == "trade_executions":
                    await process_execution(data, broadcast)
                elif channel == "status_updates":
                    await process_status_update(data, broadcast)
                elif channel == "broadcast":
                    await broadcast.publish(group="broadcast", message=data)
                else:
                    # For other channels like order_book:exchange:symbol
                    await process_channel_message(channel, data, broadcast)

            except redis.ConnectionError as ce:
                logger.error(f"Redis connection error: {ce}")
                await asyncio.sleep(5)  # Wait before reconnecting
                pubsub = redis_client.pubsub()
                pubsub.subscribe(*WEBSOCKET_CHANNELS)
                logger.info("Reconnected to Redis")

            except Exception as e:
                logger.error(f"Error processing WebSocket message: {e}", exc_info=True)
                await asyncio.sleep(0.1)  # Avoid tight loop on errors

    except asyncio.CancelledError:
        logger.info("WebSocket broadcast task cancelled")
        pubsub.unsubscribe()
        pubsub.close()
        redis_client.close()
        raise

    except Exception as e:
        logger.error(f"Unexpected error in WebSocket broadcast task: {e}", exc_info=True)
        pubsub.unsubscribe()
        pubsub.close()
        redis_client.close()
        raise


async def broadcast_to_connections(channel: str, data: str):
    """
    Broadcast a message to all WebSocket connections.

    Args:
        channel: The Redis channel the message came from
        data: The message data as a string
    """
    try:
        # Log the incoming data for debugging
        logger.info(f"Received message from channel: {channel}")
        logger.info(f"Message data: {data[:200]}..." if len(data) > 200 else f"Message data: {data}")

        # Try to parse the data as JSON
        try:
            data_obj = json.loads(data)
            logger.info(f"Parsed JSON with keys: {list(data_obj.keys())}")

            # Log different data based on message type
            if "type" in data_obj:
                logger.info(f"Message type: {data_obj['type']}")

            if "opportunity_id" in data_obj:
                logger.info(f"Opportunity ID: {data_obj['opportunity_id']}")

            if "strategy" in data_obj:
                logger.info(f"Strategy: {data_obj['strategy']}")

            if "pair" in data_obj:
                logger.info(f"Trading pair: {data_obj['pair']}")
        except json.JSONDecodeError:
            logger.warning(f"Could not parse message as JSON: {data[:50]}...")

        # Create a payload based on the channel
        payload = {"type": "update", "channel": channel, "data": data, "timestamp": datetime.now().isoformat()}

        # Broadcast to all active connections
        for connection in manager.active_connections:
            try:
                await connection.send_text(json.dumps(payload))
            except Exception as e:
                logger.error(f"Error sending WebSocket message to client: {e}")
    except Exception as e:
        logger.error(f"Error in broadcast_to_connections: {e}", exc_info=True)


async def process_opportunity(data: str, broadcast: Broadcast):
    """Process and broadcast trade opportunities"""
    try:
        payload = {"type": "opportunity", "data": data, "timestamp": datetime.now().isoformat()}
        await broadcast.publish(group="trade_opportunities", message=json.dumps(payload))
    except Exception as e:
        logger.error(f"Error processing opportunity: {e}")


async def process_execution(data: str, broadcast: Broadcast):
    """Process and broadcast trade executions"""
    try:
        payload = {"type": "execution", "data": data, "timestamp": datetime.now().isoformat()}
        await broadcast.publish(group="trade_executions", message=json.dumps(payload))
    except Exception as e:
        logger.error(f"Error processing execution: {e}")


async def process_status_update(data: str, broadcast: Broadcast):
    """Process and broadcast status updates"""
    try:
        payload = {"type": "status", "data": data, "timestamp": datetime.now().isoformat()}
        await broadcast.publish(group="status", message=json.dumps(payload))
    except Exception as e:
        logger.error(f"Error processing status update: {e}")


async def process_channel_message(channel: str, data: str, broadcast: Broadcast):
    """Process and broadcast other channel messages"""
    try:
        # Extract channel type from channel name (e.g., "order_book:binance:BTC-USDT" → "order_book")
        channel_parts = channel.split(":")
        channel_type = channel_parts[0] if channel_parts else "unknown"

        payload = {"type": channel_type, "channel": channel, "data": data, "timestamp": datetime.now().isoformat()}

        # Publish to group matching the channel type
        await broadcast.publish(group=channel_type, message=json.dumps(payload))
    except Exception as e:
        logger.error(f"Error processing channel message: {e}")
