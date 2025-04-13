import asyncio
import json
import logging
import time  # Add this import
from datetime import datetime
from typing import Any, Dict, List

import redis
from broadcaster import Broadcast
from fastapi import WebSocket

from src.arbirich.config.config import REDIS_CONFIG, REDIS_DB, REDIS_HOST, REDIS_PASSWORD, REDIS_PORT
from src.arbirich.constants import (
    TRADE_EXECUTIONS_CHANNEL,
    TRADE_OPPORTUNITIES_CHANNEL,
    WEBSOCKET_CHANNELS,  # Use the constant from constants.py
)
from src.arbirich.core.state.system_state import is_system_shutting_down  # Add this import
from src.arbirich.services.database.database_service import DatabaseService
from src.arbirich.web.frontend import (
    get_dashboard_stats,
    get_recent_executions,
    get_recent_opportunities,
    get_strategy_leaderboard,
)

logger = logging.getLogger(__name__)

# Define REDIS_URL using the configuration
REDIS_URL = f"redis://{REDIS_CONFIG['host']}:{REDIS_CONFIG['port']}/{REDIS_CONFIG['db']}"

# Add or update channels to the existing WEBSOCKET_CHANNELS if needed
WEBSOCKET_CHANNELS.extend(
    [
        # Add any additional channels here if necessary
    ]
)


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
        message_type = data.get("type", "")

        if message_type == "get_dashboard_data":
            # Send full dashboard data when requested
            dashboard_data = await get_full_dashboard_data()
            await websocket.send_text(json.dumps(dashboard_data))

        elif message_type == "subscribe":
            # Handle subscription requests
            channels = data.get("channels", [])

            if channels:
                for channel in channels:
                    if channel in WEBSOCKET_CHANNELS:
                        # Log subscription request
                        logger.info(f"Client requested subscription to channel: {channel}")

                        # Send confirmation
                        await websocket.send_text(json.dumps({"type": "subscription_confirmed", "channel": channel}))

        elif message_type == "ping":
            # Simple ping/pong for connection testing
            await websocket.send_text(json.dumps({"type": "pong", "timestamp": datetime.now().isoformat()}))

        else:
            logger.warning(f"Unknown message type: {message_type}")
            await websocket.send_text(json.dumps({"type": "error", "message": "Unknown message type"}))

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON received: {message}")
        await websocket.send_text(json.dumps({"type": "error", "message": "Invalid JSON format"}))
    except Exception as e:
        logger.error(f"Error handling WebSocket message: {e}", exc_info=True)
        await websocket.send_text(json.dumps({"type": "error", "message": "Internal server error"}))


async def websocket_broadcast_task(broadcast: Broadcast = None):
    """
    Task to listen for Redis messages and broadcast them to WebSocket clients.

    Args:
        broadcast: Optional Broadcast instance. If None, a new instance will be created.
    """
    logger.info("Starting WebSocket broadcast task")

    # Create a new Broadcast instance if one wasn't provided
    if broadcast is None:
        broadcast = Broadcast(REDIS_URL)
        await broadcast.connect()
        logger.info("Created new Broadcast instance")

    redis_client = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, password=REDIS_PASSWORD, decode_responses=False
    )
    pubsub = redis_client.pubsub()

    try:
        # Subscribe to channels
        pubsub.subscribe(*WEBSOCKET_CHANNELS)
        logger.info(f"Subscribed to channels: {WEBSOCKET_CHANNELS}")

        while True:
            # Check if the system is shutting down before processing messages
            if is_system_shutting_down():
                logger.info("System shutdown detected, exiting WebSocket broadcast task")
                break

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
                if channel == TRADE_OPPORTUNITIES_CHANNEL:
                    await process_opportunity(data, broadcast)
                elif channel == TRADE_EXECUTIONS_CHANNEL:
                    await process_execution(data, broadcast)
                elif channel == "status_updates":
                    await process_status_update(data, broadcast)
                elif channel == "broadcast":
                    # Use the simplified version without group parameter
                    await broadcast.publish(json.dumps(data))
                else:
                    # For other channels like ORDER_BOOK_CHANNEL
                    await process_channel_message(channel, data, broadcast)

            except redis.ConnectionError as ce:
                # Check shutdown state before attempting reconnection
                if is_system_shutting_down():
                    logger.info("System shutting down during connection error, exiting")
                    break

                logger.error(f"Redis connection error: {ce}")
                await asyncio.sleep(5)  # Wait before reconnecting

                # Recreate the pubsub object
                try:
                    pubsub.close()
                except Exception:
                    pass  # Ignore errors on close

                pubsub = redis_client.pubsub()
                pubsub.subscribe(*WEBSOCKET_CHANNELS)
                logger.info("Reconnected to Redis")

            except Exception as e:
                if is_system_shutting_down():
                    logger.info("System shutting down during error, exiting")
                    break

                logger.error(f"Error processing WebSocket message: {e}", exc_info=True)
                await asyncio.sleep(0.1)  # Avoid tight loop on errors

    except asyncio.CancelledError:
        logger.info("WebSocket broadcast task cancelled")
        raise
    except Exception as e:
        logger.error(f"Unexpected error in WebSocket broadcast task: {e}", exc_info=True)
        raise
    finally:
        # Ensure resources are closed when exiting the loop
        logger.info("Cleaning up WebSocket broadcast resources")
        try:
            pubsub.unsubscribe()
            pubsub.close()
            redis_client.close()
            logger.info("Redis connections closed")
        except Exception as e:
            logger.error(f"Error closing Redis connections: {e}")

        # If we created our own broadcast instance, close it
        if broadcast is not None and broadcast is not Broadcast:
            try:
                await broadcast.disconnect()
                logger.info("Broadcast instance disconnected")
            except Exception as e:
                logger.error(f"Error disconnecting broadcast: {e}")


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


async def process_opportunity(data, broadcast: Broadcast):
    """Process and broadcast a trade opportunity to websocket clients."""
    try:
        logger.debug("Processing opportunity data")

        # Handle different input types
        if hasattr(data, "channel") and hasattr(data, "data"):
            # It's a message object (with channel and data attributes)
            opportunity_data = data.data
        else:
            # It's a string or another format
            opportunity_data = data

        # If opportunity_data is a string, try to parse it
        if isinstance(opportunity_data, str):
            try:
                opportunity = json.loads(opportunity_data)
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse opportunity data as JSON: {opportunity_data[:100]}...")
                opportunity = {"raw_data": opportunity_data}
        else:
            opportunity = opportunity_data

        # Extract key fields for better logging and display
        opportunity_id = opportunity.get("id", "unknown")
        pair = opportunity.get("pair", "unknown")
        buy_exchange = opportunity.get("buy_exchange", "unknown")
        sell_exchange = opportunity.get("sell_exchange", "unknown")
        spread = opportunity.get("spread", 0)

        # Log meaningful information about the opportunity
        logger.debug(
            f"Processing opportunity: {opportunity_id} - {pair} - {buy_exchange}/{sell_exchange} - {spread:.4%}"
        )

        # Prepare the message with event type and add a timestamp if not present
        if "timestamp" not in opportunity and "opportunity_timestamp" in opportunity:
            opportunity["timestamp"] = opportunity["opportunity_timestamp"]
        elif "timestamp" not in opportunity:
            opportunity["timestamp"] = time.time()

        ws_message = {"event": "opportunity", "data": opportunity}

        # Broadcast to all connected clients - FIXED: added required 'message' parameter name
        await broadcast.publish(channel="trade_opportunities", message=json.dumps(ws_message))

        logger.debug(f"Broadcasted opportunity: {opportunity.get('id', 'unknown')} for {pair}")
    except Exception as e:
        logger.error(f"Error processing opportunity: {e}", exc_info=True)


async def process_execution(data: str, broadcast: Broadcast):
    """Process and broadcast trade executions"""
    try:
        payload = {"type": "execution", "data": data, "timestamp": datetime.now().isoformat()}
        # Remove the group parameter to fix the error
        await broadcast.publish(json.dumps(payload))
    except Exception as e:
        logger.error(f"Error processing execution: {e}", exc_info=True)


async def process_status_update(data: str, broadcast: Broadcast):
    """Process and broadcast status updates"""
    try:
        payload = {"type": "status", "data": data, "timestamp": datetime.now().isoformat()}
        # Remove the group parameter to fix the error
        await broadcast.publish(json.dumps(payload))
    except Exception as e:
        logger.error(f"Error processing status update: {e}", exc_info=True)


async def process_channel_message(channel: str, data: str, broadcast: Broadcast):
    """Process and broadcast other channel messages"""
    try:
        # Extract channel type from channel name (e.g., "order_books:binance:BTC-USDT" → "order_books")
        channel_parts = channel.split(":")
        channel_type = channel_parts[0] if channel_parts else "unknown"

        payload = {"type": channel_type, "channel": channel, "data": data, "timestamp": datetime.now().isoformat()}

        # Remove the group parameter to fix the error
        await broadcast.publish(json.dumps(payload))
    except Exception as e:
        logger.error(f"Error processing channel message: {e}", exc_info=True)
