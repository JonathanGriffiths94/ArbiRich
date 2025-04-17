"""
Binance exchange connector implementation.
"""

import asyncio
import hashlib
import hmac
import json
import logging
import time
import uuid
from typing import Any, Callable, Dict

import aiohttp
import websockets

from arbirich.services.exchange_connectors.connectors.base_connector import BaseExchangeConnector
from src.arbirich.models import OrderBookUpdate, TradeRequest
from src.arbirich.models.enums import OrderType


class BinanceConnector(BaseExchangeConnector):
    """
    Connector for the Binance exchange.

    This provides implementation for interacting with Binance's API.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Binance connector with configuration.

        Args:
            config: Exchange-specific configuration
        """
        super().__init__(config)
        self.rest_url = config.get("rest_url", "https://api.binance.com")
        self.ws_url = config.get("ws_url", "wss://stream.binance.com:9443/ws")
        self.session = None
        self.ws_connections = {}
        self.active_subscriptions = {}
        self.logger = logging.getLogger("arbirich.connectors.binance")

    async def initialize(self) -> bool:
        """
        Initialize the Binance connector.

        Returns:
            bool: True if initialization was successful
        """
        try:
            self.session = aiohttp.ClientSession()
            self.logger.info("Initialized Binance connector")
            return True
        except Exception as e:
            self.logger.error(f"Error initializing Binance connector: {e}")
            return False

    async def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Get current ticker information for a symbol.

        Args:
            symbol: Trading pair symbol (e.g., BTCUSDT)

        Returns:
            Dict containing ticker information
        """
        try:
            # Binance uses different symbol format (no hyphen)
            binance_symbol = symbol.replace("-", "")

            url = f"{self.rest_url}/api/v3/ticker/bookTicker"
            params = {"symbol": binance_symbol}

            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()

                return {
                    "symbol": symbol,
                    "bid": float(data.get("bidPrice", 0)),
                    "ask": float(data.get("askPrice", 0)),
                    "bid_size": float(data.get("bidQty", 0)),
                    "ask_size": float(data.get("askQty", 0)),
                    "timestamp": time.time(),
                }
        except Exception as e:
            self.logger.error(f"Error getting ticker for {symbol}: {e}")
            return {}

    async def get_order_book(self, symbol: str, depth: int = 20) -> OrderBookUpdate:
        """
        Get order book for a symbol.

        Args:
            symbol: Trading pair symbol
            depth: Depth of the order book to retrieve

        Returns:
            OrderBookUpdate containing order book data with bids and asks
        """
        try:
            # Binance uses different symbol format (no hyphen)
            binance_symbol = symbol.replace("-", "")

            url = f"{self.rest_url}/api/v3/depth"
            params = {"symbol": binance_symbol, "limit": depth}

            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()

                # Convert to bids/asks dictionaries
                bids_dict = {float(bid[0]): float(bid[1]) for bid in data.get("bids", [])}
                asks_dict = {float(ask[0]): float(ask[1]) for ask in data.get("asks", [])}

                return OrderBookUpdate(
                    id=str(uuid.uuid4()),
                    exchange="binance",
                    symbol=symbol,
                    bids=bids_dict,
                    asks=asks_dict,
                    timestamp=time.time(),
                    sequence=data.get("lastUpdateId"),
                )
        except Exception as e:
            self.logger.error(f"Error getting order book for {symbol}: {e}")
            return OrderBookUpdate(
                id=str(uuid.uuid4()), exchange="binance", symbol=symbol, bids={}, asks={}, timestamp=time.time()
            )

    def _generate_signature(self, params: Dict[str, Any]) -> str:
        """
        Generate signature for authenticated requests.

        Args:
            params: Request parameters

        Returns:
            HMAC signature string
        """
        api_secret = self.api_secret
        if not api_secret:
            self.logger.error("API secret not provided for authentication")
            return ""

        query_string = "&".join([f"{key}={params[key]}" for key in params])

        signature = hmac.new(api_secret.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256).hexdigest()

        return signature

    async def get_account_balance(self) -> Dict[str, float]:
        """
        Get account balances for all currencies.

        Returns:
            Dict mapping currency symbols to their available balances
        """
        if not self.api_key or not self.api_secret:
            self.logger.error("API credentials required for account balance")
            return {}

        try:
            url = f"{self.rest_url}/api/v3/account"
            timestamp = int(time.time() * 1000)

            params = {"timestamp": timestamp}

            # Add signature
            params["signature"] = self._generate_signature(params)

            headers = {"X-MBX-APIKEY": self.api_key}

            async with self.session.get(url, params=params, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()

                balances = {}
                for asset in data.get("balances", []):
                    currency = asset.get("asset")
                    available = float(asset.get("free", 0))
                    balances[currency] = available
                return balances
        except Exception as e:
            self.logger.error(f"Error getting account balance: {e}")
            return {}

    async def create_order(self, trade_request: TradeRequest) -> Dict[str, Any]:
        """
        Create an order on the exchange.

        Args:
            trade_request: Trade request model containing order parameters

        Returns:
            Dict containing order information
        """
        if not self.api_key or not self.api_secret:
            self.logger.error("API credentials required for creating orders")
            return {"success": False, "error": "API credentials not provided"}

        try:
            url = f"{self.rest_url}/api/v3/order"
            timestamp = int(time.time() * 1000)

            # Binance uses different symbol format (no hyphen)
            binance_symbol = trade_request.symbol.replace("-", "")

            params = {
                "symbol": binance_symbol,
                "side": trade_request.side.value.upper(),
                "type": "LIMIT" if trade_request.order_type == OrderType.LIMIT else "MARKET",
                "quantity": trade_request.amount,
                "timestamp": timestamp,
            }

            if trade_request.order_type == OrderType.LIMIT and trade_request.price is not None:
                params["price"] = trade_request.price
                params["timeInForce"] = "GTC"  # Good Till Canceled

            # Add signature
            params["signature"] = self._generate_signature(params)

            headers = {"X-MBX-APIKEY": self.api_key}

            async with self.session.post(url, params=params, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()

                return {
                    "success": True,
                    "order_id": str(data.get("orderId")),
                    "symbol": trade_request.symbol,
                    "side": trade_request.side.value,
                    "order_type": trade_request.order_type.value,
                    "amount": trade_request.amount,
                    "price": trade_request.price,
                    "timestamp": time.time(),
                    "raw_response": data,
                }
        except Exception as e:
            self.logger.error(f"Error creating order: {e}")
            return {"success": False, "error": str(e)}

    async def cancel_order(self, symbol: str, order_id: str) -> Dict[str, Any]:
        """
        Cancel an existing order.

        Args:
            symbol: Trading pair symbol
            order_id: ID of the order to cancel

        Returns:
            Dict containing cancellation information
        """
        if not self.api_key or not self.api_secret:
            self.logger.error("API credentials required for cancelling orders")
            return {"success": False, "error": "API credentials not provided"}

        try:
            url = f"{self.rest_url}/api/v3/order"
            timestamp = int(time.time() * 1000)

            # Binance uses different symbol format (no hyphen)
            binance_symbol = symbol.replace("-", "")

            params = {
                "symbol": binance_symbol,
                "orderId": order_id,
                "timestamp": timestamp,
            }

            # Add signature
            params["signature"] = self._generate_signature(params)

            headers = {"X-MBX-APIKEY": self.api_key}

            async with self.session.delete(url, params=params, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()

                return {
                    "success": True,
                    "order_id": order_id,
                    "symbol": symbol,
                    "timestamp": time.time(),
                    "raw_response": data,
                }
        except Exception as e:
            self.logger.error(f"Error cancelling order: {e}")
            return {"success": False, "error": str(e)}

    async def get_order_status(self, symbol: str, order_id: str) -> Dict[str, Any]:
        """
        Get status of an order.

        Args:
            symbol: Trading pair symbol
            order_id: ID of the order to check

        Returns:
            Dict containing order status
        """
        if not self.api_key or not self.api_secret:
            self.logger.error("API credentials required for order status")
            return {"success": False, "error": "API credentials not provided"}

        try:
            url = f"{self.rest_url}/api/v3/order"
            timestamp = int(time.time() * 1000)

            # Binance uses different symbol format (no hyphen)
            binance_symbol = symbol.replace("-", "")

            params = {
                "symbol": binance_symbol,
                "orderId": order_id,
                "timestamp": timestamp,
            }

            # Add signature
            params["signature"] = self._generate_signature(params)

            headers = {"X-MBX-APIKEY": self.api_key}

            async with self.session.get(url, params=params, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()

                # Map Binance order status to common format
                status_mapping = {
                    "NEW": "open",
                    "PARTIALLY_FILLED": "partially_filled",
                    "FILLED": "filled",
                    "CANCELED": "canceled",
                    "REJECTED": "rejected",
                    "EXPIRED": "expired",
                }

                status = status_mapping.get(data.get("status"), "unknown")
                executed_qty = float(data.get("executedQty", 0))
                original_qty = float(data.get("origQty", 0))
                remaining_qty = original_qty - executed_qty

                return {
                    "success": True,
                    "order_id": order_id,
                    "symbol": symbol,
                    "status": status,
                    "filled": executed_qty,
                    "remaining": remaining_qty,
                    "average_price": float(data.get("price", 0)),
                    "timestamp": time.time(),
                    "raw_response": data,
                }
        except Exception as e:
            self.logger.error(f"Error getting order status: {e}")
            return {"success": False, "error": str(e)}

    async def subscribe_to_ticker(self, symbol: str, callback: Callable) -> Any:
        """
        Subscribe to ticker updates for a symbol.

        Args:
            symbol: Trading pair symbol
            callback: Callback function to call with ticker updates

        Returns:
            Subscription identifier
        """
        subscription_id = f"ticker_{symbol}"

        if subscription_id in self.active_subscriptions:
            self.logger.info(f"Already subscribed to ticker for {symbol}")
            return subscription_id

        try:
            # Binance uses different symbol format (no hyphen)
            binance_symbol = symbol.replace("-", "").lower()

            # Start WebSocket connection if not already running
            if "public" not in self.ws_connections:
                self.ws_connections["public"] = await websockets.connect(f"{self.ws_url}")
                asyncio.create_task(self._handle_public_ws_messages())

            # Subscribe to ticker
            stream_name = f"{binance_symbol}@bookTicker"
            subscribe_message = {"method": "SUBSCRIBE", "params": [stream_name], "id": int(time.time() * 1000)}

            await self.ws_connections["public"].send(json.dumps(subscribe_message))
            self.active_subscriptions[subscription_id] = (callback, stream_name)
            self.logger.info(f"Subscribed to ticker for {symbol}")

            return subscription_id

        except Exception as e:
            self.logger.error(f"Error subscribing to ticker: {e}")
            return None

    async def subscribe_to_order_book(self, symbol: str, callback: Callable[[OrderBookUpdate], Any]) -> Any:
        """
        Subscribe to order book updates for a symbol.

        Args:
            symbol: Trading pair symbol
            callback: Callback function to call with order book updates

        Returns:
            Subscription identifier
        """
        subscription_id = f"orderbook_{symbol}"

        if subscription_id in self.active_subscriptions:
            self.logger.info(f"Already subscribed to orderbook for {symbol}")
            return subscription_id

        try:
            # Binance uses different symbol format (no hyphen)
            binance_symbol = symbol.replace("-", "").lower()

            # Start WebSocket connection if not already running
            if "public" not in self.ws_connections:
                self.ws_connections["public"] = await websockets.connect(f"{self.ws_url}")
                asyncio.create_task(self._handle_public_ws_messages())

            # Subscribe to order book with 20 levels of depth
            stream_name = f"{binance_symbol}@depth20@100ms"
            subscribe_message = {"method": "SUBSCRIBE", "params": [stream_name], "id": int(time.time() * 1000)}

            await self.ws_connections["public"].send(json.dumps(subscribe_message))
            self.active_subscriptions[subscription_id] = (callback, stream_name)
            self.logger.info(f"Subscribed to orderbook for {symbol}")

            return subscription_id

        except Exception as e:
            self.logger.error(f"Error subscribing to orderbook: {e}")
            return None

    async def _handle_public_ws_messages(self):
        """
        Handle WebSocket messages from the public stream.
        """
        if "public" not in self.ws_connections:
            self.logger.error("Public WebSocket connection not established")
            return

        ws = self.ws_connections["public"]

        try:
            while True:
                try:
                    message = await ws.recv()
                    data = json.loads(message)

                    # Handle subscription responses
                    if "result" in data:
                        continue

                    # Handle heartbeat messages
                    if "ping" in data:
                        pong_message = {"pong": data["ping"]}
                        await ws.send(json.dumps(pong_message))
                        continue

                    # Handle stream data
                    if "stream" in data:
                        stream = data["stream"]
                        stream_data = data["data"]

                        # Handle ticker updates
                        if "@bookTicker" in stream:
                            symbol = stream.split("@")[0].upper()
                            formatted_symbol = f"{symbol[:-4]}-{symbol[-4:]}"  # Convert BTCUSDT to BTC-USDT
                            subscription_id = f"ticker_{formatted_symbol}"

                            if subscription_id in self.active_subscriptions:
                                callback, _ = self.active_subscriptions[subscription_id]
                                ticker_data = {
                                    "symbol": formatted_symbol,
                                    "bid": float(stream_data.get("b", 0)),
                                    "ask": float(stream_data.get("a", 0)),
                                    "bid_size": float(stream_data.get("B", 0)),
                                    "ask_size": float(stream_data.get("A", 0)),
                                    "timestamp": time.time(),
                                }
                                await callback(ticker_data)

                        # Handle order book updates
                        elif "@depth" in stream:
                            symbol = stream.split("@")[0].upper()
                            formatted_symbol = f"{symbol[:-4]}-{symbol[-4:]}"  # Convert BTCUSDT to BTC-USDT
                            subscription_id = f"orderbook_{formatted_symbol}"

                            if subscription_id in self.active_subscriptions:
                                callback, _ = self.active_subscriptions[subscription_id]

                                # Convert to order book update model
                                bids_dict = {float(bid[0]): float(bid[1]) for bid in stream_data.get("bids", [])}
                                asks_dict = {float(ask[0]): float(ask[1]) for ask in stream_data.get("asks", [])}

                                order_book_update = OrderBookUpdate(
                                    id=str(uuid.uuid4()),
                                    exchange="binance",
                                    symbol=formatted_symbol,
                                    bids=bids_dict,
                                    asks=asks_dict,
                                    timestamp=time.time(),
                                    sequence=stream_data.get("lastUpdateId"),
                                )

                                await callback(order_book_update)

                except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedError):
                    self.logger.warning("WebSocket connection closed, attempting to reconnect...")
                    self.ws_connections["public"] = await websockets.connect(f"{self.ws_url}")

                    # Re-subscribe to all active subscriptions
                    for sub_id, (callback, stream_name) in self.active_subscriptions.items():
                        subscribe_message = {
                            "method": "SUBSCRIBE",
                            "params": [stream_name],
                            "id": int(time.time() * 1000),
                        }
                        await self.ws_connections["public"].send(json.dumps(subscribe_message))

                except Exception as e:
                    self.logger.error(f"Error in WebSocket message handler: {e}")
                    await asyncio.sleep(1)  # To avoid tight loop on repeated errors

        except Exception as e:
            self.logger.error(f"Fatal error in WebSocket handler: {e}")
        finally:
            # Clean up on exit
            if "public" in self.ws_connections:
                await self.ws_connections["public"].close()
                del self.ws_connections["public"]

    async def close(self) -> None:
        """
        Close connections and clean up resources.
        """
        self.logger.info("Closing Binance connector")

        # Close WebSocket connections
        for ws_name, ws in self.ws_connections.items():
            try:
                await ws.close()
                self.logger.info(f"Closed WebSocket connection: {ws_name}")
            except Exception as e:
                self.logger.error(f"Error closing WebSocket connection {ws_name}: {e}")

        self.ws_connections = {}
        self.active_subscriptions = {}

        # Close HTTP session
        if self.session:
            try:
                await self.session.close()
                self.logger.info("Closed HTTP session")
            except Exception as e:
                self.logger.error(f"Error closing HTTP session: {e}")
            finally:
                self.session = None
