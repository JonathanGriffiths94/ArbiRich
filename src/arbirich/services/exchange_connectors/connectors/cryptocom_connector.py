"""
Crypto.com exchange connector implementation.
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


class CryptocomConnector(BaseExchangeConnector):
    """
    Connector for the Crypto.com exchange.

    This provides implementation for interacting with Crypto.com's API.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Crypto.com connector with configuration.

        Args:
            config: Exchange-specific configuration
        """
        super().__init__(config)
        self.rest_url = config.get("rest_url", "https://api.crypto.com")
        self.ws_url = config.get("ws_url", "wss://stream.crypto.com/v2/market")
        self.session = None
        self.ws_connections = {}
        self.active_subscriptions = {}
        self.request_id = 0
        self.logger = logging.getLogger("arbirich.connectors.cryptocom")

    async def initialize(self) -> bool:
        """
        Initialize the Crypto.com connector.

        Returns:
            bool: True if initialization was successful
        """
        try:
            self.session = aiohttp.ClientSession()
            self.logger.info("Initialized Crypto.com connector")
            return True
        except Exception as e:
            self.logger.error(f"Error initializing Crypto.com connector: {e}")
            return False

    async def get_ticker(self, symbol: str) -> Dict[str, Any]:
        """
        Get current ticker information for a symbol.

        Args:
            symbol: Trading pair symbol

        Returns:
            Dict containing ticker information
        """
        try:
            url = f"{self.rest_url}/v2/public/get-ticker"
            params = {"instrument_name": symbol}

            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()

                if data.get("code") == 0 and "result" in data:
                    ticker_data = data["result"]["data"]
                    return {
                        "symbol": symbol,
                        "bid": float(ticker_data.get("b", 0)),
                        "ask": float(ticker_data.get("a", 0)),
                        "last": float(ticker_data.get("l", 0)),
                        "volume": float(ticker_data.get("v", 0)),
                        "timestamp": time.time(),
                    }
                else:
                    self.logger.error(f"Error getting ticker for {symbol}: {data}")
                    return {}
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
            url = f"{self.rest_url}/v2/public/get-book"
            params = {"instrument_name": symbol, "depth": depth}

            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()

                if data.get("code") == 0 and "result" in data:
                    result = data["result"]["data"][0]
                    timestamp = result.get("t") / 1000 if result.get("t") else time.time()

                    # Convert to bids/asks dictionaries
                    bids_dict = {float(bid[0]): float(bid[1]) for bid in result.get("bids", [])}
                    asks_dict = {float(ask[0]): float(ask[1]) for ask in result.get("asks", [])}

                    return OrderBookUpdate(
                        id=str(uuid.uuid4()),
                        exchange="cryptocom",
                        symbol=symbol,
                        bids=bids_dict,
                        asks=asks_dict,
                        timestamp=timestamp,
                    )
                else:
                    self.logger.error(f"Error getting order book for {symbol}: {data}")
                    return OrderBookUpdate(
                        id=str(uuid.uuid4()),
                        exchange="cryptocom",
                        symbol=symbol,
                        bids={},
                        asks={},
                        timestamp=time.time(),
                    )
        except Exception as e:
            self.logger.error(f"Error getting order book for {symbol}: {e}")
            return OrderBookUpdate(
                id=str(uuid.uuid4()), exchange="cryptocom", symbol=symbol, bids={}, asks={}, timestamp=time.time()
            )

    def _generate_signature(self, request_path: str, params: Dict[str, Any], nonce: int) -> str:
        """
        Generate signature for authenticated requests.

        Args:
            request_path: API endpoint path
            params: Request parameters
            nonce: Unique nonce value

        Returns:
            HMAC signature string
        """
        api_secret = self.api_secret
        if not api_secret:
            self.logger.error("API secret not provided for authentication")
            return ""

        param_str = ""

        # Sort params by key
        for key in sorted(params.keys()):
            param_str += f"{key}{params[key]}"

        sig_payload = f"{request_path}{param_str}{nonce}"

        signature = hmac.new(
            api_secret.encode("utf-8"), sig_payload.encode("utf-8"), digestmod=hashlib.sha256
        ).hexdigest()

        return signature

    def _get_next_request_id(self) -> int:
        """
        Get the next request ID for API calls.

        Returns:
            int: Unique request ID
        """
        self.request_id += 1
        return self.request_id

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
            url = f"{self.rest_url}/v2/private/get-account-summary"
            nonce = int(time.time() * 1000)
            request_path = "/v2/private/get-account-summary"

            params = {
                "api_key": self.api_key,
                "nonce": nonce,
            }

            params["sig"] = self._generate_signature(request_path, params, nonce)

            async with self.session.post(url, json=params) as response:
                response.raise_for_status()
                data = await response.json()

                if data.get("code") == 0 and "result" in data:
                    balances = {}
                    for account in data["result"]["accounts"]:
                        currency = account.get("currency")
                        available = float(account.get("available", 0))
                        balances[currency] = available
                    return balances
                else:
                    self.logger.error(f"Error getting account balance: {data}")
                    return {}
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
            url = f"{self.rest_url}/v2/private/create-order"
            nonce = int(time.time() * 1000)
            request_path = "/v2/private/create-order"

            params = {
                "api_key": self.api_key,
                "nonce": nonce,
                "instrument_name": trade_request.symbol,
                "side": trade_request.side.value.lower(),
                "quantity": trade_request.amount,
            }

            if trade_request.order_type == OrderType.LIMIT and trade_request.price is not None:
                params["type"] = "LIMIT"
                params["price"] = trade_request.price
            else:
                params["type"] = "MARKET"

            params["sig"] = self._generate_signature(request_path, params, nonce)

            async with self.session.post(url, json=params) as response:
                response.raise_for_status()
                data = await response.json()

                if data.get("code") == 0 and "result" in data:
                    return {
                        "success": True,
                        "order_id": data["result"]["order_id"],
                        "symbol": trade_request.symbol,
                        "side": trade_request.side.value,
                        "order_type": trade_request.order_type.value,
                        "amount": trade_request.amount,
                        "price": trade_request.price,
                        "timestamp": time.time(),
                        "raw_response": data,
                    }
                else:
                    self.logger.error(f"Error creating order: {data}")
                    return {"success": False, "error": data.get("message", "Unknown error"), "raw_response": data}
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
            url = f"{self.rest_url}/v2/private/cancel-order"
            nonce = int(time.time() * 1000)
            request_path = "/v2/private/cancel-order"

            params = {
                "api_key": self.api_key,
                "nonce": nonce,
                "instrument_name": symbol,
                "order_id": order_id,
            }

            params["sig"] = self._generate_signature(request_path, params, nonce)

            async with self.session.post(url, json=params) as response:
                response.raise_for_status()
                data = await response.json()

                if data.get("code") == 0:
                    return {
                        "success": True,
                        "order_id": order_id,
                        "symbol": symbol,
                        "timestamp": time.time(),
                        "raw_response": data,
                    }
                else:
                    self.logger.error(f"Error cancelling order: {data}")
                    return {"success": False, "error": data.get("message", "Unknown error"), "raw_response": data}
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
            url = f"{self.rest_url}/v2/private/get-order-detail"
            nonce = int(time.time() * 1000)
            request_path = "/v2/private/get-order-detail"

            params = {
                "api_key": self.api_key,
                "nonce": nonce,
                "order_id": order_id,
            }

            params["sig"] = self._generate_signature(request_path, params, nonce)

            async with self.session.post(url, json=params) as response:
                response.raise_for_status()
                data = await response.json()

                if data.get("code") == 0 and "result" in data:
                    order_data = data["result"]["order_info"]
                    return {
                        "success": True,
                        "order_id": order_id,
                        "symbol": symbol,
                        "status": order_data.get("status"),
                        "filled": float(order_data.get("cumulative_quantity", 0)),
                        "remaining": float(order_data.get("quantity", 0))
                        - float(order_data.get("cumulative_quantity", 0)),
                        "average_price": float(order_data.get("avg_price", 0)),
                        "timestamp": time.time(),
                        "raw_response": data,
                    }
                else:
                    self.logger.error(f"Error getting order status: {data}")
                    return {"success": False, "error": data.get("message", "Unknown error"), "raw_response": data}
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
            # Start WebSocket connection if not already running
            if "public" not in self.ws_connections:
                self.ws_connections["public"] = await websockets.connect(self.ws_url)
                asyncio.create_task(self._handle_public_ws_messages())

            # Subscribe to ticker
            subscribe_message = {
                "id": self._get_next_request_id(),
                "method": "subscribe",
                "params": {"channels": [f"ticker.{symbol}"]},
            }

            await self.ws_connections["public"].send(json.dumps(subscribe_message))
            self.active_subscriptions[subscription_id] = callback
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
            # Start WebSocket connection if not already running
            if "public" not in self.ws_connections:
                self.ws_connections["public"] = await websockets.connect(self.ws_url)
                asyncio.create_task(self._handle_public_ws_messages())

            # Subscribe to order book
            subscribe_message = {
                "id": self._get_next_request_id(),
                "method": "subscribe",
                "params": {
                    "channels": [f"book.{symbol}.50"]  # Using depth of 50
                },
            }

            await self.ws_connections["public"].send(json.dumps(subscribe_message))
            self.active_subscriptions[subscription_id] = callback
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
                    if "result" in data and isinstance(data["result"], dict) and "channel" in data["result"]:
                        continue

                    # Handle ticker updates
                    if "method" in data and data["method"] == "public/heartbeat":
                        # Send heartbeat response
                        heartbeat_response = {"id": data["id"], "method": "public/respond-heartbeat"}
                        await ws.send(json.dumps(heartbeat_response))
                        continue

                    # Handle ticker updates
                    if (
                        "method" in data
                        and data["method"] == "subscribe"
                        and "result" in data
                        and "channel" in data["result"]
                    ):
                        channel = data["result"]["channel"]

                        if channel.startswith("ticker."):
                            symbol = channel.split(".")[1]
                            subscription_id = f"ticker_{symbol}"

                            if subscription_id in self.active_subscriptions and "data" in data["result"]:
                                callback = self.active_subscriptions[subscription_id]
                                ticker_data = data["result"]["data"][0]

                                await callback(
                                    {
                                        "symbol": symbol,
                                        "bid": float(ticker_data.get("b", 0)),
                                        "ask": float(ticker_data.get("a", 0)),
                                        "last": float(ticker_data.get("l", 0)),
                                        "volume": float(ticker_data.get("v", 0)),
                                        "timestamp": ticker_data.get("t", time.time()) / 1000,
                                    }
                                )

                        # Handle order book updates
                        elif channel.startswith("book."):
                            parts = channel.split(".")
                            symbol = parts[1]
                            subscription_id = f"orderbook_{symbol}"

                            if subscription_id in self.active_subscriptions and "data" in data["result"]:
                                callback = self.active_subscriptions[subscription_id]
                                book_data = data["result"]["data"][0]
                                timestamp = book_data.get("t", time.time()) / 1000

                                # Convert to order book update model
                                bids_dict = {float(bid[0]): float(bid[1]) for bid in book_data.get("bids", [])}
                                asks_dict = {float(ask[0]): float(ask[1]) for ask in book_data.get("asks", [])}

                                order_book_update = OrderBookUpdate(
                                    id=str(uuid.uuid4()),
                                    exchange="cryptocom",
                                    symbol=symbol,
                                    bids=bids_dict,
                                    asks=asks_dict,
                                    timestamp=timestamp,
                                    sequence=book_data.get("s"),
                                )

                                await callback(order_book_update)

                except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedError):
                    self.logger.warning("WebSocket connection closed, attempting to reconnect...")
                    self.ws_connections["public"] = await websockets.connect(self.ws_url)

                    # Re-subscribe to all active subscriptions
                    for sub_id, callback in self.active_subscriptions.items():
                        if sub_id.startswith("ticker_"):
                            symbol = sub_id[7:]  # Remove "ticker_" prefix
                            subscribe_message = {
                                "id": self._get_next_request_id(),
                                "method": "subscribe",
                                "params": {"channels": [f"ticker.{symbol}"]},
                            }
                        elif sub_id.startswith("orderbook_"):
                            symbol = sub_id[10:]  # Remove "orderbook_" prefix
                            subscribe_message = {
                                "id": self._get_next_request_id(),
                                "method": "subscribe",
                                "params": {"channels": [f"book.{symbol}.50"]},
                            }
                        else:
                            continue

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
        self.logger.info("Closing Crypto.com connector")

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
