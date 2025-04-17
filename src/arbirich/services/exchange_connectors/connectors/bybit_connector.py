"""
Bybit exchange connector implementation.
"""

import asyncio
import hmac
import json
import logging
import time
import uuid
from typing import Any, Callable, Dict

import aiohttp
import websockets

from arbirich.services.exchange_connectors.connectors.base_connector import BaseExchangeConnector
from src.arbirich.models.enums import OrderType
from src.arbirich.models.models import OrderBookUpdate, TradeRequest


class BybitConnector(BaseExchangeConnector):
    """
    Connector for the Bybit exchange.

    This provides implementation for interacting with Bybit's API.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the Bybit connector with configuration.

        Args:
            config: Exchange-specific configuration
        """
        super().__init__(config)
        self.rest_url = config.get("rest_url", "https://api.bybit.com")
        self.ws_url = config.get("ws_url", "wss://stream.bybit.com/v5/public/spot")
        self.session = None
        self.ws_connections = {}
        self.active_subscriptions = {}
        self.logger = logging.getLogger("arbirich.connectors.bybit")

    async def initialize(self) -> bool:
        """
        Initialize the Bybit connector.

        Returns:
            bool: True if initialization was successful
        """
        try:
            self.session = aiohttp.ClientSession()
            self.logger.info("Initialized Bybit connector")
            return True
        except Exception as e:
            self.logger.error(f"Error initializing Bybit connector: {e}")
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
            url = f"{self.rest_url}/v5/market/tickers"
            params = {"category": "spot", "symbol": symbol}

            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()

                if data.get("retCode") == 0 and data.get("result", {}).get("list"):
                    ticker_data = data["result"]["list"][0]
                    return {
                        "symbol": symbol,
                        "bid": float(ticker_data.get("bid1Price", 0)),
                        "ask": float(ticker_data.get("ask1Price", 0)),
                        "last": float(ticker_data.get("lastPrice", 0)),
                        "volume": float(ticker_data.get("volume24h", 0)),
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
            url = f"{self.rest_url}/v5/market/orderbook"
            params = {"category": "spot", "symbol": symbol, "limit": depth}

            async with self.session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()

                if data.get("retCode") == 0 and data.get("result"):
                    result = data["result"]
                    timestamp = result.get("ts") / 1000 if result.get("ts") else time.time()

                    # Convert to bids/asks dictionaries
                    bids_dict = {float(bid[0]): float(bid[1]) for bid in result.get("b", [])}
                    asks_dict = {float(ask[0]): float(ask[1]) for ask in result.get("a", [])}

                    return OrderBookUpdate(
                        id=str(uuid.uuid4()),
                        exchange="bybit",
                        symbol=symbol,
                        bids=bids_dict,
                        asks=asks_dict,
                        timestamp=timestamp,
                    )
                else:
                    self.logger.error(f"Error getting order book for {symbol}: {data}")
                    return OrderBookUpdate(
                        id=str(uuid.uuid4()), exchange="bybit", symbol=symbol, bids={}, asks={}, timestamp=time.time()
                    )
        except Exception as e:
            self.logger.error(f"Error getting order book for {symbol}: {e}")
            return OrderBookUpdate(
                id=str(uuid.uuid4()), exchange="bybit", symbol=symbol, bids={}, asks={}, timestamp=time.time()
            )

    def _generate_signature(self, timestamp: int, params: Dict[str, Any]) -> str:
        """
        Generate signature for authenticated requests.

        Args:
            timestamp: Current timestamp in milliseconds
            params: Request parameters

        Returns:
            HMAC signature string
        """
        api_secret = self.api_secret
        if not api_secret:
            self.logger.error("API secret not provided for authentication")
            return ""

        param_str = f"{timestamp}{self.api_key}"

        if params:
            param_str += json.dumps(params)

        signature = hmac.new(api_secret.encode("utf-8"), param_str.encode("utf-8"), digestmod="sha256").hexdigest()

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
            url = f"{self.rest_url}/v5/account/wallet-balance"
            timestamp = int(time.time() * 1000)
            params = {"accountType": "SPOT"}

            headers = {
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-TIMESTAMP": str(timestamp),
                "X-BAPI-SIGN": self._generate_signature(timestamp, params),
            }

            async with self.session.get(url, params=params, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()

                if data.get("retCode") == 0 and data.get("result"):
                    balances = {}
                    for coin in data["result"]["list"]:
                        for asset in coin.get("coin", []):
                            currency = asset.get("coin")
                            available = float(asset.get("free", 0))
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
            url = f"{self.rest_url}/v5/order/create"
            timestamp = int(time.time() * 1000)

            params = {
                "category": "spot",
                "symbol": trade_request.symbol,
                "side": trade_request.side.value.upper(),
                "orderType": "Limit" if trade_request.order_type == OrderType.LIMIT else "Market",
                "qty": str(trade_request.amount),
            }

            if trade_request.order_type == OrderType.LIMIT and trade_request.price is not None:
                params["price"] = str(trade_request.price)

            headers = {
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-TIMESTAMP": str(timestamp),
                "X-BAPI-SIGN": self._generate_signature(timestamp, params),
                "Content-Type": "application/json",
            }

            async with self.session.post(url, json=params, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()

                if data.get("retCode") == 0 and data.get("result"):
                    return {
                        "success": True,
                        "order_id": data["result"].get("orderId"),
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
                    return {"success": False, "error": data.get("retMsg", "Unknown error"), "raw_response": data}
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
            url = f"{self.rest_url}/v5/order/cancel"
            timestamp = int(time.time() * 1000)

            params = {
                "category": "spot",
                "symbol": symbol,
                "orderId": order_id,
            }

            headers = {
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-TIMESTAMP": str(timestamp),
                "X-BAPI-SIGN": self._generate_signature(timestamp, params),
                "Content-Type": "application/json",
            }

            async with self.session.post(url, json=params, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()

                if data.get("retCode") == 0:
                    return {
                        "success": True,
                        "order_id": order_id,
                        "symbol": symbol,
                        "timestamp": time.time(),
                        "raw_response": data,
                    }
                else:
                    self.logger.error(f"Error cancelling order: {data}")
                    return {"success": False, "error": data.get("retMsg", "Unknown error"), "raw_response": data}
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
            url = f"{self.rest_url}/v5/order/realtime"
            timestamp = int(time.time() * 1000)
            params = {
                "category": "spot",
                "symbol": symbol,
                "orderId": order_id,
            }

            headers = {
                "X-BAPI-API-KEY": self.api_key,
                "X-BAPI-TIMESTAMP": str(timestamp),
                "X-BAPI-SIGN": self._generate_signature(timestamp, params),
            }

            async with self.session.get(url, params=params, headers=headers) as response:
                response.raise_for_status()
                data = await response.json()

                if data.get("retCode") == 0 and data.get("result", {}).get("list"):
                    order_data = data["result"]["list"][0]
                    return {
                        "success": True,
                        "order_id": order_id,
                        "symbol": symbol,
                        "status": order_data.get("orderStatus"),
                        "filled": float(order_data.get("cumExecQty", 0)),
                        "remaining": float(order_data.get("leavesQty", 0)),
                        "average_price": float(order_data.get("avgPrice", 0)),
                        "timestamp": time.time(),
                        "raw_response": data,
                    }
                else:
                    self.logger.error(f"Error getting order status: {data}")
                    return {"success": False, "error": data.get("retMsg", "Unknown error"), "raw_response": data}
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
            subscribe_message = {"op": "subscribe", "args": [f"tickers.{symbol}"]}

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
                "op": "subscribe",
                "args": [f"orderbook.50.{symbol}"],  # Using depth of 50
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

                    if "topic" in data:
                        topic = data["topic"]

                        # Handle ticker updates
                        if topic.startswith("tickers."):
                            symbol = topic.split(".")[1]
                            subscription_id = f"ticker_{symbol}"

                            if subscription_id in self.active_subscriptions:
                                callback = self.active_subscriptions[subscription_id]
                                ticker_data = {
                                    "symbol": symbol,
                                    "bid": float(data["data"]["bid1Price"]),
                                    "ask": float(data["data"]["ask1Price"]),
                                    "last": float(data["data"]["lastPrice"]),
                                    "volume": float(data["data"]["volume24h"]),
                                    "timestamp": data["ts"] / 1000 if "ts" in data else time.time(),
                                }
                                await callback(ticker_data)

                        # Handle order book updates
                        elif topic.startswith("orderbook."):
                            parts = topic.split(".")
                            symbol = parts[2]
                            subscription_id = f"orderbook_{symbol}"

                            if subscription_id in self.active_subscriptions:
                                callback = self.active_subscriptions[subscription_id]
                                timestamp = data["ts"] / 1000 if "ts" in data else time.time()

                                # Convert to order book update model
                                bids_dict = {float(bid[0]): float(bid[1]) for bid in data["data"]["b"]}
                                asks_dict = {float(ask[0]): float(ask[1]) for ask in data["data"]["a"]}

                                order_book_update = OrderBookUpdate(
                                    id=str(uuid.uuid4()),
                                    exchange="bybit",
                                    symbol=symbol,
                                    bids=bids_dict,
                                    asks=asks_dict,
                                    timestamp=timestamp,
                                )

                                await callback(order_book_update)

                except (websockets.exceptions.ConnectionClosed, websockets.exceptions.ConnectionClosedError):
                    self.logger.warning("WebSocket connection closed, attempting to reconnect...")
                    self.ws_connections["public"] = await websockets.connect(self.ws_url)

                    # Re-subscribe to all active subscriptions
                    for sub_id, callback in self.active_subscriptions.items():
                        if sub_id.startswith("ticker_"):
                            symbol = sub_id[7:]  # Remove "ticker_" prefix
                            subscribe_message = {"op": "subscribe", "args": [f"tickers.{symbol}"]}
                        elif sub_id.startswith("orderbook_"):
                            symbol = sub_id[10:]  # Remove "orderbook_" prefix
                            subscribe_message = {"op": "subscribe", "args": [f"orderbook.50.{symbol}"]}
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
        self.logger.info("Closing Bybit connector")

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
