from typing import Any, Dict

from src.arbirich.exchange.base_exchange import ExchangeClient


class MockExchangeClient(ExchangeClient):
    """Mock exchange client for testing execution strategies"""

    async def place_market_order(self, symbol: str, side: str, quantity: float) -> Dict[str, Any]:
        """Simulate placing a market order"""
        return {
            "success": True,
            "order_id": f"mock-market-{side}-{symbol}-{quantity}",
            "price": 0.0,  # Would be filled in with real execution price
            "executed_quantity": quantity,
        }

    async def place_limit_order(self, symbol: str, side: str, quantity: float, price: float) -> Dict[str, Any]:
        """Simulate placing a limit order"""
        return {
            "success": True,
            "order_id": f"mock-limit-{side}-{symbol}-{quantity}-{price}",
            "price": price,
            "executed_quantity": 0.0,  # Not executed yet
        }

    async def cancel_order(self, symbol: str, order_id: str) -> Dict[str, Any]:
        """Simulate cancelling an order"""
        return {"success": True, "order_id": order_id}

    async def get_order_status(self, symbol: str, order_id: str) -> Dict[str, Any]:
        """Simulate getting order status"""
        # For testing, assume all orders get filled after a short time
        return {
            "order_id": order_id,
            "symbol": symbol,
            "status": "filled",
            "executed_price": 0.0,  # Would be the actual price
            "executed_quantity": 0.0,  # Would be the actual quantity
        }
