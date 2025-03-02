import asyncio
import json
import logging

import requests
import websockets

from src.arbirich.io.websockets.base import BaseOrderBookProcessor

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class KuCoinOrderBookProcessor(BaseOrderBookProcessor):
    def __init__(self, product: str, depth=50, subscription_type="delta"):
        super().__init__(product, subscription_type)
        # Example URL and subscription parameters; adjust per KuCoin docs.
        self.ws_url = "wss://ws-api.kucoin.com/endpoint"
        self.snapshot_url = f"https://api.kucoin.com/api/v1/market/orderbook/level2?symbol={self.product}&limit={depth}"

    async def connect(self):
        return await websockets.connect(self.ws_url)

    async def subscribe(self, websocket):
        sub_msg = {
            "id": 1,
            "type": "subscribe",
            "topic": f"/market/level2:{self.product}",
            "privateChannel": False,
            "response": True,
        }
        await websocket.send(json.dumps(sub_msg))

    async def buffer_events(self, websocket):
        buffered_events = []
        first_event = None
        start = asyncio.get_event_loop().time()
        while True:
            message = await websocket.recv()
            data = json.loads(message)
            # For KuCoin, filter messages that are not orderbook updates.
            if (
                data.get("type") != "message"
                or data.get("topic") != f"/market/level2:{self.product}"
            ):
                continue
            # For delta updates, we expect a field (for example, "sequence" or similar).
            # Assume our delta update has "U" and "u" similar to Binance.
            if first_event is None:
                first_event = data["data"]
                logger.info(f"First buffered event U: {first_event.get('U')}")
            buffered_events.append(data["data"])
            if asyncio.get_event_loop().time() - start > 1 or len(buffered_events) >= 5:
                break
        return buffered_events, first_event

    def fetch_snapshot(self):
        response = requests.get(self.snapshot_url)
        # Assume the snapshot structure includes 'bids', 'asks', and a 'lastUpdateId' field.
        return response.json()["data"]

    def get_snapshot_update_id(self, snapshot):
        # For example, KuCoin might have 'lastUpdateId'
        return snapshot["lastUpdateId"]

    def init_order_book(self, snapshot):
        bids = {float(price): float(qty) for price, qty in snapshot["bids"]}
        asks = {float(price): float(qty) for price, qty in snapshot["asks"]}
        return {"bids": bids, "asks": asks}

    def process_buffered_events(self, events, snapshot_lastUpdateId):
        for event in events:
            if event["u"] < snapshot_lastUpdateId:
                continue
            if event["U"] > snapshot_lastUpdateId + 1:
                logger.error("Gap detected in buffered events; re-subscribing...")
                return
            self.apply_event(event)
            self.local_update_id = event["u"]

    async def live_updates(self, websocket):
        async for message in websocket:
            yield message

    def apply_event(self, event):
        # Assume event contains updated bids/asks arrays similar to snapshot.
        for side in ["bids", "asks"]:
            for price, qty in event.get(side, []):
                price = float(price)
                qty = float(qty)
                if qty == 0:
                    self.order_book[side].pop(price, None)
                else:
                    self.order_book[side][price] = qty

    async def resubscribe(self, websocket):
        logger.info("Resubscribing to KuCoin stream for fresh snapshot...")
        await self.subscribe(websocket)
