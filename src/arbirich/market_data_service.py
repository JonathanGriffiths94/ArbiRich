import json

import redis


class MarketDataService:
    """Handles real-time market data storage, retrieval, and event publishing using Redis"""

    def __init__(self, host="localhost", port=6379, db=0):
        """Initialize Redis connection"""
        self.redis_client = redis.Redis(
            host=host, port=port, db=db, decode_responses=True
        )

    # === Trade Opportunity Methods ===
    def store_trade_opportunity(self, opportunity_data):
        opportunity_id = (
            opportunity_data.get("id") or f"opp:{opportunity_data.get('timestamp')}"
        )
        key = f"trade_opportunity:{opportunity_id}"
        self.redis_client.set(key, json.dumps(opportunity_data), ex=300)
        self.publish_trade_opportunity(opportunity_data)

    def get_trade_opportunity(self, opportunity_id):
        key = f"trade_opportunity:{opportunity_id}"
        data = self.redis_client.get(key)
        return json.loads(data) if data else None

    def publish_trade_opportunity(self, opportunity_data):
        channel = "trade_opportunity"
        message = json.dumps(opportunity_data)
        self.redis_client.publish(channel, message)

    def subscribe_to_trade_opportunities(self, callback):
        pubsub = self.redis_client.pubsub()
        pubsub.subscribe("trade_opportunity")
        for message in pubsub.listen():
            if message["type"] == "message":
                data = json.loads(message["data"])
                callback(data)

    # === Trade Execution Methods ===
    def store_trade_execution(self, execution_data):
        execution_id = (
            execution_data.get("id") or f"exec:{execution_data.get('timestamp')}"
        )
        key = f"trade_execution:{execution_id}"
        self.redis_client.set(key, json.dumps(execution_data), ex=3600)
        self.publish_trade_execution(execution_data)

    def get_trade_execution(self, execution_id):
        key = f"trade_execution:{execution_id}"
        data = self.redis_client.get(key)
        return json.loads(data) if data else None

    def publish_trade_execution(self, execution_data):
        channel = "trade_executions"
        message = json.dumps(execution_data)
        self.redis_client.publish(channel, message)

    def subscribe_to_trade_executions(self, callback):
        pubsub = self.redis_client.pubsub()
        pubsub.subscribe("trade_executions")
        for message in pubsub.listen():
            if message["type"] == "message":
                data = json.loads(message["data"])
                callback(data)
