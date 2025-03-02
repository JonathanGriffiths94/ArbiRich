import logging
from dataclasses import dataclass
from datetime import timedelta
from typing import Callable, List

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition, batch_async

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class ExchangePartition(StatefulSourcePartition):
    def __init__(
        self,
        exchange: str,
        product_id: str,
        processor_cls: type,
        subscription_type: str = "snapshot",
        use_rest_snapshot: bool = False,
    ):
        self.exchange = exchange
        self.product_id = product_id
        self.processor_cls = processor_cls
        self.subscription_type = subscription_type
        self.use_rest_snapshot = use_rest_snapshot
        self.processor_instance = self.processor_cls(
            self.exchange, self.product_id, self.subscription_type, self.use_rest_snapshot
        )
        self._batcher = batch_async(
            self.processor_instance.run(),
            timedelta(milliseconds=100),
            100,
        )

    def next_batch(self):
        try:
            batch = next(self._batcher)
            logger.debug(f"Fetched batch for {self.exchange}-{self.product_id}: {batch}")
            # Wrap each update to ensure it's a 3-tuple.
            wrapped_batch = [(self.exchange, self.product_id, update) for update in batch]
            return wrapped_batch
        except Exception as e:
            logger.error(
                f"Error fetching next batch for {self.exchange}-{self.product_id}: {e}"
            )
            return []

    def snapshot(self):
        return None


@dataclass
class MultiExchangeSource(FixedPartitionedSource):
    exchanges: dict[str, List[str]]
    processor_loader: Callable[[str], type]

    def list_parts(self):
        parts = [
            f"{exchange}_{product}"
            for exchange, products in self.exchanges.items()
            for product in products
        ]
        if not parts:
            logger.error(
                "No partitions were created! Check your exchange-product mapping."
            )
        logger.info(f"List of partitions: {parts}")
        return parts

    def build_part(self, step_id, for_key, _resume_state):
        try:
            exchange, product_id = for_key.split("_", 1)
            logger.info(f"Building partition for key: {for_key}")
            return ExchangePartition(
                exchange, product_id, self.processor_loader(exchange)
            )
        except Exception as e:
            logger.error(f"Invalid partition key: {for_key}, Error: {e}")
            return None
